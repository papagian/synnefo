# Copyright (C) 2010-2014 GRNET S.A.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import uuid as uuidlib
import logging
import hashlib
import binascii

from collections import defaultdict
from functools import wraps, partial
from traceback import format_exc
from time import time

from pithos.workers import glue
from archipelago.common import Segment, Xseg_ctx
from objpool import ObjectPool


try:
    from astakosclient import AstakosClient
except ImportError:
    AstakosClient = None

from pithos.backends.base import (
    DEFAULT_ACCOUNT_QUOTA, DEFAULT_CONTAINER_QUOTA,
    DEFAULT_CONTAINER_VERSIONING, NotAllowedError, QuotaError,
    BaseBackend, AccountExists, ContainerExists, AccountNotEmpty,
    ContainerNotEmpty, ItemNotExists, VersionNotExists,
    InvalidHash, IllegalOperationError)


class DisabledAstakosClient(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __getattr__(self, name):
        m = ("AstakosClient has been disabled, "
             "yet an attempt to access it was made")
        raise AssertionError(m)


# Stripped-down version of the HashMap class found in tools.

class HashMap(list):

    def __init__(self, blocksize, blockhash):
        super(HashMap, self).__init__()
        self.blocksize = blocksize
        self.blockhash = blockhash

    def _hash_raw(self, v):
        h = hashlib.new(self.blockhash)
        h.update(v)
        return h.digest()

    def hash(self):
        if len(self) == 0:
            return self._hash_raw('')
        if len(self) == 1:
            return self.__getitem__(0)

        h = list(self)
        s = 2
        while s < len(h):
            s = s * 2
        h += [('\x00' * len(h[0]))] * (s - len(h))
        while len(h) > 1:
            h = [self._hash_raw(h[x] + h[x + 1]) for x in range(0, len(h), 2)]
        return h[0]

# Default modules and settings.
DEFAULT_DB_MODULE = 'pithos.backends.lib.sqlalchemy'
DEFAULT_DB_CONNECTION = 'sqlite:///backend.db'
DEFAULT_BLOCK_MODULE = 'pithos.backends.lib.hashfiler'
DEFAULT_BLOCK_PATH = 'data/'
DEFAULT_BLOCK_UMASK = 0o022
DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024  # 4MB
DEFAULT_HASH_ALGORITHM = 'sha256'
# DEFAULT_QUEUE_MODULE = 'pithos.backends.lib.rabbitmq'
DEFAULT_BLOCK_PARAMS = {'mappool': None, 'blockpool': None}
# DEFAULT_QUEUE_HOSTS = '[amqp://guest:guest@localhost:5672]'
# DEFAULT_QUEUE_EXCHANGE = 'pithos'
DEFAULT_PUBLIC_URL_ALPHABET = ('0123456789'
                               'abcdefghijklmnopqrstuvwxyz'
                               'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
DEFAULT_PUBLIC_URL_SECURITY = 16
DEFAULT_ARCHIPELAGO_CONF_FILE = '/etc/archipelago/archipelago.conf'

QUEUE_MESSAGE_KEY_PREFIX = 'pithos.%s'
QUEUE_CLIENT_ID = 'pithos'
QUEUE_INSTANCE_ID = '1'

(CLUSTER_NORMAL, CLUSTER_HISTORY, CLUSTER_DELETED) = range(3)

QUOTA_POLICY = 'quota'
VERSIONING_POLICY = 'versioning'
PROJECT = 'project'

inf = float('inf')

ULTIMATE_ANSWER = 42

DEFAULT_DISKSPACE_RESOURCE = 'pithos.diskspace'

DEFAULT_MAP_CHECK_INTERVAL = 5  # set to 5 secs

logger = logging.getLogger(__name__)


def backend_method(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        # if we are inside a database transaction
        # just proceed with the method execution
        # otherwise manage a new transaction
        if self.in_transaction:
            return func(self, *args, **kw)

        try:
            self.pre_exec()
            result = func(self, *args, **kw)
            success_status = True
            return result
        except:
            success_status = False
            raise
        finally:
            self.post_exec(success_status)
    return wrapper


def debug_method(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        try:
            result = func(self, *args, **kw)
            return result
        except:
            result = format_exc()
            raise
        finally:
            all_args = map(repr, args)
            map(all_args.append, ('%s=%s' % (k, v) for k, v in kw.iteritems()))
            logger.debug(">>> %s(%s) <<< %s" % (
                func.__name__, ', '.join(all_args).rstrip(', '), result))
    return wrapper


def check_allowed_paths(action):
    """Decorator for backend methods checking path access granted to user.

    The 1st argument of the decorated method is expected to be a
    ModularBackend instance, the 2nd the user performing the request and
    the path join of the rest arguments is supposed to be the requested path.

    The decorator checks whether the requested path is among the user's allowed
    cached paths.
    If this is the case, the decorator returns immediately to reduce the
    interactions with the database.
    Otherwise, it proceeds with the execution of the decorated method and if
    the method returns successfully (no exceptions are raised), the requested
    path is added to the user's cached allowed paths.

    :param action: (int) 0 for reads / 1 for writes
    :raises NotAllowedError: the user does not have access to the path
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args):
            user = args[0]
            if action == self.READ:
                d = self.read_allowed_paths
            else:
                d = self.write_allowed_paths
            path = '/'.join(args[1:])
            if path in d.get(user, []):
                return  # access is already checked
            else:
                func(self, *args)   # proceed with access check
                d[user].add(path)  # add path in the allowed user paths
        return wrapper
    return decorator


def list_method(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        marker = kw.get('marker')
        limit = kw.get('limit')
        result = func(self, *args, **kw)
        start, limit = self._list_limits(result, marker, limit)
        return result[start:start + limit]
    return wrapper


class ModularBackend(BaseBackend):
    """A modular backend.

    Uses modules for SQL functions and storage.
    """

    def __init__(self, db_module=None, db_connection=None,
                 block_module=None, block_path=None, block_umask=None,
                 block_size=None, hash_algorithm=None,
                 queue_module=None, queue_hosts=None, queue_exchange=None,
                 astakos_auth_url=None, service_token=None,
                 astakosclient_poolsize=None,
                 free_versioning=True, block_params=None,
                 public_url_security=None,
                 public_url_alphabet=None,
                 account_quota_policy=None,
                 container_quota_policy=None,
                 container_versioning_policy=None,
                 archipelago_conf_file=None,
                 xseg_pool_size=8,
                 map_check_interval=None):
        db_module = db_module or DEFAULT_DB_MODULE
        db_connection = db_connection or DEFAULT_DB_CONNECTION
        block_module = block_module or DEFAULT_BLOCK_MODULE
        block_path = block_path or DEFAULT_BLOCK_PATH
        block_umask = block_umask or DEFAULT_BLOCK_UMASK
        block_params = block_params or DEFAULT_BLOCK_PARAMS
        block_size = block_size or DEFAULT_BLOCK_SIZE
        hash_algorithm = hash_algorithm or DEFAULT_HASH_ALGORITHM
        # queue_module = queue_module or DEFAULT_QUEUE_MODULE
        account_quota_policy = account_quota_policy or DEFAULT_ACCOUNT_QUOTA
        container_quota_policy = container_quota_policy \
            or DEFAULT_CONTAINER_QUOTA
        container_versioning_policy = container_versioning_policy \
            or DEFAULT_CONTAINER_VERSIONING
        archipelago_conf_file = archipelago_conf_file \
            or DEFAULT_ARCHIPELAGO_CONF_FILE
        map_check_interval = map_check_interval \
            or DEFAULT_MAP_CHECK_INTERVAL

        self.default_account_policy = {}
        self.default_container_policy = {
            QUOTA_POLICY: container_quota_policy,
            VERSIONING_POLICY: container_versioning_policy,
            PROJECT: None
        }
        # queue_hosts = queue_hosts or DEFAULT_QUEUE_HOSTS
        # queue_exchange = queue_exchange or DEFAULT_QUEUE_EXCHANGE

        self.public_url_security = (public_url_security or
                                    DEFAULT_PUBLIC_URL_SECURITY)
        self.public_url_alphabet = (public_url_alphabet or
                                    DEFAULT_PUBLIC_URL_ALPHABET)

        self.hash_algorithm = hash_algorithm
        self.block_size = block_size
        self.free_versioning = free_versioning
        self.map_check_interval = map_check_interval

        def load_module(m):
            __import__(m)
            return sys.modules[m]

        self.db_module = load_module(db_module)
        self.wrapper = self.db_module.DBWrapper(db_connection)
        params = {'wrapper': self.wrapper}
        self.permissions = self.db_module.Permissions(**params)
        self.commission_serials = self.db_module.QuotaholderSerial(**params)
        for x in ['READ', 'WRITE']:
            setattr(self, x, getattr(self.db_module, x))
        self.node = self.db_module.Node(**params)
        for x in ['ACCOUNT', 'CONTAINER', 'OBJ', 'HASH', 'SIZE',
                  'TYPE', 'MTIME', 'MUSER', 'UUID', 'CHECKSUM', 'CLUSTER',
                  'MATCH_PREFIX', 'MATCH_EXACT',
                  'AVAILABLE', 'MAP_CHECK_TIMESTAMP']:
            setattr(self, x, getattr(self.db_module, x))

        self.ALLOWED = ['read', 'write']

        glue.WorkerGlue.setupXsegPool(ObjectPool, Segment, Xseg_ctx,
                                      cfile=archipelago_conf_file,
                                      pool_size=xseg_pool_size)
        self.block_module = load_module(block_module)
        self.block_params = block_params
        params = {'path': block_path,
                  'block_size': self.block_size,
                  'hash_algorithm': self.hash_algorithm,
                  'umask': block_umask}
        params.update(self.block_params)
        self.store = self.block_module.Store(**params)

        if queue_module and queue_hosts:
            self.queue_module = load_module(queue_module)
            params = {'hosts': queue_hosts,
                      'exchange': queue_exchange,
                      'client_id': QUEUE_CLIENT_ID}
            self.queue = self.queue_module.Queue(**params)
        else:
            class NoQueue:
                def send(self, *args):
                    pass

                def close(self):
                    pass

            self.queue = NoQueue()

        self.astakos_auth_url = astakos_auth_url
        self.service_token = service_token

        if not astakos_auth_url or not AstakosClient:
            self.astakosclient = DisabledAstakosClient(
                service_token, astakos_auth_url,
                use_pool=True,
                pool_size=astakosclient_poolsize)
        else:
            self.astakosclient = AstakosClient(
                service_token, astakos_auth_url,
                use_pool=True,
                pool_size=astakosclient_poolsize)

        self.serials = []
        self.messages = []

        self._move_object = partial(self._copy_object, is_move=True)

        self.lock_container_path = False

        self.in_transaction = False

        self._reset_allowed_paths()

    def pre_exec(self, lock_container_path=False):
        self.lock_container_path = lock_container_path
        self.wrapper.execute()
        self.serials = []
        self._reset_allowed_paths()
        self.in_transaction = True

    def post_exec(self, success_status=True):
        if success_status:
            # send messages produced
            for m in self.messages:
                self.queue.send(*m)

            # register serials
            if self.serials:
                self.commission_serials.insert_many(
                    self.serials)

                # commit to ensure that the serials are registered
                # even if resolve commission fails
                self.wrapper.commit()

                # start new transaction
                self.wrapper.execute()

                r = self.astakosclient.resolve_commissions(
                    accept_serials=self.serials,
                    reject_serials=[])
                self.commission_serials.delete_many(
                    r['accepted'])

            self.wrapper.commit()
        else:
            if self.serials:
                r = self.astakosclient.resolve_commissions(
                    accept_serials=[],
                    reject_serials=self.serials)
                self.commission_serials.delete_many(
                    r['rejected'])
            self.wrapper.rollback()
        self.in_transaction = False

    def close(self):
        self.wrapper.close()
        self.queue.close()

    @property
    def using_external_quotaholder(self):
        return not isinstance(self.astakosclient, DisabledAstakosClient)

    @debug_method
    @backend_method
    @list_method
    def list_accounts(self, user, marker=None, limit=10000):
        """Return a list of accounts the user can access."""

        return self._allowed_accounts(user)

    @debug_method
    @backend_method
    def get_account_meta(self, user, account, domain=None, until=None,
                         include_user_defined=True):
        """Return a dictionary with the account metadata for the domain."""

        self._can_read_account(user, account)
        uuid = self._lookup_account(account, create=(user == account))
        if user != account:
            if until or (uuid is None):
                raise NotAllowedError
        try:
            props = self._get_properties(account, until=until)
            mtime = props[self.MTIME]
        except NameError:
            props = None
            mtime = until
        count, bytes, tstamp = self._get_statistics(account, container='',
                                                    until=until, compute=True)
        tstamp = max(tstamp, mtime)
        if until is None:
            modified = tstamp
        else:
            # Overall last modification.
            modified = self._get_statistics(account, container='',
                                            compute=True)[2]
            modified = max(modified, mtime)

        if user != account:
            meta = {'name': account}
        else:
            meta = {}
            if props is not None and include_user_defined:
                if domain is None:
                    raise ValueError(
                        'Domain argument is obligatory for getting '
                        'user defined metadata')
                meta.update(
                    dict(self.node.attribute_get(props[self.UUID], domain,
                                                 before=until or inf)))
            if until is not None:
                meta.update({'until_timestamp': tstamp})
            meta.update({'name': account, 'count': count, 'bytes': bytes})
            if self.using_external_quotaholder and until is None:
                external_quota = self.astakosclient.service_get_quotas(
                    account)[account]
                meta['bytes'] = sum(d['pithos.diskspace']['usage'] for d in
                                    external_quota.values())
        meta.update({'modified': modified})
        return meta

    @debug_method
    @backend_method
    def update_account_meta(self, user, account, domain, meta, replace=False):
        """Update the metadata associated with the account for the domain."""

        self._can_write_account(user, account)
        uuid = self._lookup_account(account, create=True)
        self._put_metadata(uuid, domain, meta, account, replace=replace)

    @debug_method
    @backend_method
    def get_account_groups(self, user, account):
        """Return a dictionary with the user groups defined for the account."""

        self._can_read_account(user, account)
        if user != account:
            return {}
        self._lookup_account(account, create=True)
        return self.permissions.group_dict(account)

    @debug_method
    @backend_method
    def update_account_groups(self, user, account, groups, replace=False):
        """Update the groups associated with the account."""

        self._can_write_account(user, account)
        self._lookup_account(account, create=True)
        self._check_groups(groups)
        if replace:
            self.permissions.group_destroy(account)
        for k, v in groups.iteritems():
            if not replace:  # If not already deleted.
                self.permissions.group_delete(account, k)
            if v:
                self.permissions.group_addmany(account, k, v)

    @debug_method
    @backend_method
    def get_account_policy(self, user, account):
        """Return a dictionary with the account policy."""

        self._can_read_account(user, account)
        if user != account:
            return {}
        self._lookup_account(account, create=True)
        policy = self._get_policy(account)
        if self.using_external_quotaholder:
            external_quota = self.astakosclient.service_get_quotas(
                account)[account]
            policy.update(dict(('%s-%s' % (QUOTA_POLICY, k),
                                v['pithos.diskspace']['limit']) for k, v in
                               external_quota.items()))

        return policy

    @debug_method
    @backend_method
    def update_account_policy(self, user, account, policy, replace=False):
        """Update the policy associated with the account."""

        self._can_write_account(user, account)
        self._lookup_account(account, create=True)
        self._put_policy(account, None, policy, replace, check=True)

    @debug_method
    @backend_method
    def put_account(self, user, account, policy=None):
        """Create a new account with the given name."""

        policy = policy or {}
        self._can_write_account(user, account)
        try:
            self._put_path(account, account)
        except:  # TODO catch specific exception?
            raise AccountExists('Account already exists')
        self._put_policy(account, None, policy, True,
                         check=True if policy else False)

    @debug_method
    @backend_method
    def delete_account(self, user, account):
        """Delete the account with the given name."""

        self._can_write_account(user, account)
        props = self.node.node_lookup(account)
        if props is None:
            return
        if not self.node.node_remove(account):
            raise AccountNotEmpty('Account is not empty')
        self.permissions.group_destroy(account)

        # remove all the cached allowed paths
        # removing the specific path could be more expensive
        self._reset_allowed_paths()

    @debug_method
    @backend_method
    @list_method
    def list_containers(self, user, account, marker=None, limit=10000,
                        shared=False, until=None, public=False):
        """Return a list of containers existing under an account."""

        self._can_read_account(user, account)
        if user != account:
            if until:
                raise NotAllowedError
            return self._allowed_containers(user, account)
        if shared or public:
            allowed = set()
            if shared:
                allowed.update([x[self.CONTAINER] for x in
                               self.permissions.access_list_shared(account)])
            if public:
                allowed.update([x[self.CONTAINER] for x in
                               self.permissions.public_list(account)])
            return sorted(allowed)

        m, _ = self.node.latest_version_list(account, container='',
                                             start=marker or '',
                                             limit=limit,
                                             before=until or inf,
                                             except_cluster=CLUSTER_DELETED)
        return [p[self.CONTAINER] for p in m]

    @debug_method
    @backend_method
    def list_container_meta(self, user, account, container, domain,
                            until=None):
        """Return a list of the container's object meta keys for a domain."""

        self._can_read_container(user, account, container)
        if user != account:
            if until:
                raise NotAllowedError
        self._lookup_container(account, container)
        before = until if until is not None else inf
        return self.node.latest_attribute_keys(account, container, domain,
                                               before=before,
                                               except_cluster=CLUSTER_DELETED,
                                               pathq=())

    @debug_method
    @backend_method
    def get_container_meta(self, user, account, container, domain=None,
                           until=None, include_user_defined=True):
        """Return a dictionary with the container metadata for the domain."""

        self._can_read_container(user, account, container)
        if user != account:
            if until:
                raise NotAllowedError
        self._lookup_container(account, container)  # TODO: get rid of it!
        props = self._get_properties(account, container=container,
                                     until=until)
        mtime = props[self.MTIME]
        count, bytes, tstamp = self._get_statistics(account,
                                                    container=container,
                                                    until=until)
        tstamp = max(tstamp, mtime)
        if until is None:
            modified = tstamp
        else:
            modified = self._get_statistics(
                account, container)[2]  # Overall last modification.
            modified = max(modified, mtime)

        if user != account:
            meta = {'name': container}
        else:
            meta = {}
            if include_user_defined:
                if domain is None:
                    raise ValueError(
                        'Domain argument is obligatory for getting '
                        'user defined metadata')
                meta.update(
                    dict(self.node.attribute_get(props[self.UUID], domain,
                                                 before=until or inf)))
            if until is not None:
                meta.update({'until_timestamp': tstamp})
            meta.update({'name': container, 'count': count, 'bytes': bytes})
        meta.update({'modified': modified})
        return meta

    @debug_method
    @backend_method
    def update_container_meta(self, user, account, container, domain, meta,
                              replace=False):
        """Update the metadata associated with the container for the domain."""

        self._can_write_container(user, account, container)
        uuid = self._lookup_container(account, container)
        self._put_metadata(uuid, domain, meta, account, container,
                           replace=replace)

    @debug_method
    @backend_method
    def get_container_policy(self, user, account, container):
        """Return a dictionary with the container policy."""

        self._can_read_container(user, account, container)
        if user != account:
            return {}
        self._lookup_container(account, container)
        return self._get_policy(account, container)

    @debug_method
    @backend_method
    def update_container_policy(self, user, account, container, policy,
                                replace=False):
        """Update the policy associated with the container."""

        self._can_write_container(user, account, container)
        self._lookup_container(account, container)

        if PROJECT in policy:
            project = self._get_project(account, container)
            try:
                serial = self.astakosclient.issue_resource_reassignment(
                    holder=account,
                    from_source=project,
                    to_source=policy[PROJECT],
                    provisions={'pithos.diskspace': self.get_container_meta(
                        user, account, container,
                        include_user_defined=False)['bytes']})
            except BaseException, e:
                raise QuotaError(e)
            else:
                self.serials.append(serial)

        self._put_policy(account, container, policy, replace,
                         default_project=account,
                         check=True)

    @debug_method
    @backend_method
    def put_container(self, user, account, container, policy=None):
        """Create a new container with the given name."""

        policy = policy or {}
        self._can_write_container(user, account, container)
        try:
            self._lookup_container(account, container)
        except NameError:
            pass
        else:
            raise ContainerExists('Container already exists')
        self._put_path(user, account, container)
        self._put_policy(account, container, policy, replace=True,
                         default_project=account,
                         check=True if policy else False)

    @debug_method
    @backend_method
    def delete_container(self, user, account, container, until=None, prefix='',
                         delimiter=None, listing_limit=None):
        """Delete/purge the container with the given name."""

        self._can_write_container(user, account, container)
        self._lookup_container(account, container)
        project = self._get_project(account, container)
        path = '/'.join([account, container])

        if until is not None:
            hashes, size, uuids = self.node.node_purge_children(
                account, container, before=until, cluster=CLUSTER_HISTORY)
            for h in hashes:
                self.store.map_delete(h)
            self.node.node_purge_children(account, container, before=until,
                                          cluster=CLUSTER_DELETED)
            if not self.free_versioning:
                self._report_size_change(
                    user, account, -size, project, {
                        'action': 'container purge',
                        'path': path,
                        'versions': ','.join(str(i) for i in uuids)
                    }
                )
            return

        if not delimiter:
            if self._get_statistics(account, container)[0] > 0:
                raise ContainerNotEmpty('Container is not empty')
            hashes, size, uuids = self.node.node_purge_children(
                account, container, before=inf, cluster=CLUSTER_HISTORY)
            for h in hashes:
                self.store.map_delete(h)
            self.node.node_purge_children(account, container, before=inf,
                                          cluster=CLUSTER_DELETED)
            self._put_version_duplicate(user, account, container, obj='',
                                        cluster=CLUSTER_DELETED)
            self.node.node_remove(account, container)
            if not self.free_versioning:
                self._report_size_change(
                    user, account, -size, project, {
                        'action': 'container purge',
                        'path': path,
                        'versions': ','.join(str(i) for i in uuids)
                    }
                )
        else:
            # remove only contents
            src_names = self._list_objects_no_limit(
                user, account, container, prefix='', delimiter=None,
                virtual=False, domain=None, keys=[], shared=False, until=None,
                size_range=None, all_props=True, public=False,
                listing_limit=listing_limit)
            paths = []
            for t in src_names:
                name = t[self.OBJ - 2]
                self._delete_object(user, account, container, name, until=None,
                                    delimiter=None, report_size_change=True)
                paths.append((account, container, name))
            self.permissions.access_clear_bulk(paths)

        # remove all the cached allowed paths
        # removing the specific path could be more expensive
        self._reset_allowed_paths()

    def _list_objects(self, user, account, container, prefix, delimiter,
                      marker, limit, virtual, domain, keys, shared, until,
                      size_range, all_props, public):
        if user != account and until:
            raise NotAllowedError

        objects = []
        allowed = set()
        update = allowed.update
        if shared:
            l = self._list_object_permissions(
                user, account, container, prefix, shared=shared, public=False)
            update(self._get_formatted_paths(l, match_prefix=True))
        if public:
            l = self._list_object_permissions(
                user, account, container, prefix, shared=False, public=public)
            update(self._get_formatted_paths(l, match_prefix=False))
        if (shared or public) and not allowed:
            return []
        self._lookup_container(account, container)  # TODO get rid of it!
        objects = self._list_object_properties(
            account, container, prefix, delimiter=delimiter, marker=marker,
            limit=limit, virtual=virtual, domain=domain, keys=keys,
            until=until, size_range=size_range, allowed=allowed,
            all_props=all_props)

        # apply limits
        start, limit = self._list_limits(objects, marker, limit)
        return objects[start:start + limit]

    def _list_objects_no_limit(self, user, account, container, prefix,
                               delimiter, virtual, domain, keys, shared, until,
                               size_range, all_props, public,
                               listing_limit=10000):
        objects = []
        while True:
            marker = objects[-1][0] if objects else None
            limit = listing_limit
            l = self._list_objects(
                user, account, container, prefix, delimiter, marker, limit,
                virtual, domain, keys, shared, until, size_range, all_props,
                public)
            objects.extend(l)
            if not l or len(l) < limit:
                break
        return objects

    def _list_object_permissions(self, user, account, container, prefix,
                                 shared=True, public=False):
        allowed = []
        path = (account, container, prefix)
        if user != account:
            allowed = self.permissions.access_list_paths(user, *path)
            if not allowed:
                raise NotAllowedError
        else:
            allowed = set()
            if shared:
                allowed.update(self.permissions.access_list_shared(*path))
            if public:
                allowed.update([x[:-1] for x in
                               self.permissions.public_list(*path)])
            allowed = sorted(allowed)
            if not allowed:
                return []
        return allowed

    @debug_method
    @backend_method
    def list_objects(self, user, account, container, prefix='', delimiter=None,
                     marker=None, limit=10000, virtual=True, domain=None,
                     keys=None, shared=False, until=None, size_range=None,
                     public=False):
        """List (object name, object version_id) under a container."""

        keys = keys or []
        return self._list_objects(
            user, account, container, prefix, delimiter, marker, limit,
            virtual, domain, keys, shared, until, size_range, False, public)

    @debug_method
    @backend_method
    def list_object_meta(self, user, account, container, prefix='',
                         delimiter=None, marker=None, limit=10000,
                         virtual=True, domain=None, keys=None, shared=False,
                         until=None, size_range=None, public=False):
        """Return a list of metadata dicts of objects under a container."""

        keys = keys or []
        props = self._list_objects(
            user, account, container, prefix, delimiter, marker, limit,
            virtual, domain, keys, shared, until, size_range, True, public)
        objects = []
        for p in props:
            if len(p) == 1:
                objects.append({'subdir': p[self.OBJ - 2]})
            else:
                objects.append({
                    'name': p[self.OBJ - 2],
                    'bytes': p[self.SIZE - 2],
                    'type': p[self.TYPE - 2],
                    'hash': p[self.HASH - 2],
                    'version': p[self.UUID - 2],
                    'version_timestamp': p[self.MTIME - 2],
                    'modified': p[self.MTIME - 2] if until is None else None,
                    'modified_by': p[self.MUSER - 2],
                    'uuid': p[self.UUID - 2],
                    'checksum': p[self.CHECKSUM - 2],
                    'available': p[self.AVAILABLE - 2],
                    'map_check_timestamp': p[self.MAP_CHECK_TIMESTAMP - 2]})
        return objects

    @debug_method
    @backend_method
    def list_object_permissions(self, user, account, container, prefix=''):
        """Return a list of paths enforce permissions under a container."""

        return self._list_object_permissions(user, account, container, prefix,
                                             shared=True, public=False)

    @debug_method
    @backend_method
    def list_object_public(self, user, account, container, prefix=''):
        """Return a mapping of object paths to public ids under a container."""

        public = {}
        path = (account, container, prefix)
        for account, container, name, p in self.permissions.public_list(*path):
            public['/'.join([account, container, name])] = p
        return public

    @debug_method
    @backend_method
    def get_object_meta(self, user, account, container, name, domain=None,
                        version=None, include_user_defined=True):
        """Return a dictionary with the object metadata for the domain."""

        self._can_read_object(user, account, container, name)
        props = self._get_version(account, container, name, version)
        if version is None:
            if not props[self.AVAILABLE]:
                try:
                    self._update_available(props)
                except (NotAllowedError, IllegalOperationError):
                    pass  # just update the database
                finally:
                    # get updated properties
                    props = self._get_version(account, container, name,
                                              version)
            modified = props[self.MTIME]
        else:
            try:
                modified = self._get_version(
                    account,
                    container,
                    name,
                    version=None)[self.MTIME]  # Overall last modification.
            except NameError:  # Object may be deleted.
                del_props = self.node.version_lookup(
                    account, container, name, before=inf,
                    cluster=CLUSTER_DELETED)
                if del_props is None:
                    raise ItemNotExists('Object does not exist')
                modified = del_props[self.MTIME]

        meta = {}
        if include_user_defined:
            if domain is None:
                raise ValueError(
                    'Domain argument is obligatory for getting '
                    'user defined metadata')
            meta.update(
                dict(self.node.attribute_get(props[self.UUID], domain)))
        meta.update({'name': name,
                     'bytes': props[self.SIZE],
                     'type': props[self.TYPE],
                     'hash': props[self.HASH],
                     'version': props[self.UUID],
                     'version_timestamp': props[self.MTIME],
                     'modified': modified,
                     'modified_by': props[self.MUSER],
                     'uuid': props[self.UUID],
                     'checksum': props[self.CHECKSUM],
                     'available': props[self.AVAILABLE],
                     'map_check_timestamp': props[self.MAP_CHECK_TIMESTAMP]})
        return meta

    @debug_method
    @backend_method
    def update_object_meta(self, user, account, container, name, domain, meta,
                           replace=False):
        """Update object metadata for a domain and return the new version."""

        self._can_write_object(user, account, container, name)
        uuid = self._lookup_object(account, container, name,
                                   lock_container=True)
        self._put_metadata(uuid, domain, meta, account, container, name,
                           replace=replace)
        return uuid

    @debug_method
    @backend_method
    def get_object_permissions_bulk(self, user, account, container, names):
        """Return the action allowed on the object, the path
        from which the object gets its permissions from,
        along with a dictionary containing the permissions."""

        permissions_paths = self._get_permissions_path_bulk(account, container,
                                                            names)
        if not permissions_paths:
            return {}

        access_objects = self.permissions.access_check_bulk(permissions_paths,
                                                            user)
        nobject_permissions = {}
        for path in permissions_paths:
            allowed = 1
            name = path[self.OBJ]
            if user != account:
                try:
                    allowed = access_objects[path]
                except KeyError:
                    raise NotAllowedError
            access_dict, allowed = \
                self.permissions.access_get_for_bulk(access_objects[path])
            nobject_permissions[name] = (self.ALLOWED[allowed], '/'.join(path),
                                         access_dict)
        # TODO: check if it is necessary
        self.node.node_lookup_bulk(permissions_paths)
        return nobject_permissions

    @debug_method
    @backend_method
    def get_object_permissions(self, user, account, container, name):
        """Return the action allowed on the object, the path
        from which the object gets its permissions from,
        along with a dictionary containing the permissions."""

        allowed = 'write'
        permissions_path = self._get_permissions_path(account, container, name)
        if user != account:
            if not permissions_path:
                raise NotAllowedError

            if self.permissions.access_check(*permissions_path,
                                             access=self.WRITE,
                                             member=user):
                allowed = 'write'
            elif self.permissions.access_check(*permissions_path,
                                               access=self.READ,
                                               member=user):
                allowed = 'read'
            else:
                raise NotAllowedError
        if not permissions_path:
            return (allowed, None, {})
        self._lookup_object(account, container, name)  # TODO: is it necessary?
        return (allowed,
                '/'.join(permissions_path),
                self.permissions.access_get(*permissions_path))

    @debug_method
    @backend_method
    def update_object_permissions(self, user, account, container, name,
                                  permissions):
        """Update the permissions associated with the object."""

        if user != account:
            raise NotAllowedError
        self._lookup_object(account, container, name, lock_container=True)
        self._check_permissions(account, container, name, permissions)
        try:
            self.permissions.access_set(account, container, name,
                                        permissions)
        except:
            raise ValueError
        else:
            self._report_sharing_change(user, account, container, name,
                                        {'members':
                                         self.permissions.access_members(
                                             account, container, name)})

        # remove all the cached allowed paths
        # filtering out only those affected could be more expensive
        self._reset_allowed_paths()

    @debug_method
    @backend_method
    def get_object_public(self, user, account, container, name):
        """Return the public id of the object if applicable."""

        self._can_read_object(user, account, container, name)
        # check whether the object exists
        self._lookup_object(account, container, name)[0]
        p = self.permissions.public_get(account, container, name)
        return p

    @debug_method
    @backend_method
    def update_object_public(self, user, account, container, name,
                             public=True):
        """Update the public status of the object."""

        self._can_write_object(user, account, container, name)
        # check whether the object exists & lock the transaction
        self._lookup_object(account, container, name, lock_container=True)
        if not public:
            self.permissions.public_unset(account, container, name)
        else:
            self.permissions.public_set(
                account,
                container,
                name,
                self.public_url_security, self.public_url_alphabet)

    def _update_available(self, props):
        """Checks if the object map exists and updates the database"""

        if not props[self.AVAILABLE]:
            if props[self.MAP_CHECK_TIMESTAMP]:
                elapsed_time = time() - float(props[self.MAP_CHECK_TIMESTAMP])
                if elapsed_time < self.map_check_interval:
                    raise NotAllowedError(
                        'Consequent map checks are limited: retry later.')
        try:
            hashmap = self.store.map_get_archipelago(props[self.HASH],
                                                     props[self.SIZE])
        except:  # map does not exist
            # Raising an exception results in db transaction rollback
            # However we have to force the update of the database
            self.wrapper.rollback()  # rollback existing transaction
            self.wrapper.execute()  # start new transaction
            self.node.version_put_property(props[self.SERIAL],
                                           'map_check_timestamp', time())
            self.wrapper.commit()  # commit transaction
            self.wrapper.execute()  # start new transaction
            raise IllegalOperationError(
                'Unable to retrieve Archipelago Volume hashmap.')
        else:  # map exists
            self.node.version_put_property(props[self.SERIAL],
                                           'available', True)
            self.node.version_put_property(props[self.SERIAL],
                                           'map_check_timestamp', time())
            return hashmap

    @debug_method
    @backend_method
    def get_object_hashmap(self, user, account, container, name, version=None):
        """Return the object's size and a list with partial hashes."""

        self._can_read_object(user, account, container, name)
        props = self._get_version(account, container, name, version)
        if props[self.HASH] is None:
            return 0, ()
        if props[self.HASH].startswith('archip:'):
            hashmap = self._update_available(props)
            return props[self.SIZE], [x for x in hashmap]
        else:
            hashmap = self.store.map_get(self._unhexlify_hash(
                props[self.HASH]))
            return props[self.SIZE], [binascii.hexlify(x) for x in hashmap]

    def _update_object_hash(self, user, account, container, name, size, type,
                            hash, checksum, domain, meta, replace_meta,
                            permissions, src_node=None, src_version_id=None,
                            is_copy=False, report_size_change=True,
                            available=True):
        if permissions is not None and user != account:
            raise NotAllowedError
        self._can_write_object(user, account, container, name)
        if permissions is not None:
            self._check_permissions(account, container, name, permissions)

        # create account if it does not exist
        self._lookup_account(account, create=True)
        # check whether the container exists and lock the container path
        self._lookup_container(account, container)
        project = self._get_project(account, container)

        pre_version_id, dest_version_id = self._put_version_duplicate(
            user, account, container, name, src_node=src_node, size=size,
            type=type, hash=hash, checksum=checksum,
            available=available, keep_available=False)

        # Handle meta.
        if src_version_id is None:
            src_version_id = pre_version_id
        self._put_metadata_duplicate(
            src_version_id, dest_version_id, domain, account, container, name,
            meta, replace_meta)

        del_size = self._apply_versioning(account, container, pre_version_id)
        size_delta = size - del_size
        if size_delta > 0:
            # Check account quota.
            if not self.using_external_quotaholder:
                account_quota = long(self._get_policy(account)[QUOTA_POLICY])
                account_usage = self._get_statistics(account, container='',
                                                     compute=True)[1]
                if (account_quota > 0 and account_usage > account_quota):
                    raise QuotaError(
                        'Account quota exceeded: limit: %s, usage: %s' % (
                            account_quota, account_usage))

            # Check container quota.
            container_quota = long(self._get_policy(account,
                                                    container)[QUOTA_POLICY])
            container_usage = self._get_statistics(account, container)[1]
            if (container_quota > 0 and container_usage > container_quota):
                # This must be executed in a transaction, so the version is
                # never created if it fails.
                raise QuotaError(
                    'Container quota exceeded: limit: %s, usage: %s' % (
                        container_quota, container_usage
                    )
                )

        path = '/'.join([account, container, name])
        if report_size_change:
            self._report_size_change(
                user, account, size_delta, project,
                {'action': 'object update', 'path': path,
                 'versions': ','.join([str(dest_version_id)])})
        if permissions is not None:
            self.permissions.access_set(account,
                                        container,
                                        name,
                                        permissions)
            self._report_sharing_change(
                user, account, container, name,
                {'members': self.permissions.access_members(account,
                                                            container,
                                                            name)})

        self._report_object_change(
            user, account, path,
            details={'version': dest_version_id, 'action': 'object update'})
        return dest_version_id

    @debug_method
    @backend_method
    def register_object_map(self, user, account, container, name, size, type,
                            mapfile, checksum='', domain='pithos', meta=None,
                            replace_meta=False, permissions=None):
        """Register an object mapfile without providing any data.

        Lock the container path, create a node pointing to the object path,
        create a version pointing to the mapfile
        and issue the size change in the quotaholder.

        :param user: the user account which performs the action

        :param account: the account under which the object resides

        :param container: the container under which the object resides

        :param name: the object name

        :param size: the object size

        :param type: the object mimetype

        :param mapfile: the mapfile pointing to the object data

        :param checkcum: the md5 checksum (optional)

        :param domain: the object domain

        :param meta: a dict with custom object metadata

        :param replace_meta: replace existing metadata or not

        :param permissions: a dict with the read and write object permissions

        :returns: the new object uuid

        :raises: ItemNotExists, NotAllowedError, QuotaError
        """

        meta = meta or {}
        try:
            self.lock_container_path = True
            self.put_container(user, account, container, policy=None)
        except ContainerExists:
            pass
        finally:
            self.lock_container_path = False
        dest_version_id = self._update_object_hash(
            user, account, container, name, size, type, mapfile, checksum,
            domain, meta, replace_meta, permissions, available=False)
        return self.node.version_get_properties(dest_version_id,
                                                keys=('uuid',))[0]

    @debug_method
    def update_object_hashmap(self, user, account, container, name, size, type,
                              hashmap, checksum, domain, meta=None,
                              replace_meta=False, permissions=None):
        """Create/update an object's hashmap and return the new version."""

        for h in hashmap:
            if h.startswith('archip_'):
                raise IllegalOperationError(
                    'Cannot update Archipelago Volume hashmap.')
        meta = meta or {}
        if size == 0:  # No such thing as an empty hashmap.
            hashmap = [self.put_block('')]
        map = HashMap(self.block_size, self.hash_algorithm)
        map.extend([self._unhexlify_hash(x) for x in hashmap])
        missing = self.store.block_search(map)
        if missing:
            ie = IndexError()
            ie.data = [binascii.hexlify(x) for x in missing]
            raise ie

        hash = map.hash()
        hexlified = binascii.hexlify(hash)
        # _update_object_hash() locks destination path
        dest_version_id = self._update_object_hash(
            user, account, container, name, size, type, hexlified, checksum,
            domain, meta, replace_meta, permissions)
        self.store.map_put(hash, map)
        return dest_version_id, hexlified

    @debug_method
    @backend_method
    def update_object_checksum(self, user, account, container, name, version,
                               checksum):
        """Update an object's checksum."""

        # Changed behavior: this function used to update objects with greater
        # version and same hashmap and size in order to cover the versions
        # created by metadata updates
        # We no longer create a new version for metadata updates so we just
        # set update the specific version
        self._can_write_object(user, account, container, name)
        props = self._get_version(account, container, name, version)
        self.node.version_put_property(props[self.UUID], 'checksum', checksum)

    def _copy_object(self, user, src_account, src_container, src_name,
                     dest_account, dest_container, dest_name, type,
                     dest_domain=None, dest_meta=None, replace_meta=False,
                     permissions=None, src_version=None, is_move=False,
                     delimiter=None, listing_limit=10000):

        dest_meta = dest_meta or {}
        dest_version_ids = []
        self._can_read_object(user, src_account, src_container, src_name)

        src_container_path = '/'.join((src_account, src_container))
        dest_container_path = '/'.join((dest_account, dest_container))
        # Lock container paths in alphabetical order
        if src_container_path < dest_container_path:
            self._lookup_container(src_account, src_container)
            self._lookup_container(dest_account, dest_container)
        else:
            self._lookup_container(dest_account, dest_container)
            self._lookup_container(src_account, src_container)

        cross_account = src_account != dest_account
        cross_container = src_container != dest_container
        if not cross_account and cross_container:
            src_project = self._get_project(src_account, src_container)
            dest_project = self._get_project(dest_account, dest_container)
            cross_project = src_project != dest_project
        else:
            cross_project = False
        report_size_change = not is_move or cross_account or cross_project

        props = self._get_version(src_account, src_container, src_name,
                                  src_version)
        src = (props[self.ACCOUNT], props[self.CONTAINER], props[self.OBJ])
        src_version_id = props[self.UUID]
        hash = props[self.HASH]
        size = props[self.SIZE]
        uuid = self._update_object_hash(
            user, dest_account, dest_container, dest_name, size, type, hash,
            None, dest_domain, dest_meta, replace_meta, permissions,
            src_node=src, src_version_id=src_version_id,
            report_size_change=report_size_change)
        dest_version_ids.append(uuid)
        if is_move and ((src_account, src_container, src_name) !=
                        (dest_account, dest_container, dest_name)):
            self._delete_object(user, src_account, src_container, src_name,
                                report_size_change=report_size_change)

        if delimiter:
            prefix = (src_name + delimiter if not
                      src_name.endswith(delimiter) else src_name)
            objects = self._list_objects_no_limit(
                user, src_account, src_container, prefix, delimiter=None,
                virtual=False, domain=None, keys=[], shared=False, until=None,
                size_range=None, all_props=True, public=False,
                listing_limit=listing_limit)

            for props in objects:
                name = props[self.OBJ - 2]
                vtype = props[self.TYPE - 2]
                uuid = props[self.UUID - 2]
                src_meta = dict(self.node.attribute_get(uuid, domain='pithos'))
                uuid = self._copy_object(
                    user, src_account, src_container, name,
                    dest_account, dest_container, name.replace(src_name,
                                                               dest_name,
                                                               1),
                    type=vtype, dest_domain=dest_domain, dest_meta=src_meta,
                    replace_meta=False, permissions=None, src_version=uuid,
                    is_move=is_move, delimiter=None)
                dest_version_ids.append(uuid)
        return (dest_version_ids[0] if len(dest_version_ids) == 1 else
                dest_version_ids)

    @debug_method
    @backend_method
    def copy_object(self, user, src_account, src_container, src_name,
                    dest_account, dest_container, dest_name, type, domain,
                    meta=None, replace_meta=False, permissions=None,
                    src_version=None, delimiter=None, listing_limit=None):
        """Copy an object's data and metadata."""

        meta = meta or {}
        dest_version_id = self._copy_object(
            user, src_account, src_container, src_name, dest_account,
            dest_container, dest_name, type, domain, meta, replace_meta,
            permissions, src_version, False, delimiter,
            listing_limit=listing_limit)
        return dest_version_id

    @debug_method
    @backend_method
    def move_object(self, user, src_account, src_container, src_name,
                    dest_account, dest_container, dest_name, type, domain,
                    meta=None, replace_meta=False, permissions=None,
                    delimiter=None, listing_limit=None):
        """Move an object's data and metadata."""

        meta = meta or {}
        if user != src_account:
            raise NotAllowedError
        dest_version_id = self._move_object(
            user, src_account, src_container, src_name, dest_account,
            dest_container, dest_name, type, domain, meta, replace_meta,
            permissions, None, delimiter=delimiter,
            listing_limit=listing_limit)
        return dest_version_id

    def _delete_object(self, user, account, container, name, until=None,
                       delimiter=None, report_size_change=True,
                       listing_limit=None):
        if user != account:
            raise NotAllowedError

        # lock container path
        self._lookup_container(account, container)
        project = self._get_project(account, container)
        uuid = self._lookup_object(account, container, name)
        path = '/'.join([account, container, name])

        if until is not None:
            if uuid is None:
                return
            hashes = []
            size = 0
            uuids = []
            h, s, v = self.node.node_purge(account, container, name,
                                           before=until,
                                           cluster=CLUSTER_NORMAL)

            hashes += h
            size += s
            uuids += v
            h, s, v = self.node.node_purge(account, container, name,
                                           before=until,
                                           cluster=CLUSTER_HISTORY)

            hashes += h
            if not self.free_versioning:
                size += s
            uuids += v
            for h in hashes:
                self.store.map_delete(h)
            self.node.node_purge(account, container, name,
                                 before=until, cluster=CLUSTER_DELETED)
            try:
                self._get_version(account, container, name)
            except (ItemNotExists, VersionNotExists):
                self.permissions.access_clear(account, container, name)
            self._report_size_change(
                user, account, -size, project, {
                    'action': 'object purge',
                    'path': path,
                    'versions': ','.join(str(i) for i in uuids)
                }
            )
            return

        try:
            self._get_version(account, container, name)
        except ItemNotExists:
            raise ItemNotExists('Object is deleted.')

        src_version_id, dest_version_id = self._put_version_duplicate(
            user, account, container, name, size=0, type='', hash=None,
            checksum='', cluster=CLUSTER_DELETED)
        del_size = self._apply_versioning(account, container, src_version_id)

        if report_size_change:
            self._report_size_change(
                user, account, -del_size, project,
                {'action': 'object delete',
                 'path': path,
                 'versions': ','.join([str(dest_version_id)])})
        self._report_object_change(
            user, account, path, details={'action': 'object delete'})
        self.permissions.access_clear(account, container, name)
        self.node.node_remove(account, container, name)  # delete from nodes

        if delimiter:
            prefix = name + delimiter if not name.endswith(delimiter) else name
            src_names = self._list_objects_no_limit(
                user, account, container, prefix, delimiter=None,
                virtual=False, domain=None, keys=[], shared=False, until=None,
                size_range=None, all_props=True, public=False,
                listing_limit=listing_limit)
            paths = []
            for t in src_names:
                name = t[self.OBJ - 2]
                self._delete_object(user, account, container, name, until=None,
                                    delimiter=None,
                                    report_size_change=report_size_change)
                paths.append((account, container, name))
            self.permissions.access_clear_bulk(paths)

        # remove all the cached allowed paths
        # removing the specific path could be more expensive
        self._reset_allowed_paths()

    @debug_method
    @backend_method
    def delete_object(self, user, account, container, name, until=None,
                      delimiter=None, listing_limit=None):
        """Delete/purge an object."""

        self._delete_object(user, account, container, name, until, delimiter,
                            listing_limit=listing_limit)

    @debug_method
    @backend_method
    def list_versions(self, user, account, container, name):
        """Return a list of all object (version, version_timestamp) tuples."""

        self._can_read_object(user, account, container, name)
        versions = self.node.node_get_versions(account, container, name)
        filtered = [[x[self.UUID], x[self.MTIME]] for x in versions if
                    x[self.CLUSTER] != CLUSTER_DELETED]
        if not filtered:
            raise ItemNotExists('Object does not exist')
        return filtered

    @debug_method
    @backend_method
    def get_uuid(self, user, uuid, check_permissions=True):
        """Return the (account, container, name) for the UUID given."""

        info = self.node.latest_uuid(uuid, CLUSTER_NORMAL)
        if info is None:
            raise NameError
        account, container, name, _ = info
        if check_permissions:
            self._can_read_object(user, account, container, name)
        return (account, container, name)

    @debug_method
    @backend_method
    def get_public(self, user, public):
        """Return the (account, container, name) for the public id given."""

        t = self.permissions.public_path(public)
        if t is None:
            raise NameError
        account, container, name = t
        self._can_read_object(user, account, container, name)
        return (account, container, name)

    def get_block(self, hash):
        """Return a block's data."""

        logger.debug("get_block: %s", hash)
        if hash.startswith('archip_'):
            block = self.store.block_get_archipelago(hash)
        else:
            block = self.store.block_get(self._unhexlify_hash(hash))
        if not block:
            raise ItemNotExists('Block does not exist')
        return block

    def put_block(self, data):
        """Store a block and return the hash."""

        logger.debug("put_block: %s", len(data))
        return binascii.hexlify(self.store.block_put(data))

    def update_block(self, hash, data, offset=0):
        """Update a known block and return the hash."""

        logger.debug("update_block: %s %s %s", hash, len(data), offset)
        if hash.startswith('archip_'):
            raise IllegalOperationError(
                'Cannot update an Archipelago Volume block.')
        if offset == 0 and len(data) == self.block_size:
            return self.put_block(data)
        h = self.store.block_update(self._unhexlify_hash(hash), offset, data)
        return binascii.hexlify(h)

    # Path functions.

    def _generate_uuid(self):
        return str(uuidlib.uuid4())

    def _put_path(self, user, account, container='', obj=''):
        args = (account, container, obj, None, 0, '', None, user,
                self._generate_uuid(), '', time())
        uuid = self.node.node_create(*args)
        self.node.version_create(*args, cluster=CLUSTER_NORMAL)
        return uuid

    def _lookup_account(self, account, create=True):
        props = self.node.node_lookup(account, all_props=False)
        if props is None and create:
            uuid = self._put_path(account, account)
        else:
            uuid = props[0]
        return uuid

    def _lookup_container(self, account, container):
        for_update = True if self.lock_container_path else False
        props = self.node.node_lookup(account, container,
                                      for_update=for_update,
                                      all_props=False)
        if props is None:
            raise ItemNotExists('Container does not exist')
        uuid = props[0]
        return uuid

    def _lookup_object(self, account, container, name, lock_container=False):
        if lock_container:
            self._lookup_container(account, container)

        props = self.node.node_lookup(account, container, name,
                                      all_props=False)
        if props is None:
            raise ItemNotExists('Object does not exist')
        uuid = props[0]
        return uuid

    def _get_properties(self, account, container='', until=None,
                        for_update=False):
        """Return properties until the timestamp given."""

        if not until:
            props = self.node.node_lookup(account, container,
                                          for_update=for_update)
        else:
            props = self.node.version_lookup(account, container,
                                             before=until,
                                             cluster=CLUSTER_NORMAL)
            if props is None:
                props = self.node.version_lookup(account, container,
                                                 before=until,
                                                 cluster=CLUSTER_HISTORY)

        if props is None:
            raise ItemNotExists('Path does not exist')
        return props

    def _get_statistics(self, account, container='', until=None,
                        compute=False):
        """Return (count, sum of size, timestamp) of everything under node."""

        if until is not None:
            stats = self.node.statistics_latest(account, container, until,
                                                CLUSTER_DELETED)
        elif compute:
            stats = self.node.statistics_latest(account, container,
                                                except_cluster=CLUSTER_DELETED)
        else:
            stats = self.node.statistics_get(account, container,
                                             CLUSTER_NORMAL)
        if stats is None:
            stats = (0, 0, 0)
        return stats

    def _get_version(self, account, container, obj, version=None):
        if version is None:
            props = self.node.node_lookup(account, container, obj)
            if props is None:
                raise ItemNotExists('Object does not exist')
        else:
            props = self.node.version_get_properties(version,
                                                     account=account,
                                                     container=container,
                                                     obj=obj)
            if props is None or props[self.CLUSTER] == CLUSTER_DELETED:
                raise VersionNotExists('Version does not exist')
        return props

    def _get_versions(self, paths):
        return self.node.nodes_lookup_bulk(paths)

    def _put_version_duplicate(self, user, account, container, obj,
                               src_node=(),
                               size=None,
                               type=None, hash=None, checksum=None,
                               cluster=CLUSTER_NORMAL,
                               available=True, keep_available=True):
        """Create a new version of the node."""

        if src_node:
            if len(src_node) < 3:
                raise ValueError('Invalid source object')
            props = self.node.node_lookup(*src_node)
        else:
            props = self.node.node_lookup(account, container, obj)

        if props is not None:
            src_version_id = props[self.UUID]
            src_hash = props[self.HASH]
            src_size = props[self.SIZE]
            src_type = props[self.TYPE]
            src_checksum = props[self.CHECKSUM]
            if keep_available:
                src_available = props[self.AVAILABLE]
                src_map_check_timestamp = props[self.MAP_CHECK_TIMESTAMP]
            else:
                src_available = available
                src_map_check_timestamp = None
        else:
            src_version_id = None
            src_hash = None
            src_size = 0
            src_type = ''
            src_checksum = ''
            src_available = available
            src_map_check_timestamp = None
        if size is None:  # Set metadata.
            hash = src_hash  # This way hash can be set to None
            # (account or container).
            size = src_size
        if type is None:
            type = src_type
        if checksum is None:
            checksum = src_checksum
        uuid = self._generate_uuid()  # always update uuid

        if not src_node:
            pre_version_id = src_version_id
        else:
            pre_version_id = None
            props = self.node.node_lookup(account, container, obj)
            if props is not None:
                pre_version_id = props[self.UUID]
        if pre_version_id is not None:
            self.node.version_recluster(pre_version_id, CLUSTER_HISTORY)

        args = (account, container, obj, hash, size, type, src_version_id,
                user, uuid, checksum, time())

        node_exists = props is not None
        if not node_exists:
            #TODO: assert cluster == CLUSTER_NORMAL
            self.node.node_create(*args,
                                  available=src_available,
                                  map_check_timestamp=src_map_check_timestamp)
        else:
            if cluster == CLUSTER_NORMAL:
                self.node.node_update(
                    *args,
                    available=src_available,
                    map_check_timestamp=src_map_check_timestamp)
            else:
                self.node.node_remove(account, container, obj)

        self.node.version_create(*args,
                                 cluster=cluster,
                                 available=src_available,
                                 map_check_timestamp=src_map_check_timestamp)
        return pre_version_id, uuid

    def _put_metadata_duplicate(self, src_version_uuid, dest_version_uuid,
                                domain, account, container, obj, meta,
                                replace=False):
        if src_version_uuid is not None:
            self.node.attribute_copy(src_version_uuid, dest_version_uuid)
        # TODO test this!!!
        self._put_metadata(dest_version_uuid, domain, meta, account,
                           container, obj, replace)

    def _put_metadata(self, uuid, domain, meta, account, container='',
                      obj='', replace=False):
        """Create a new version and store metadata."""

        # Changed behavior: no new version duplicate is created
        # TODO bulk operations
        if not replace:
            self.node.attribute_del(uuid, domain, (
                k for k, v in meta.iteritems() if v == ''))
            self.node.attribute_set(uuid, domain, account,
                                    container, obj, ((k, v) for k, v in
                                                     meta.iteritems() if
                                                     v != ''))
        else:
            self.node.attribute_del(uuid, domain)
            self.node.attribute_set(uuid, domain, account,
                                    container, obj, ((k, v) for k, v in
                                                     meta.iteritems()))

    def _list_limits(self, listing, marker, limit):
        start = 0
        if marker:
            try:
                start = listing.index(marker) + 1
            except ValueError:
                pass
        if not limit or limit > 10000:
            limit = 10000
        return start, limit

    def _list_object_properties(self, account, container='', prefix='',
                                delimiter=None,
                                marker=None, limit=10000, virtual=True,
                                domain=None, keys=None, until=None,
                                size_range=None, allowed=None,
                                all_props=False):
        keys = keys or []
        allowed = allowed or []
        start = marker or ''
        before = until if until is not None else inf
        filterq = keys if domain else []
        sizeq = size_range

        objects, prefixes = self.node.latest_version_list(
            account, container, prefix=prefix, delimiter=delimiter,
            start=start, limit=limit, before=before,
            except_cluster=CLUSTER_DELETED, pathq=allowed, domain=domain,
            filterq=filterq, sizeq=sizeq, all_props=all_props)
        objects.extend(prefixes if virtual else [])
        objects.sort(key=lambda x: x[self.OBJ])
        objects = [(x[self.OBJ],) + x[self.OBJ + 1:] for x in objects]
        return objects

    # Reporting functions.

    @debug_method
    @backend_method
    def _report_size_change(self, user, account, size, source, details=None):
        details = details or {}

        if size == 0:
            return

        total = self._get_statistics(account, container='', compute=True)[1]
        details.update({'user': user, 'total': total})
        self.messages.append(
            (QUEUE_MESSAGE_KEY_PREFIX % ('resource.diskspace',),
             account, QUEUE_INSTANCE_ID, 'diskspace', float(size), details))

        if not self.using_external_quotaholder:
            return

        try:
            name = details['path'] if 'path' in details else ''
            serial = self.astakosclient.issue_one_commission(
                holder=account,
                source=source,
                provisions={'pithos.diskspace': size},
                name=name)
        except BaseException, e:
            raise QuotaError(e)
        else:
            self.serials.append(serial)

    @debug_method
    @backend_method
    def _report_object_change(self, user, account, path, details=None):
        details = details or {}
        details.update({'user': user})
        self.messages.append((QUEUE_MESSAGE_KEY_PREFIX % ('object',),
                              account, QUEUE_INSTANCE_ID, 'object', path,
                              details))

    @debug_method
    @backend_method
    def _report_sharing_change(self, user, account, container, obj,
                               details=None):
        details = details or {}
        details.update({'user': user})
        path = '/'.join([account, container, obj])
        self.messages.append((QUEUE_MESSAGE_KEY_PREFIX % ('sharing',),
                              account, QUEUE_INSTANCE_ID, 'sharing', path,
                              details))

    # Policy functions.

    def _check_project(self, value):
        # raise ValueError('Bad quota source policy')
        pass

    def _check_policy(self, policy):
        for k, v in policy.iteritems():
            if k == QUOTA_POLICY:
                q = int(v)  # May raise ValueError.
                if q < 0:
                    raise ValueError
            elif k == VERSIONING_POLICY:
                if v not in ['auto', 'none']:
                    raise ValueError
            elif k == PROJECT:
                self._check_project(v)
            else:
                raise ValueError

    def _get_default_policy(self, account, container='',
                            default_project=None):
        is_account_policy = container is None
        if is_account_policy:
            default_policy = self.default_account_policy
        else:
            default_policy = self.default_container_policy
            if default_project is None:
                # set container's account as the default quota source
                default_project = account
            default_policy[PROJECT] = default_project
        return default_policy

    def _put_policy(self, account, container, policy, replace,
                    default_project=None,
                    check=True):
        default_policy = self._get_default_policy(account,
                                                  container,
                                                  default_project)
        if replace:
            for k, v in default_policy.iteritems():
                if k not in policy:
                    policy[k] = v
        if check:
            self._check_policy(policy)

        self.node.policy_set(account, container, policy)

    def _get_policy(self, account, container='', default_project=None):
        default_policy = self._get_default_policy(account,
                                                  container,
                                                  default_project)
        policy = default_policy.copy()
        policy.update(self.node.policy_get(account, container))
        return policy

    def _get_project(self, account, container):
        policy = self._get_policy(account, container)
        return policy[PROJECT]

    def _apply_versioning(self, account, container, version_id):
        """Delete the provided version if such is the policy.
           Return size of object removed.
        """

        if version_id is None:
            return 0
        versioning = self._get_policy(account, container)[VERSIONING_POLICY]
        if versioning != 'auto':
            hash, size = self.node.version_remove(version_id)
            self.store.map_delete(hash)
            return size
        elif self.free_versioning:
            return self.node.version_get_properties(
                version_id, keys=('size',))[0]
        return 0

    # Access control functions.

    def _check_groups(self, groups):
        # raise ValueError('Bad characters in groups')
        pass

    def _check_permissions(self, account, container, obj, permissions):
        # raise ValueError('Bad characters in permissions')
        pass

    def _get_formatted_paths(self, paths, match_prefix=True):
        formatted = []
        if len(paths) == 0:
            return formatted
        props = self.node.node_lookup_bulk(paths)
        if not props:
            return []

        for p in props:
            path = '/'.join([p[self.ACCOUNT],
                             p[self.CONTAINER],
                             p[self.OBJ]]).rstrip('/')
            if match_prefix and p[self.TYPE].split(';', 1)[0].strip() in (
                    'application/directory', 'application/folder'):
                formatted.append((path + '/', self.MATCH_PREFIX))
            formatted.append((path, self.MATCH_EXACT))
        return formatted

    def _get_permissions_path(self, account, container, name):
        permission_paths = self.permissions.access_inherit(account,
                                                           container,
                                                           name)
        permission_paths.sort()
        permission_paths.reverse()
        for p in permission_paths:
            if p == (account, container, name):
                return p
            else:
                props = self.node.node_lookup(*p)
                if props is not None:
                    if props[self.TYPE].split(';', 1)[0].strip() in (
                            'application/directory', 'application/folder'):
                        return p
        return None

    def _get_permissions_path_bulk(self, account, container, names):
        formatted_paths = []
        for name in names:
            formatted_paths.append((account, container, name))
        permission_paths = self.permissions.access_inherit_bulk(
            account, container, names)
        permission_paths.sort()
        permission_paths.reverse()
        permission_paths_list = []
        lookup_list = []
        for p in permission_paths:
            if p in formatted_paths:
                permission_paths_list.append(p)
            else:
                if p.count('/') < 2:
                    continue
                lookup_list.append(p)

        if len(lookup_list) > 0:
            props = self.node.node_lookup_bulk(lookup_list)
            if props:
                for prop in props:
                    if prop[self.TYPE].split(';', 1)[0].strip() in (
                            'application/directory', 'application/folder'):
                        permission_paths_list.append(prop[0])

        if len(permission_paths_list) > 0:
            return permission_paths_list

        return None

    def _reset_allowed_paths(self):
        self.read_allowed_paths = defaultdict(set)
        self.write_allowed_paths = defaultdict(set)

    @check_allowed_paths(action=0)
    def _can_read_account(self, user, account):
        if user != account:
            if account not in self._allowed_accounts(user):
                raise NotAllowedError

    @check_allowed_paths(action=1)
    def _can_write_account(self, user, account):
        if user != account:
            raise NotAllowedError

    @check_allowed_paths(action=0)
    def _can_read_container(self, user, account, container):
        if user != account:
            if container not in self._allowed_containers(user, account):
                raise NotAllowedError

    @check_allowed_paths(action=1)
    def _can_write_container(self, user, account, container):
        if user != account:
            raise NotAllowedError

    def can_write_container(self, user, account, container):
        return self._can_write_container(user, account, container)

    @check_allowed_paths(action=0)
    def _can_read_object(self, user, account, container, name):
        if user == account:
            return True
        if self.permissions.public_get(account, container, name) is not None:
            return True
        path = self._get_permissions_path(account, container, name)
        if not path:
            raise NotAllowedError
        if (not self.permissions.access_check(*path, access=self.READ,
                                              member=user)
            and not self.permissions.access_check(*path, access=self.WRITE,
                                                  member=user)):
            raise NotAllowedError

    @check_allowed_paths(action=1)
    def _can_write_object(self, user, account, container, name):
        if user == account:
            return True
        path = self._get_permissions_path(account, container, name)
        if not path:
            raise NotAllowedError
        if not self.permissions.access_check(*path, access=self.WRITE,
                                             member=user):
            raise NotAllowedError

    def _allowed_accounts(self, user):
        allow = set()
        map(allow.add, [acc for acc, container, obj in
                        self.permissions.access_list_paths(user)])
        self.read_allowed_paths[user] |= allow
        return sorted(allow)

    def _allowed_containers(self, user, account):
        allow = set()
        map(allow.add, [container for acc, container, obj in
                        self.permissions.access_list_paths(user, account)])
        self.read_allowed_paths[user] |= allow
        return sorted(allow)

    # Domain functions

    @debug_method
    @backend_method
    def get_domain_objects(self, domain, user=None):
        allowed_paths = self.permissions.access_list_paths(
            user, include_owned=user is not None, include_containers=False)
        if not allowed_paths:
            return []
        obj_list = self.node.domain_object_list(domain, allowed_paths)
        return [('/'.join([account, container, obj]),
                 self._build_metadata(props, user_defined_meta),
                 self.permissions.access_get(account, container, obj)) for
                account, container, obj, props, user_defined_meta in obj_list]

    # util functions

    def _build_metadata(self, props, user_defined=None,
                        include_user_defined=True):
        meta = {'bytes': props[self.SIZE],
                'type': props[self.TYPE],
                'hash': props[self.HASH],
                'version': props[self.UUID],
                'version_timestamp': props[self.MTIME],
                'modified_by': props[self.MUSER],
                'uuid': props[self.UUID],
                'checksum': props[self.CHECKSUM]}
        if include_user_defined and user_defined is not None:
            meta.update(user_defined)
        return meta

    def _unhexlify_hash(self, hash):
        try:
            return binascii.unhexlify(hash)
        except TypeError:
            raise InvalidHash(hash)
