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

from time import time
from operator import itemgetter
from collections import defaultdict

from sqlalchemy import (Table, Integer, BigInteger, DECIMAL,
                        Column, String, MetaData, Boolean)
from sqlalchemy.schema import Index
from sqlalchemy.sql import func, and_, or_, not_, select, bindparam, exists
from sqlalchemy.sql.expression import case, literal, desc
from sqlalchemy.sql.functions import concat
from sqlalchemy.exc import NoSuchTableError

from dbworker import DBWorker, ESCAPE_CHAR

from pithos.backends.filter import parse_filters

import itertools

DEFAULT_DISKSPACE_RESOURCE = 'pithos.diskspace'

(ACCOUNT, CONTAINER, OBJ, HASH, SIZE, TYPE, SOURCE, MTIME, MUSER, UUID,
 CHECKSUM, CLUSTER, AVAILABLE, MAP_CHECK_TIMESTAMP) = range(14)

(MATCH_PREFIX, MATCH_EXACT) = range(2)

inf = float('inf')


def strnextling(prefix):
    """Return the first unicode string
       greater than but not starting with given prefix.
       strnextling('hello') -> 'hellp'
    """
    if not prefix:
        ## all strings start with the null string,
        ## therefore we have to approximate strnextling('')
        ## with the last unicode character supported by python
        ## 0x10ffff for wide (32-bit unicode) python builds
        ## 0x00ffff for narrow (16-bit unicode) python builds
        ## We will not autodetect. 0xffff is safe enough.
        return unichr(0xffff)
    s = prefix[:-1]
    c = ord(prefix[-1])
    if c >= 0xffff:
        raise RuntimeError
    s += unichr(c + 1)
    return s


def strprevling(prefix):
    """Return an approximation of the last unicode string
       less than but not starting with given prefix.
       strprevling(u'hello') -> u'helln\\xffff'
    """
    if not prefix:
        ## There is no prevling for the null string
        return prefix
    s = prefix[:-1]
    c = ord(prefix[-1])
    if c > 0:
        s += unichr(c - 1) + unichr(0xffff)
    return s

_props = ('account', 'container', 'obj', 'hash', 'size', 'type', 'source',
          'mtime', 'muser', 'uuid', 'checksum', 'cluster',
          'available', 'map_check_timestamp')
_propnames = dict((_props[i], i) for i in range(len(_props)))


def create_tables(engine):
    metadata = MetaData()

    #create nodes table
    columns = []
    columns.append(Column('account', String(256), nullable=False, default=''))
    columns.append(Column('container', String(256), nullable=False,
                          default=''))
    columns.append(Column('obj', String(1024), nullable=False, default=''))
    columns.append(Column('hash', String(256)))
    columns.append(Column('size', BigInteger, nullable=False, default=0))
    columns.append(Column('type', String(256), nullable=False, default=''))
    columns.append(Column('source', String(64)))
    columns.append(Column('mtime', DECIMAL(precision=16, scale=6)))
    columns.append(Column('muser', String(256), nullable=False, default=''))
    columns.append(Column('uuid', String(64), primary_key=True))
    columns.append(Column('checksum', String(256), nullable=False, default=''))
    columns.append(Column('available', Boolean, nullable=False, default=True))
    columns.append(Column('map_check_timestamp', DECIMAL(precision=16,
                                                         scale=6)))
    nodes = Table('nodes', metadata, *columns, mysql_engine='InnoDB')
    Index('idx_nodes_path_2', nodes.c.account, nodes.c.container, nodes.c.obj,
          unique=True)

    #create policy table
    columns = []
    columns.append(Column('account', String(256), nullable=False,
                          primary_key=True))
    columns.append(Column('container', String(256), nullable=True,
                          primary_key=True))
    columns.append(Column('key', String(128), primary_key=True))
    columns.append(Column('value', String(256)))
    Table('policy', metadata, *columns, mysql_engine='InnoDB')

    #create statistics table
    columns = []
    columns.append(Column('account', String(256), nullable=False,
                          primary_key=True))
    columns.append(Column('container', String(256), nullable=False,
                          primary_key=True, default=''))
    columns.append(Column('population', Integer, nullable=False, default=0))
    columns.append(Column('size', BigInteger, nullable=False, default=0))
    columns.append(Column('mtime', DECIMAL(precision=16, scale=6)))
    Table('statistics', metadata, *columns, mysql_engine='InnoDB')

    #create v_statistics table
    columns = []
    columns.append(Column('account', String(256), nullable=False,
                          primary_key=True))
    columns.append(Column('container', String(256), nullable=False,
                          primary_key=True, default=''))
    columns.append(Column('population', Integer, nullable=False, default=0))
    columns.append(Column('size', BigInteger, nullable=False, default=0))
    columns.append(Column('mtime', DECIMAL(precision=16, scale=6)))
    columns.append(Column('cluster', Integer, nullable=False, default=0,
                          primary_key=True))
    Table('v_statistics', metadata, *columns, mysql_engine='InnoDB')

    #create versions table
    columns = []
    columns.append(Column('account', String(256), nullable=False, default=''))
    columns.append(Column('container', String(256), nullable=True, default=''))
    columns.append(Column('obj', String(1024), nullable=True, default=''))
    columns.append(Column('hash', String(256)))
    columns.append(Column('size', BigInteger, nullable=False, default=0))
    columns.append(Column('type', String(256), nullable=False, default=''))
    columns.append(Column('source', String(64)))
    columns.append(Column('mtime', DECIMAL(precision=16, scale=6)))
    columns.append(Column('muser', String(256), nullable=False, default=''))
    columns.append(Column('uuid', String(64), primary_key=True))
    columns.append(Column('checksum', String(256), nullable=False, default=''))
    columns.append(Column('cluster', Integer, nullable=False, default=0))
    columns.append(Column('available', Boolean, nullable=False, default=True))
    columns.append(Column('map_check_timestamp', DECIMAL(precision=16,
                                                         scale=6)))
    versions = Table('versions', metadata, *columns, mysql_engine='InnoDB')
    Index('idx_versions_path_mtime', versions.c.account, versions.c.container,
          versions.c.obj, versions.c.mtime)

    #create attributes table
    columns = []
    columns.append(Column('uuid', String(64), primary_key=True))
    columns.append(Column('account', String(256), nullable=False, default=''))
    columns.append(Column('container', String(256), nullable=True, default=''))
    columns.append(Column('obj', String(1024), nullable=True, default=''))
    columns.append(Column('domain', String(256), primary_key=True))
    columns.append(Column('key', String(128), primary_key=True))
    columns.append(Column('value', String(256)))
    attributes = Table('attributes', metadata, *columns, mysql_engine='InnoDB')
    Index('idx_attributes_domain', attributes.c.domain)
    Index('idx_attributes_path2', attributes.c.account, attributes.c.container,
          attributes.c.obj)

    #create v_attributes table
    columns = []
    columns.append(Column('uuid', String(64), primary_key=True))
    columns.append(Column('account', String(256), nullable=False, default=''))
    columns.append(Column('container', String(256), nullable=True, default=''))
    columns.append(Column('obj', String(1024), nullable=True, default=''))
    columns.append(Column('domain', String(256), primary_key=True))
    columns.append(Column('key', String(128), primary_key=True))
    columns.append(Column('value', String(256)))
    columns.append(Column('mtime', DECIMAL(precision=16, scale=6),
                          primary_key=True))
    v_attributes = Table('v_attributes', metadata, *columns,
                         mysql_engine='InnoDB')
    Index('idx_v_attributes_domain', v_attributes.c.domain)
    Index('idx_v_attributes_path2', v_attributes.c.account,
          v_attributes.c.container, v_attributes.c.obj)
    Index('idx_v_attributes_uuid', v_attributes.c.uuid, v_attributes.c.mtime)
    metadata.create_all(engine)
    return metadata.sorted_tables


class Node(DBWorker):
    """Nodes store path organization and have multiple versions.
       Versions store object history and have multiple attributes.
       Attributes store metadata.
    """

    # TODO: Provide an interface for included and excluded clusters.

    def __init__(self, **params):
        DBWorker.__init__(self, **params)
        try:
            metadata = MetaData(self.engine)
            self.nodes = Table('nodes', metadata, autoload=True)
            self.policy = Table('policy', metadata, autoload=True)
            self.statistics = Table('statistics', metadata, autoload=True)
            self.v_statistics = Table('v_statistics', metadata, autoload=True)
            self.versions = Table('versions', metadata, autoload=True)
            self.attributes = Table('attributes', metadata, autoload=True)
            self.v_attributes = Table('v_attributes', metadata, autoload=True)
        except NoSuchTableError:
            tables = create_tables(self.engine)
            map(lambda t: self.__setattr__(t.name, t), tables)

    def _latest_versions_uuid_expr(self, where_clause):
        s = select([func.max(self.versions.c.mtime).label('max_mtime'),
                    self.versions.c.account,
                    self.versions.c.container,
                    self.versions.c.obj])
        s = s.group_by(self.versions.c.account,
                       self.versions.c.container,
                       self.versions.c.obj).alias('s')
        join = self.versions.join(
            s, onclause=(and_(self.versions.c.mtime == s.c.max_mtime,
                              self.versions.c.account == s.c.account,
                              self.versions.c.container == s.c.container,
                              self.versions.c.obj == s.c.obj)))
        return select([self.versions.c.uuid],
                      from_obj=join).where(where_clause)

    def _latest_obj_version_uuid_expr(self, account, container, obj='',
                                      before=inf):
        s = select([self.versions.c.uuid],
                   and_(self.versions.c.account == account,
                        self.versions.c.container == container,
                        self.versions.c.obj == obj,
                        self.versions.c.mtime < before))
        return s.order_by(desc(self.versions.c.mtime)).limit(1)

    def _path_expr(self, t, path):
        parts = path.split('/', 2)
        if len(parts) == 1:
            c = t.c.account
        elif len(parts) == 2:
            c = concat(t.c.account, '/', t.c.container)
        else:
            c = concat(t.c.account, '/', t.c.container, '/', t.c.obj)
        return c

    def _path_like_expr(self, t, path):
        return self._path_expr(t, path).like(self.escape_like(path) + '%',
                                             escape=ESCAPE_CHAR)

    def node_create(self, account, container, obj, hash, size, type, source,
                    muser, uuid, checksum, mtime=None,
                    available=True, map_check_timestamp=None):
        """Create a new node from the given properties.
           Return the node identifier of the new node.
        """

        mtime = mtime or time()
        #TODO catch IntegrityError?
        s = self.nodes.insert().values(account=account, container=container,
                                       obj=obj, hash=hash, size=size,
                                       type=type, source=source, mtime=mtime,
                                       muser=muser, uuid=uuid,
                                       checksum=checksum,
                                       available=available,
                                       map_check_timestamp=map_check_timestamp)
        r = self.conn.execute(s)
        r.close()
        return uuid

    def node_update(self, account, container, obj, hash, size, type, source,
                    muser, uuid, checksum, mtime=None,
                    available=True, map_check_timestamp=None):
        u = self.nodes.update().where(and_(self.nodes.c.account == account,
                                           self.nodes.c.container == container,
                                           self.nodes.c.obj == obj))
        self.conn.execute(u, hash=hash, size=size, type=type, source=source,
                          mtime=mtime, muser=muser, uuid=uuid,
                          checksum=checksum,
                          available=available,
                          map_check_timestamp=map_check_timestamp)

    def node_lookup(self, account, container='', obj='', for_update=False,
                    all_props=True):
        """Lookup the current node of the given path.
           Return None if the path is not found.
        """

        if not all_props:
            columns = [self.nodes.c.uuid]
        else:
            columns = [self.nodes.c.account, self.nodes.c.container,
                       self.nodes.c.obj,
                       self.nodes.c.hash, self.nodes.c.size,
                       self.nodes.c.type, self.nodes.c.source,
                       self.nodes.c.mtime, self.nodes.c.muser,
                       self.nodes.c.uuid, self.nodes.c.checksum,
                       literal(0).label('cluster'),
                       self.nodes.c.available,
                       self.nodes.c.map_check_timestamp]
        s = select(columns,
                   and_(self.nodes.c.account == account,
                        self.nodes.c.container == container,
                        self.nodes.c.obj == obj), for_update=for_update)
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        if row:
            return row
        return None

    def node_lookup_bulk(self, paths, all_props=True):
        """Lookup the current nodes for the given paths.
           Return () if the path is not found.
        """

        if not paths:
            return ()

        if not all_props:
            columns = [self.nodes.c.uuid]
        else:
            columns = [self.nodes.c.account, self.nodes.c.container,
                       self.nodes.c.obj,
                       self.nodes.c.hash, self.nodes.c.size,
                       self.nodes.c.type, self.nodes.c.source,
                       self.nodes.c.mtime, self.nodes.c.muser,
                       self.nodes.c.uuid, self.nodes.c.checksum,
                       literal(0).label('cluster')]
        s = select(columns,
                   or_(*[and_(self.nodes.c.account == account,
                              self.nodes.c.container == container,
                              self.nodes.c.obj == obj) for (account,
                                                            container,
                                                            obj) in paths]))
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        if not all_props:
            return [row[0] for row in rows]
        else:
            return rows

    def node_get_versions(self, account, container, obj,
                          keys=(), propnames=_propnames):
        """Return the properties of all versions at node.
           If keys is empty, return all properties in the order
           (account, container, obj, hash, size, type, source, mtime, muser,
            uuid, checksum, cluster, available, map_check_timestamp).
        """

        s = select([self.versions.c.account,
                    self.versions.c.container,
                    self.versions.c.obj,
                    self.versions.c.hash,
                    self.versions.c.size,
                    self.versions.c.type,
                    self.versions.c.source,
                    self.versions.c.mtime,
                    self.versions.c.muser,
                    self.versions.c.uuid,
                    self.versions.c.checksum,
                    self.versions.c.cluster,
                    self.versions.c.available,
                    self.versions.c.map_check_timestamp],
                   and_(self.versions.c.account == account,
                        self.versions.c.container == container,
                        self.versions.c.obj == obj))
        s = s.order_by(self.versions.c.mtime)
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        if not rows:
            return rows

        if not keys:
            return rows

        return [[p[propnames[k]] for k in keys if k in propnames] for
                p in rows]

    def node_count_children(self, account, container=''):
        """Return node's child count."""

        s = select([func.count(self.nodes.c.uuid)],
                   self.nodes.c.account == account)
        if container:  # look for objects
            condition = and_(self.nodes.c.container == container,
                             self.nodes.c.obj != '')
        else:  # look for containers
            condition = and_(self.nodes.c.container != '',
                             self.nodes.c.obj == '')

        s = s.where(condition)
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        return row[0]

    def node_purge_children(self, account, container, before=inf, cluster=0):
        """Delete all versions with the specified
           parent and cluster, and return
           the hashes, the total size and the uuids of versions deleted.
           Clears out nodes with no remaining versions.
        """
        #update statistics
        where_clause = and_(self.versions.c.account == account,
                            self.versions.c.container == container,
                            self.versions.c.obj != '',
                            self.versions.c.cluster == cluster)
        if before != inf:
            where_clause = and_(where_clause,
                                self.versions.c.mtime <= before)
        s = select([func.count(self.versions.c.uuid),
                    func.sum(self.versions.c.size)])
        s = s.where(where_clause)
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        if not row:
            return (), 0, ()
        nr, size = row[0], row[1] if row[1] else 0
        mtime = time()
        self.statistics_update(account, container, -nr, -size, mtime, cluster)

        s = select([self.versions.c.hash, self.versions.c.uuid])
        s = s.where(where_clause)
        r = self.conn.execute(s)
        hashes = []
        uuids = []
        for row in r.fetchall():
            hashes += [row[0]]
            uuids += [row[1]]
        r.close()

        #delete versions
        s = self.versions.delete().where(where_clause)
        r = self.conn.execute(s)
        r.close()

        #delete nodes
        s = select([self.nodes.c.uuid],
                   and_(select([func.count(self.versions.c.uuid)],
                               and_(self.versions.c.account ==
                                    self.nodes.c.account,
                                    self.versions.c.container ==
                                    self.nodes.c.container,
                                    self.versions.c.obj ==
                                    self.nodes.c.obj)).as_scalar() == 0))
        d = self.nodes.delete().where(self.nodes.c.uuid.in_(s))
        self.conn.execute(d).close()

        return hashes, size, uuids

    def node_purge(self, account, container, obj, before=inf, cluster=0):
        """Delete all versions with the specified
           node and cluster, and return
           the hashes and size of versions deleted.
           Clears out the node if it has no remaining versions.
        """

        #update statistics
        s = select([func.count(self.versions.c.uuid),
                    func.sum(self.versions.c.size)])
        where_clause = and_(self.versions.c.account == account,
                            self.versions.c.container == container,
                            self.versions.c.obj == obj,
                            self.versions.c.cluster == cluster)
        if before != inf:
            where_clause = and_(where_clause,
                                self.versions.c.mtime <= before)
        if obj != '':
            s = s.where(where_clause)
            r = self.conn.execute(s)
            row = r.fetchone()
            nr, size = row[0], row[1]
            r.close()
            if not nr:
                return (), 0, ()
            mtime = time()

            self.statistics_update_ancestor(account, container, -nr, -size,
                                            mtime, cluster)

        s = select([self.versions.c.hash, self.versions.c.uuid])
        s = s.where(where_clause)
        r = self.conn.execute(s)
        hashes = []
        uuids = []
        for row in r.fetchall():
            hashes += [row[0]]
            uuids += [row[1]]
        r.close()

        #delete versions
        s = self.versions.delete().where(where_clause)
        r = self.conn.execute(s)
        r.close()

        #delete nodes
        s = select([self.nodes.c.uuid],
                   select([func.count(self.versions.c.uuid)],
                          and_(self.versions.c.account == account,
                               self.versions.c.container == container,
                               self.versions.c.obj == obj)).as_scalar() == 0)
        s = self.nodes.delete().where(self.nodes.c.uuid.in_(s))
        self.conn.execute(s).close()

        return hashes, size, uuids

    def node_remove(self, account, container='', obj=''):
        """Remove the node specified.
           Return false if the node has children or is not found.
        """

        is_object = obj != ''
        if not is_object and self.node_count_children(account, container) != 0:
            return False

        mtime = time()
        s = select([func.count(self.versions.c.uuid),
                    func.sum(self.versions.c.size),
                    self.versions.c.cluster])
        s = s.where(and_(self.versions.c.account == account,
                         self.versions.c.container == container,
                         self.versions.c.obj == obj))
        s = s.group_by(self.versions.c.cluster)
        r = self.conn.execute(s)

        if obj != '':
            for population, size, cluster in r.fetchall():
                self.statistics_update_ancestor(
                    account, container, -population, -size, mtime, cluster)

        r.close()

        s = self.nodes.delete().where(and_(self.nodes.c.account == account,
                                           self.nodes.c.container == container,
                                           self.nodes.c.obj == obj))
        self.conn.execute(s).close()
        return True

    def node_accounts(self, accounts=()):
        s = select([self.nodes.c.account],
                   self.nodes.c.account != '').distinct()
        if accounts:
            s = s.where(self.nodes.c.account.in_(accounts))
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        return rows

    def node_account_quotas(self):
        s = select([self.policy.c.account, self.policy],
                   and_(self.policy.c.container == '',
                        self.policy.c.key == 'quota'))
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        return dict(rows)

    def node_account_usage(self, account=None, project=None, cluster=0):
        """Return usage for a specific account or project.

        Keyword arguments:
        account -- (default None: list usage for all accounts)
        project -- (default None: list usage for all projects)
        cluster -- list current, history or deleted usage (default 0: normal)
        """

        if cluster == 0:
            table = self.nodes
        else:
            table = self.versions
        s = select([self.policy.c.account, self.policy.c.value,
                    select([func.sum(table.c.size)],
                           and_(table.c.account == self.policy.c.account,
                                table.c.account != '',
                                table.c.container != '',
                                table.c.obj != '')).label('usage')],
                   self.policy.c.key == 'project')
        if account:
            s = s.where(self.policy.c.account == account)
        if project:
            s = s.where(self.policy.c.value == project)

        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        d = defaultdict(lambda: defaultdict(dict))
        for account, project, usage in rows:
            d[account][project][DEFAULT_DISKSPACE_RESOURCE] = usage
        return d

    def policy_get(self, account, container):
        s = select([self.policy.c.key, self.policy.c.value],
                   and_(self.policy.c.account == account,
                        self.policy.c.container == container))
        r = self.conn.execute(s)
        d = dict(r.fetchall())
        r.close()
        return d

    def policy_set(self, account, container, policy):
        # TODO upsert
        for k, v in policy.iteritems():
            s = self.policy.update().where(and_(
                self.policy.c.account == account,
                self.policy.c.container == container,
                self.policy.c.key == k))
            s = s.values(value=v)
            rp = self.conn.execute(s)
            rp.close()
            if rp.rowcount == 0:
                s = self.policy.insert()
                values = {'account': account, 'container': container,
                          'key': k, 'value': v}
                r = self.conn.execute(s, values)
                r.close()

    def statistics_get(self, account, container, cluster=0):
        """Return population, total size and last mtime
           for all versions under node that belong to the cluster.
        """

        if cluster == 0:
            table = self.statistics
        else:
            table = self.v_statistics
        s = select([table.c.population,
                    table.c.size,
                    table.c.mtime])
        s = s.where(and_(table.c.account == account,
                         table.c.container == container))
        if cluster != 0:
            s = s.where(table.c.cluster == cluster)
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        return row

    def statistics_update(self, account, container, population, size, mtime,
                          cluster=0):
        """Update the statistics of the given node.
           Statistics keep track the population, total
           size of objects and mtime in the node's namespace.
           May be zero or positive or negative numbers.
        """

        # TODO do we really need this?
        if cluster == 0:
            # update both tables
            tables = (self.statistics, self.v_statistics)
        else:
            # update just v_statistics
            tables = (self.v_statistics,)

        for table in tables:
            #TODO upsert
            u = table.update().where(and_(
                table.c.account == account,
                table.c.container == container))
            if cluster != 0:
                u = u.where(table.c.cluster == cluster)
            u = u.values(population=case([(table.c.population + population > 0,
                                           table.c.population + population)],
                                         else_=0),
                         size=table.c.size + size,
                         mtime=mtime)
            rp = self.conn.execute(u)
            rp.close()
            if rp.rowcount == 0:
                ins = table.insert()
                if table == self.statistics:
                    ins = ins.values(account=account, container=container,
                                     population=population, size=size,
                                     mtime=mtime)
                else:
                    ins = ins.values(account=account, container=container,
                                     population=population, size=size,
                                     mtime=mtime, cluster=cluster)
                self.conn.execute(ins).close()

    def statistics_update_ancestor(self, account, container, population, size,
                                   mtime, cluster=0):
        """Update the statistics of the given node's parent.
           Then recursively update all parents up to the root
           or up to the ``recursion_depth`` (if not None).
           Population is not recursive.
        """

        # update the container
        self.statistics_update(account, container, population, size, mtime,
                               cluster)
        # Do not update account statistics (always compute them) due to issues
        # that may occur with concurrent updates in several containers under
        # the account

    def statistics_latest(self, account, container, before=inf,
                          except_cluster=0):
        """Return population, total size and last mtime
           for all latest versions under node that
           do not belong to the cluster.
        """

        path = '/'.join([account, container]).rstrip('/')

        # The latest version.
        if before != inf:
            t = self.versions
            filtered = self._latest_obj_version_uuid_expr(account, container,
                                                          obj='',
                                                          before=before)
            where_clause = and_(t.c.cluster != except_cluster,
                                t.c.uuid == filtered)
        else:
            t = self.nodes
            where_clause = and_(t.c.account == account,
                                t.c.container == container,
                                t.c.obj == '')
        s = select([t.c.mtime, t.c.size], where_clause)
        r = self.conn.execute(s)
        props = r.fetchone()
        r.close()
        if not props:
            return None
        mtime = props.mtime

        # First level, just under node (get population).
        if before != inf:
            t = self.versions.alias('v')
            c1 = self._latest_obj_version_uuid_expr(t.c.account, t.c.container,
                                                    t.c.obj, before)
            where_clause = and_(t.c.uuid == c1,
                                t.c.cluster != except_cluster,
                                t.c.account == account)
        else:
            t = self.nodes
            where_clause = t.c.account == account
        if container == '':
            where_clause = and_(where_clause, and_(t.c.container != '',
                                                   t.c.obj == ''))
        else:
            where_clause = and_(where_clause, and_(t.c.container == container,
                                                   t.c.obj != ''))
        s = select([func.count(t.c.uuid).label('population'),
                    func.max(t.c.mtime).label('mtime')],
                   where_clause)
        rp = self.conn.execute(s)
        r = rp.fetchone()
        rp.close()
        if not r:
            return None
        count = r.population
        mtime = max(mtime, r.mtime)
        if count == 0:
            return (0, 0, mtime)

        # All children (get size and mtime).
        # This is why the full path is stored.
        if before != inf:
            t = self.versions.alias('v')
            c1 = self._latest_obj_version_uuid_expr(t.c.account, t.c.container,
                                                    t.c.obj, before)
            where_clause = and_(t.c.uuid == c1,
                                t.c.cluster != except_cluster,
                                self._path_like_expr(t, path))
        else:
            t = self.nodes
            where_clause = self._path_like_expr(t, path)

        s = select([func.sum(t.c.size).label('size'),
                    func.max(t.c.mtime).label('mtime')],
                   where_clause)
        rp = self.conn.execute(s)
        r = rp.fetchone()
        rp.close()
        if not r:
            return None
        size = long(r.size - props.size)
        mtime = max(mtime, r.mtime)
        return (count, size, mtime)

    def version_create(self, account, container, obj, hash, size, type, source,
                       muser, uuid, checksum, mtime=None, cluster=0,
                       available=True, map_check_timestamp=None):
        """Create a new version from the given properties.
           Return the (uuid, mtime) of the new version.
        """

        mtime = mtime or time()
        s = self.versions.insert().values(
            account=account, container=container, obj=obj, hash=hash,
            size=size, type=type, source=source, mtime=mtime, muser=muser,
            uuid=uuid, checksum=checksum, cluster=cluster,
            available=available, map_check_timestamp=map_check_timestamp)
        r = self.conn.execute(s)
        r.close()
        if obj != '':
            self.statistics_update_ancestor(account, container, 1, size, mtime,
                                            cluster)
        return uuid, mtime

    def version_lookup(self, account, container='', obj='', before=inf,
                       cluster=0, all_props=True):
        """Lookup the current version of the given node.
           Return a list with its properties:
           (hash, size, type, source, mtime, muser, uuid, checksum, cluster,
            available, map_check_timestamp)
           or None if the current version is not found in the given cluster.
        """

        v = self.versions.alias('v')
        if not all_props:
            s = select([v.c.uuid])
        else:
            s = select([v.c.account, v.c.container, v.c.obj,
                        v.c.hash, v.c.size, v.c.type, v.c.source, v.c.mtime,
                        v.c.muser, v.c.uuid, v.c.checksum, v.c.cluster,
                        v.c.available, v.c.map_check_timestamp])

        c = select([self.versions.c.uuid],
                   and_(self.versions.c.account == account,
                        self.versions.c.container == container,
                        self.versions.c.obj == obj,
                        self.versions.c.mtime < before))
        c = c.order_by(desc(self.versions.c.mtime)).limit(1)
        s = s.where(and_(v.c.uuid == c,
                         v.c.cluster == cluster))
        r = self.conn.execute(s)
        props = r.fetchone()
        r.close()
        if props:
            return props
        return None

    def version_lookup_bulk(self, paths, before=inf, cluster=0,
                            all_props=True, order_by_path=False):
        """Lookup the current versions of the given nodes.
           Return a list with their properties:
           (account, container, obj, hash, size, type, source, mtime, muser,
            uuid, checksum, cluster, available, map_check_timestamp).
        """

        if not paths:
            return ()
        v = self.versions.alias('v')
        if not all_props:
            s = select([v.c.account, v.c.container, v.c.obj])
        else:
            s = select([v.c.account, v.c.container, v.c.obj,
                        v.c.hash, v.c.size, v.c.type, v.c.source, v.c.mtime,
                        v.c.muser, v.c.uuid, v.c.checksum, v.c.cluster,
                        v.c.available, v.c.map_check_timestamp])
        where_clause = or_(*[and_(self.versions.c.account == account,
                                  self.versions.c.container == container,
                                  self.versions.c.obj == obj,
                                  self.versions.c.mtime < before) for
                             account, container, obj in paths])
        c = self._latest_versions_uuid_expr(where_clause)
        s = s.where(and_(v.c.uuid.in_(c),
                         v.c.cluster == cluster))
        if order_by_path:
            s = s.order_by(v.c.account, v.c.container, v.c.obj)
        else:
            s = s.order_by(v.c.uuid)
        r = self.conn.execute(s)
        rproxy = r.fetchall()
        r.close()
        return (tuple(row.values()) for row in rproxy)

    def version_get_properties(self, uuid, keys=(), propnames=_propnames,
                               account='', container='', obj=''):
        """Return a sequence of values for the properties of
           the version specified by uuid and the keys, in the order given.
           If keys is empty, return all properties in the order
           (account, container, obj, hash, size, type, source, mtime, muser,
            uuid, checksum, cluster, available, map_check_timestamp).
        """

        v = self.versions.alias()
        s = select([v.c.account, v.c.container, v.c.obj, v.c.hash,
                    v.c.size, v.c.type, v.c.source,
                    v.c.mtime, v.c.muser, v.c.uuid,
                    v.c.checksum, v.c.cluster,
                    v.c.available, v.c.map_check_timestamp],
                   v.c.uuid == uuid)
        if account:
            s = s.where(and_(v.c.account == account,
                             v.c.container == container,
                             v.c.obj == obj))

        rp = self.conn.execute(s)
        r = rp.fetchone()
        rp.close()
        if r is None:
            return r

        if not keys:
            return r
        return [r[propnames[k]] for k in keys if k in propnames]

    def version_put_property(self, uuid, key, value):
        """Set value for the property of version specified by key."""

        if key not in _propnames:
            return
        s = self.versions.update()
        s = s.where(self.versions.c.uuid == uuid)
        s = s.values(**{key: value})
        self.conn.execute(s).close()

    def version_recluster(self, uuid, cluster):
        """Move the version into another cluster."""

        props = self.version_get_properties(uuid)
        if not props:
            return
        account = props[ACCOUNT]
        container = props[CONTAINER]
        obj = props[OBJ]
        size = props[SIZE]
        oldcluster = props[CLUSTER]
        if cluster == oldcluster:
            return

        mtime = time()
        if obj != '':
            self.statistics_update_ancestor(account, container,
                                            -1, -size, mtime, oldcluster)
            self.statistics_update_ancestor(account, container,
                                            1, size, mtime, cluster)

        s = self.versions.update()
        s = s.where(self.versions.c.uuid == uuid)
        s = s.values(cluster=cluster)
        self.conn.execute(s).close()

    def version_remove(self, uuid):

        props = self.version_get_properties(uuid)
        if not props:
            return
        account = props[ACCOUNT]
        container = props[CONTAINER]
        obj = props[OBJ]
        hash = props[HASH]
        size = props[SIZE]
        cluster = props[CLUSTER]

        mtime = time()
        if obj != '':
            self.statistics_update_ancestor(account, container, -1, -size,
                                            mtime, cluster)

        s = self.versions.delete().where(self.versions.c.uuid == uuid)
        self.conn.execute(s).close()

        return hash, size

    def attribute_get(self, uuid, domain, keys=(), before=inf):
        """Return a list of (key, value) pairs of the specific version.

        If keys is empty, return all attributes.
        Othwerise, return only those specified.
        """

        if before != inf:
            attrs = self.v_attributes
        else:
            attrs = self.attributes

        s = select([attrs.c.key, attrs.c.value])
        s = s.where(and_(attrs.c.uuid == uuid, attrs.c.domain == domain))
        if keys:
            s = s.where(attrs.c.key.in_(keys))

        if before != inf:
            s = s.where(attrs.c.mtime < before)

        r = self.conn.execute(s)
        l = r.fetchall()
        r.close()
        return l

    def attribute_set(self, uuid, domain, account, container, obj, items):
        """Set the attributes of the version specified by uuid.
           Receive attributes as an iterable of (key, value) pairs.
        """
        #TODO better upsert
        for k, v in items:
            s = self.attributes.update()
            s = s.where(and_(self.attributes.c.uuid == uuid,
                             self.attributes.c.domain == domain,
                             self.attributes.c.key == k))
            s = s.values(value=v)
            rp = self.conn.execute(s)
            rp.close()
            if rp.rowcount == 0:
                s = self.attributes.insert()
                s = s.values(uuid=uuid, domain=domain,
                             account=account, container=container, obj=obj,
                             key=k, value=v)
                self.conn.execute(s).close()

            s = self.v_attributes.insert()
            s = s.values(uuid=uuid, domain=domain,
                         account=account, container=container, obj=obj,
                         key=k, value=v, mtime=time())
            self.conn.execute(s).close()

    def attribute_del(self, uuid, domain, keys=()):
        """Delete attributes of the version specified by uuid.
           If keys is empty, delete all attributes.
           Otherwise delete those specified.
        """

        if keys:
            #TODO more efficient way to do this?
            for key in keys:
                s = self.attributes.delete()
                s = s.where(and_(self.attributes.c.uuid == uuid,
                                 self.attributes.c.domain == domain,
                                 self.attributes.c.key == key))
                self.conn.execute(s).close()
        else:
            s = self.attributes.delete()
            s = s.where(and_(self.attributes.c.uuid == uuid,
                             self.attributes.c.domain == domain))
            self.conn.execute(s).close()

    def attribute_copy(self, source, dest):
        s = select(
            [self.attributes.c.domain, self.attributes.c.key,
             self.attributes.c.value],
            self.attributes.c.uuid == source)
        rp = self.conn.execute(s)
        attributes = rp.fetchall()
        rp.close()

        props = self.version_get_properties(dest)
        dest_account = props[ACCOUNT]
        dest_container = props[CONTAINER]
        dest_obj = props[OBJ]
        for domain, k, v in attributes:
            # insert or replace
            s = self.attributes.update().where(and_(
                self.attributes.c.uuid == dest,
                self.attributes.c.domain == domain,
                self.attributes.c.key == k))
            s = s.where(self.versions.c.uuid == dest)
            s = s.values(account=self.versions.c.account,
                         container=self.versions.c.container,
                         obj=self.versions.c.obj,
                         value=v)
            rp = self.conn.execute(s)
            rp.close()
            if rp.rowcount == 0:
                s = self.attributes.insert()
                s = s.values(uuid=dest, domain=domain,
                             account=dest_account,
                             container=dest_container,
                             obj=dest_obj,
                             key=k, value=v)
            self.conn.execute(s).close()

            s = self.v_attributes.insert()
            s = s.values(uuid=dest, domain=domain,
                         account=dest_account,
                         container=dest_container,
                         obj=dest_obj,
                         key=k, value=v, mtime=time())
            self.conn.execute(s).close()

    def latest_attribute_keys(self, account, container, domain, before=inf,
                              except_cluster=0, pathq=None):
        """Return a list with all keys pairs defined
           for all latest versions under parent that
           do not belong to the cluster.
        """

        pathq = pathq or []

        if before != inf:
            t = self.v_attributes
        else:
            t = self.attributes

        s = select([t.c.key]).distinct()

        if before != inf:
            filtered = select([self.versions.c.uuid],
                              and_(self.versions.c.mtime < before,
                                   self.versions.c.uuid == t.c.uuid))
            filtered = filtered.order_by(desc(self.versions.c.mtime)).limit(1)
            s = s.where(t.c.uuid == filtered)
            s = s.where(select([self.versions.c.cluster],
                        self.versions.c.uuid == t.c.uuid) != except_cluster)
        s = s.where(t.c.account == account)
        s = s.where(t.c.container == container)
        s = s.where(t.c.obj != '')
        s = s.where(t.c.domain == domain)
        s = s.order_by(t.c.key)
        conja = []
        conjb = []
        for path, match in pathq:
            if match == MATCH_PREFIX:
                conja.append(self._path_like_expr(t, path))
            elif match == MATCH_EXACT:
                conjb.append(path)
        if conja:
            s = s.where(or_(*conja))
        if conjb:
            s = s.where(or_(*[and_(t.c.account == account,
                                   t.c.container == container,
                                   t.c.obj == obj) for (acc,
                                                        cont,
                                                        obj) in conjb]))
        rp = self.conn.execute(s)
        rows = rp.fetchall()
        rp.close()
        return [r[0] for r in rows]

    def latest_version_list(self, account, container, prefix='',
                            delimiter=None,
                            start='', limit=10000, before=inf,
                            except_cluster=0, pathq=[], domain=None,
                            filterq=[], sizeq=None, all_props=False):
        """Return a (list of (account, container, obj, uuid) tuples,
                     list of common prefixes)
           for the current versions of the paths with the given parent,
           matching the following criteria.

           The property tuple for a version is returned if all
           of these conditions are true:

                a. parent matches

                b. path > start

                c. path starts with prefix (and paths in pathq)

                d. version is the max up to before

                e. version is not in cluster

                f. the path does not have the delimiter occuring
                   after the prefix, or ends with the delimiter

                g. uuids the attribute filter query.

                   A filter query is a comma-separated list of
                   terms in one of these three forms:

                   key
                       an attribute with this key must exist

                   !key
                       an attribute with this key must not exist

                   key ?op value
                       the attribute with this key satisfies the value
                       where ?op is one of ==, != <=, >=, <, >.

                h. the size is in the range set by sizeq

           The list of common prefixes includes the prefixes
           matching up to the first delimiter after prefix,
           and are reported only once, as "virtual directories".
           The delimiter is included in the prefixes.

           If arguments are None, then the corresponding matching rule
           will always match.

           Limit applies to the first list of tuples returned.

           If all_props is True, return all properties after path.
        """

        if not start or start < prefix:
            start = strprevling(prefix)

        if before != inf:
            t = self.versions.alias('v')
        else:
            t = self.nodes

        columns = [t.c.account, t.c.container, t.c.obj]
        if all_props:
            columns += [t.c.hash, t.c.size, t.c.type, t.c.source, t.c.mtime,
                        t.c.muser, t.c.uuid, t.c.checksum]
            if before != inf:
                columns += [t.c.cluster]
            else:
                columns += [literal(0).label('cluster')]
            columns += [t.c.available, t.c.map_check_timestamp]

        s = select(columns).distinct()
        if before != inf:
            s = s.where(t.c.cluster != except_cluster)
            filtered = self._latest_obj_version_uuid_expr(t.c.account,
                                                          t.c.container,
                                                          t.c.obj, before)
            s = s.where(t.c.uuid.in_(filtered))

        s = s.where(t.c.account == account)
        if container != '':
            s = s.where(and_(t.c.container == container,
                             t.c.obj > bindparam('start')))
        else:
            s = s.where(and_(t.c.container > bindparam('start'),
                             t.c.obj == ''))

        if prefix:
            nextling = strnextling(prefix)
            s = s.where(t.c.obj < nextling)

        conj = []
        for path, match in pathq:
            if match == MATCH_PREFIX:
                conj.append(self._path_like_expr(t, path))
            elif match == MATCH_EXACT:
                conj.append(self._path_expr(t, path) == path)
        if conj:
            s = s.where(or_(*conj))

        if sizeq and len(sizeq) == 2:
            if sizeq[0]:
                s = s.where(t.c.size >= sizeq[0])
            if sizeq[1]:
                s = s.where(t.c.size < sizeq[1])

        if domain and filterq:
            included, excluded, opers = parse_filters(filterq)
            if included:
                subs = select([1])
                subs = subs.where(
                    self.attributes.c.uuid == t.c.uuid).correlate(t)
                subs = subs.where(self.attributes.c.domain == domain)
                subs = subs.where(or_(*[self.attributes.c.key.op('=')(x) for
                                  x in included]))
                s = s.where(exists(subs))
            if excluded:
                subs = select([1])
                subs = subs.where(
                    self.attributes.c.uuid == t.c.uuid).correlate(t)
                subs = subs.where(self.attributes.c.domain == domain)
                subs = subs.where(or_(*[self.attributes.c.key.op('=')(x) for
                                  x in excluded]))
                s = s.where(not_(exists(subs)))
            if opers:
                for k, o, val in opers:
                    subs = select([1])
                    subs = subs.where(
                        self.attributes.c.uuid == t.c.uuid).correlate(t)
                    subs = subs.where(self.attributes.c.domain == domain)
                    subs = subs.where(
                        and_(self.attributes.c.key.op('=')(k),
                             self.attributes.c.value.op(o)(val)))
                    s = s.where(exists(subs))

        s = s.order_by(t.c.account, t.c.container, t.c.obj)

        if not delimiter:
            s = s.limit(limit)
            rp = self.conn.execute(s, start=start)
            r = rp.fetchall()
            rp.close()
            return r, ()

        pfz = len(prefix)
        dz = len(delimiter)
        count = 0
        prefixes = []
        pappend = prefixes.append
        matches = []
        mappend = matches.append

        rp = self.conn.execute(s, start=start)
        while True:
            props = rp.fetchone()
            if props is None:
                break
            account, container, path = props[:OBJ + 1]
            idx = path.find(delimiter, pfz)

            if idx < 0:
                mappend(props)
                count += 1
                if count >= limit:
                    break
                continue

            if idx + dz == len(path):
                mappend(props)
                count += 1
                continue  # Get one more, in case there is a path.
            pf = path[:idx + dz]
            pappend((account, container, pf))
            if count >= limit:
                break

            rp = self.conn.execute(s, start=strnextling(pf))  # New start.
        rp.close()

        return matches, prefixes

    def latest_uuid(self, uuid, cluster):
        """Return the latest version of the given uuid and cluster.

        Return a (account, container, obj, uuid) tuple.
        If cluster is None, all clusters are considered.

        """

        if cluster == 0:
            t = self.nodes
        else:
            t = self.versions.alias('v')
            filtered = self._latest_obj_version_uuid_expr(t.c.account,
                                                          t.c.container,
                                                          t.c.obj, before=inf)

        s = select([t.c.account, t.c.container, t.c.obj, t.c.uuid])
        if cluster != 0:
            s = s.where(t.c.uuid == filtered)
            filtered = filtered.where(t.c.cluster == cluster)

        r = self.conn.execute(s)
        l = r.fetchone()
        r.close()
        return l

    def domain_object_list(self, domain, paths):
        """Return properties and attributes of the objects under the domain.

           (account, container, object, property list, attribute dictionary)
        """

        t = self.nodes
        a = self.attributes

        s = select([t.c.account, t.c.container, t.c.obj,
                    t.c.hash, t.c.size, t.c.type, t.c.source, t.c.mtime,
                    t.c.muser, t.c.uuid, t.c.checksum,
                    literal(0).label('cluster'),
                    t.c.available, t.c.map_check_timestamp,
                    a.c.key, a.c.value],
                   and_(t.c.uuid == a.c.uuid, a.c.domain == domain))
        if paths:
            s = s.where(or_(*[and_(t.c.account == account,
                                   t.c.container == container,
                                   t.c.obj == obj) for (account,
                                                        container,
                                                        obj) in paths]))

        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()

        key_item = itemgetter(slice(12))
        rows.sort(key=key_item)
        groups = itertools.groupby(rows, key_item)
        return [(k[0], k[1], k[2], k[3:], dict([i[12:] for i in data])) for
                (k, data) in groups]
