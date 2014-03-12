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

from sqlalchemy.sql import select, literal, or_, and_
from sqlalchemy.sql.expression import join, union, null

from xfeatures import XFeatures
from groups import Groups
from public import Public
from node import Node
from collections import defaultdict

from dbworker import ESCAPE_CHAR


READ = 0
WRITE = 1


class Permissions(XFeatures, Groups, Public, Node):

    def __init__(self, **params):
        XFeatures.__init__(self, **params)
        Groups.__init__(self, **params)
        Public.__init__(self, **params)
        Node.__init__(self, **params)

    def access_grant(self, account, container, obj, access, members=()):
        """Grant members with access to path.
           Members can also be '*' (all),
           or some group specified as 'owner:group'."""

        self.feature_setmany(account, container, obj, access, members)

    def access_set(self, account, container, obj, permissions):
        """Set permissions for path. The permissions dict
           maps 'read', 'write' keys to member lists."""

        r = permissions.get('read', [])
        w = permissions.get('write', [])
        self.feature_clear(account, container, obj, READ)
        self.feature_clear(account, container, obj, WRITE)
        self.feature_setmany(account, container, obj, READ, r)
        self.feature_setmany(account, container, obj, WRITE, w)

    def access_get_for_bulk(self, perms):
        """Get permissions for path."""

        allowed = None
        d = defaultdict(list)
        for value, feature_id, key in perms:
            d[key].append(value)
        permissions = d
        if READ in permissions:
            allowed = 0
            permissions['read'] = permissions[READ]
            del(permissions[READ])
        if WRITE in permissions:
            allowed = 1
            permissions['write'] = permissions[WRITE]
            del(permissions[WRITE])
        return (permissions, allowed)

    def access_get(self, account, container, obj):
        """Get permissions for path."""

        permissions = self.feature_dict(account, container, obj)
        if READ in permissions:
            permissions['read'] = permissions[READ]
            del(permissions[READ])
        if WRITE in permissions:
            permissions['write'] = permissions[WRITE]
            del(permissions[WRITE])
        return permissions

    def access_members(self, account, container, obj):
        permissions = self.feature_dict(account, container, obj)
        members = set()
        members.update(permissions.get(READ, []))
        members.update(permissions.get(WRITE, []))
        for m in set(members):
            parts = m.split(':', 1)
            if len(parts) != 2:
                continue
            user, group = parts
            members.remove(m)
            members.update(self.group_members(user, group))
        return list(members)

    def access_clear(self, account, container, obj):
        """Revoke access to path (both permissions and public)."""

        self.xfeature_destroy(account, container, obj)
        self.public_unset(account, container, obj)

    def access_clear_bulk(self, t):
        """Revoke access to path (both permissions and public)."""

        self.xfeature_destroy_bulk(t)
        self.public_unset_bulk(t)

    def access_check(self, account, container, obj, access, member):
        """Return true if the member has this access to the path."""

        members = self.feature_get(account, container, obj, access)
        if member in members or '*' in members:
            return True
        for owner, group in self.group_parents(member):
            if owner + ':' + group in members:
                return True
        return False

    def access_check_bulk(self, paths, member):
        s = select([self.xfeatures.c.account,
                    self.xfeatures.c.container,
                    self.xfeatures.c.obj,
                    self.xfeatures.c.value,
                    self.xfeatures.c.feature_id,
                    self.xfeatures.c.key],
                   or_(*[and_(self.xfeatures.c.account == account,
                              self.xfeatures.c.container == container,
                              self.xfeatures.c.obj == obj) for (
                       account, container, obj) in paths]))
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        if rows:
            access_check_paths = {}
            for account, container, obj, value, feature_id, key in rows:
                path = (account, container, obj)
                try:
                    access_check_paths[path].append((value, feature_id, key))
                except KeyError:
                    access_check_paths[path] = [(value, feature_id, key)]
            access_check_paths['group_parents'] = self.group_parents(member)
            return access_check_paths
        return None

    def access_inherit(self, account, container='', obj=''):
        """Return the paths influencing the access for path."""

        # Only keep path components.
        container_path = '/'.join([account, container])
        idx = len(container_path) + 1
        path = '/'.join([account, container, obj])
        parts = path.rstrip('/').split('/')
        valid = set()
        for i in range(1, len(parts)):
            subp = '/'.join(parts[:i + 1])
            valid.add(subp[idx:])
            if subp != path:
                valid.add(subp[idx:] + '/')
        valid = self.xfeature_get_bulk(account, container, valid)
        return [(x.account, x.container, x.obj) for x in valid]

    def access_inherit_bulk(self, account, container, paths):
        """Return the paths influencing the access for specific paths."""

        # Only keep path components.
        container_path = '/'.join([account, container])
        idx = len(container_path) + 1
        valid = set()
        for p in paths:
            path = '/'.join([container_path, p])
            parts = path.rstrip('/').split('/')
            for i in range(1, len(parts)):
                subp = '/'.join(parts[:i + 1])
                valid.add(subp[idx:])
                if subp != path:
                    valid.add(subp[idx:] + '/')
        valid = self.xfeature_get_bulk(account, container, valid)
        return [(x.account, x.container, x.obj) for x in valid]

    def access_list_paths(self, member, account='', container='',
                          prefix='', include_owned=False,
                          include_containers=True):
        """Return the list of paths granted to member.

        Keyword arguments:
        prefix -- return only objects starting with prefix (default None)
        include_owned -- return also paths owned by member (default False)
        include_containers -- return also container paths owned by member
                              (default True)

        """

        selectable = (self.groups.c.owner + ':' + self.groups.c.name)
        member_groups = select([selectable.label('value')],
                               self.groups.c.member == member)

        members = select([literal(member).label('value')])
        any = select([literal('*').label('value')])

        u = union(member_groups, members, any).alias()
        inner_join = join(self.xfeatures, u,
                          self.xfeatures.c.value == u.c.value)
        s = select([self.xfeatures.c.account,
                    self.xfeatures.c.container,
                    self.xfeatures.c.obj], from_obj=[inner_join]).distinct()
        if account:
            if container and prefix:
                like = lambda p: and_(
                    self.xfeatures.c.account == p[0],
                    self.xfeatures.c.container == p[1],
                    self.xfeatures.c.obj.like(self.escape_like(p[2]) + '%',
                                              escape=ESCAPE_CHAR))
            elif container:
                like = lambda p: and_(
                    self.xfeatures.c.account == p[0],
                    self.xfeatures.c.container == p[1],
                    self.xfeatures.c.obj.like('%', escape=ESCAPE_CHAR))
            else:
                like = lambda p: and_(
                    self.xfeatures.c.account == p[0],
                    self.xfeatures.c.container.like('%', escape=ESCAPE_CHAR),
                    self.xfeatures.c.obj.like('%', escape=ESCAPE_CHAR))
            t = (account, container, prefix)
            s = s.where(or_(*map(like, self.access_inherit(*t) or (t,))))
        r = self.conn.execute(s)
        l = r.fetchall()
        r.close()

        if include_owned:
            s = select([self.nodes.c.account,
                        self.nodes.c.container,
                        self.nodes.c.obj])
            condition = and_(self.nodes.c.account == member,
                             self.nodes.c.container != null(),
                             self.nodes.c.obj != null())
            if include_containers:
                condition = or_(condition,
                                and_(self.nodes.c.account == member,
                                     self.nodes.c.container != null(),
                                     self.nodes.c.obj == null()))
            s = s.where(condition)
            r = self.conn.execute(s)
            l += [row for row in r.fetchall() if row not in l]
            r.close()
        return l

    def access_list_shared(self, account, container='', prefix=''):
        """Return the list of shared paths."""

        s = select([self.xfeatures.c.account,
                    self.xfeatures.c.container,
                    self.xfeatures.c.obj]).distinct()
        if account and container and prefix:
            like = lambda p: and_(
                self.xfeatures.c.account == p[0],
                self.xfeatures.c.container == p[1],
                self.xfeatures.c.obj.like(self.escape_like(p[2]) + '%',
                                          escape=ESCAPE_CHAR))
        elif container:
            like = lambda p: and_(
                self.xfeatures.c.account == p[0],
                self.xfeatures.c.container == p[1],
                self.xfeatures.c.obj.like('%', escape=ESCAPE_CHAR))
        else:
            like = lambda p: and_(
                self.xfeatures.c.account == p[0],
                self.xfeatures.c.container.like('%', escape=ESCAPE_CHAR),
                self.xfeatures.c.obj.like('%', escape=ESCAPE_CHAR))
        t = (account, container, prefix)
        s = s.where(or_(*map(like, self.access_inherit(*t) or (t,))))
        s = s.order_by(self.xfeatures.c.account.asc(),
                       self.xfeatures.c.container.asc(),
                       self.xfeatures.c.obj.asc())
        r = self.conn.execute(s)
        l = [row[:] for row in r.fetchall()]
        r.close()
        return l
