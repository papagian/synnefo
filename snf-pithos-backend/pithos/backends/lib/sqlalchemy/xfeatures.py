# Copyright 2011-2014 GRNET S.A. All rights reserved.
#
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
#
#   1. Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer.
#
#   2. Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY GRNET S.A. ``AS IS'' AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GRNET S.A OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
# USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of GRNET S.A.

from collections import defaultdict
from sqlalchemy import Table, Column, String, Integer, MetaData
from sqlalchemy.sql import select, and_, or_
from sqlalchemy.schema import Index
from sqlalchemy.exc import NoSuchTableError

from dbworker import DBWorker


def create_tables(engine):
    metadata = MetaData()
    columns = []
    columns.append(Column('feature_id', Integer, primary_key=True))
    columns.append(Column('account', String(256), nullable=False, default=''))
    columns.append(Column('container', String(256), nullable=False,
                          default=''))
    columns.append(Column('obj', String(1024), nullable=False, default=''))
    columns.append(Column('key', Integer, primary_key=True,
                          autoincrement=False))
    columns.append(Column('value', String(256), primary_key=True))
    xfeatures = Table('xfeatures', metadata, *columns, mysql_engine='InnoDB')
    # place an index on path
    Index('idx_xfeatures_path_2',
          xfeatures.c.account, xfeatures.c.container, xfeatures.c.obj,
          xfeatures.c.key, xfeatures.c.value, unique=True)

    metadata.create_all(engine)
    return metadata.sorted_tables


class XFeatures(DBWorker):
    """XFeatures are path properties that allow non-nested
       inheritance patterns. Currently used for storing permissions.
    """

    def __init__(self, **params):
        DBWorker.__init__(self, **params)
        try:
            metadata = MetaData(self.engine)
            self.xfeatures = Table('xfeatures', metadata, autoload=True)
            self.xfeaturevals = Table('xfeaturevals', metadata, autoload=True)
        except NoSuchTableError:
            tables = create_tables(self.engine)
            map(lambda t: self.__setattr__(t.name, t), tables)

    def xfeature_get(self, account, container, obj):
        """Return feature for path."""

        s = select([self.xfeatures.c.feature_id])
        s = s.where(self.xfeatures.c.account == account)
        s = s.where(self.xfeatures.c.container == container)
        s = s.where(self.xfeatures.c.obj == obj)
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        if row:
            return row[0]
        return None

    def xfeature_get_bulk(self, account, container, paths):
        """Return features for paths."""

        if not paths:
            return ()

        s = select([self.xfeatures.c.feature_id,
                    self.xfeatures.c.account,
                    self.xfeatures.c.container,
                    self.xfeatures.c.obj]).distinct()
        s = s.where(and_(self.xfeatures.c.account == account,
                         self.xfeatures.c.container == container,
                         self.xfeatures.c.obj.in_(paths)))
        s = s.order_by(self.xfeatures.c.account,
                       self.xfeatures.c.container,
                       self.xfeatures.c.obj)
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        return rows

    def xfeature_create(self, account, container, obj):
        """Create and return a feature for path.
           If the path has a feature, return it.
        """

        feature = self.xfeature_get(account, container, obj)
        if feature is not None:
            return feature
        s = self.xfeatures.insert()
        r = self.conn.execute(s, account=account, container=container, obj=obj)
        inserted_primary_key = r.inserted_primary_key[0]
        r.close()
        return inserted_primary_key

    def xfeature_destroy(self, account, container, obj):
        """Destroy a feature and all its key, value pairs."""

        s = self.xfeatures.delete().where(and_(
            self.xfeatures.c.account == account,
            self.xfeatures.c.container == container,
            self.xfeatures.c.obj == obj))
        r = self.conn.execute(s)
        r.close()

    def xfeature_destroy_bulk(self, t):
        """Destroy features and all their key, value pairs."""

        if not t:
            return
        s = self.xfeatures.delete().where(
            or_(*[and_(self.xfeatures.c.account == account,
                       self.xfeatures.c.container == container,
                       self.xfeatures.c.obj == obj) for (account,
                                                         container,
                                                         obj) in t]))
        r = self.conn.execute(s)
        r.close()

    def feature_dict(self, account, container, obj):
        """Return a dict mapping keys to list of values for feature."""

        s = select([self.xfeatures.c.key, self.xfeatures.c.value])
        s = s.where(and_(self.xfeatures.c.account == account,
                         self.xfeatures.c.container == container,
                         self.xfeatures.c.obj == obj))
        r = self.conn.execute(s)
        d = defaultdict(list)
        for key, value in r.fetchall():
            d[key].append(value)
        r.close()
        return d

    def feature_set(self, account, container, obj, key, value):
        """Associate a key, value pair with a feature."""

        # TODO: insert if not exists
        s = self.xfeatures.select()
        s = s.where(self.xfeatures.c.account == account)
        s = s.where(self.xfeatures.c.container == container)
        s = s.where(self.xfeatures.c.obj == obj)
        s = s.where(self.xfeatures.c.key == key)
        s = s.where(self.xfeatures.c.value == value)
        r = self.conn.execute(s)
        xfeatures = r.fetchall()
        r.close()
        if len(xfeatures) == 0:
            s = self.xfeatures.insert()
            r = self.conn.execute(s, account=account, container=container,
                                  obj=obj, key=key, value=value)
            r.close()

    def feature_setmany(self, account, container, obj, key, values):
        """Associate the given key, and values with a feature."""

        if not values:
            return
        ins = self.xfeatures.insert()
        self.conn.execute(ins, [{'account': account,
                                 'container':  container,
                                 'obj': obj,
                                 'key': key,
                                 'value': v} for v in values])

    def feature_unset(self, account, container, obj, key, value):
        """Disassociate a key, value pair from a feature."""

        s = self.xfeatures.delete()
        s = s.where(and_(self.xfeatures.c.account == account,
                         self.xfeatures.c.container == container,
                         self.xfeatures.c.obj == obj,
                         self.xfeatures.c.key == key,
                         self.xfeatures.c.value == value))
        r = self.conn.execute(s)
        r.close()

    def feature_unsetmany(self, account, container, obj, key, values):
        """Disassociate the key for the values given, from a feature."""

        conditional = or_(*[and_(self.xfeatures.c.account == account,
                                 self.xfeatures.c.container == container,
                                 self.xfeatures.c.obj == obj,
                                 self.xfeatures.c.key == key,
                                 self.xfeatures.c.value == v) for v in values])
        s = self.xfeatures.delete().where(conditional)
        r = self.conn.execute(s)
        r.close()

    def feature_get(self, account, container, obj, key):
        """Return the list of values for a key of a feature."""

        s = select([self.xfeatures.c.value])
        s = s.where(and_(self.xfeatures.c.account == account,
                         self.xfeatures.c.container == container,
                         self.xfeatures.c.obj == obj,
                         self.xfeatures.c.key == key))
        r = self.conn.execute(s)
        l = [row[0] for row in r.fetchall()]
        r.close()
        return l

    def feature_clear(self, account, container, obj, key):
        """Delete all key, value pairs for a key of a feature."""

        s = self.xfeatures.delete()
        s = s.where(and_(self.xfeatures.c.account == account,
                         self.xfeatures.c.container == container,
                         self.xfeatures.c.obj == obj,
                         self.xfeatures.c.key == key))
        r = self.conn.execute(s)
        r.close()
