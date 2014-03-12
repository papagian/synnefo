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

from dbworker import DBWorker
from sqlalchemy import Table, Column, String, Integer, Boolean, MetaData
from sqlalchemy.sql import or_, and_, select
from sqlalchemy.sql.expression import true
from sqlalchemy.schema import Index
from sqlalchemy.exc import NoSuchTableError

from pithos.backends.random_word import get_random_word

from dbworker import ESCAPE_CHAR

import logging

logger = logging.getLogger(__name__)


def create_tables(engine):
    metadata = MetaData()
    columns = []
    columns.append(Column('public_id', Integer, primary_key=True))
    columns.append(Column('account', String(256), nullable=False))
    columns.append(Column('container', String(256), nullable=False))
    columns.append(Column('obj', String(1024), nullable=False))
    columns.append(Column('active', Boolean, nullable=False, default=True))
    columns.append(Column('url', String(2048), nullable=True))
    public = Table('public', metadata, *columns, mysql_engine='InnoDB',
                   sqlite_autoincrement=True)
    # place an index on path
    Index('idx_public_path_2',
          public.c.account, public.c.container, public.c.obj, unique=True)
    # place an index on url
    Index('idx_public_url', public.c.url, unique=True)
    metadata.create_all(engine)
    return metadata.sorted_tables


def get_path(account, container, obj):
    return '/'.join([account, container, obj])


class Public(DBWorker):
    """Paths can be marked as public."""

    def __init__(self, **params):
        DBWorker.__init__(self, **params)
        try:
            metadata = MetaData(self.engine)
            self.public = Table('public', metadata, autoload=True)
        except NoSuchTableError:
            tables = create_tables(self.engine)
            map(lambda t: self.__setattr__(t.name, t), tables)

    def get_unique_url(self, public_security, public_url_alphabet):
        l = public_security
        while 1:
            candidate = get_random_word(length=l, alphabet=public_url_alphabet)
            if self.public_path(candidate) is None:
                return candidate
            l += 1

    def public_set(self, account, container, obj, public_security,
                   public_url_alphabet):
        # TODO insert if not exists
        s = select([self.public.c.public_id])
        s = s.where(and_(self.public.c.account == account,
                         self.public.c.container == container,
                         self.public.c.obj == obj))
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()

        if not row:
            url = self.get_unique_url(
                public_security, public_url_alphabet
            )
            s = self.public.insert()
            s = s.values(account=account,
                         container=container,
                         obj=obj,
                         active=True, url=url)
            r = self.conn.execute(s)
            r.close()
            logger.info('Public url set for path: %s' % get_path(account,
                                                                 container,
                                                                 obj))

    def public_unset(self, account, container, obj):
        s = self.public.delete()
        s = s.where(and_(self.public.c.account == account,
                         self.public.c.container == container,
                         self.public.c.obj == obj))
        r = self.conn.execute(s)
        if r.rowcount != 0:
            logger.info('Public url unset for path: %s' % get_path(account,
                                                                   container,
                                                                   obj))
        r.close()

    def public_unset_bulk(self, t):
        if not t:
            return
        s = self.public.delete()
        s = s.where(or_(*[and_(self.public.c.account == account,
                               self.public.c.container == container,
                               self.public.c.obj == obj) for (account,
                                                              container,
                                                              obj) in t]))
        self.conn.execute(s).close()

    def public_get(self, account, container, obj):
        s = select([self.public.c.url])
        s = s.where(and_(self.public.c.account == account,
                         self.public.c.container == container,
                         self.public.c.obj == obj,
                         self.public.c.active == true()))
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        if row:
            return row[0]
        return None

    def public_list(self, account, container='', prefix=''):
        s = select([self.public.c.account,
                    self.public.c.container,
                    self.public.c.obj,
                    self.public.c.url])
        s = s.where(self.public.c.account == account)
        if container:
            s = s.where(self.public.c.container == container)
        if prefix:
            s = s.where(self.public.c.obj.like(self.escape_like(prefix) + '%',
                                               escape=ESCAPE_CHAR))
        s = s.where(self.public.c.active == true())
        r = self.conn.execute(s)
        rows = r.fetchall()
        r.close()
        return rows

    def public_path(self, public):
        s = select([self.public.c.account,
                    self.public.c.container,
                    self.public.c.obj])
        s = s.where(and_(self.public.c.url == public,
                         self.public.c.active == true()))
        r = self.conn.execute(s)
        row = r.fetchone()
        r.close()
        if row:
            return row
        return None
