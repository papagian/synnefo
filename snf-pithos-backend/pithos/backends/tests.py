# Copyright 2014 GRNET S.A. All rights reserved.
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

from mock import MagicMock, call
from functools import partial

from pithos.backends.random_word import get_random_word
from pithos.backends.util import connect_backend

import random
import unittest


serial = 0

get_random_data = lambda length: get_random_word(length)[:length]
get_random_name = partial(get_random_word, length=8)


class CommonTestQuotaMixin(object):
    """Challenge quota accounting.

    Each test case records the expected quota commission calls resulting from
    the execution of the respective backend methods.

    Finally, the tearDown() method asserts that these calls have been actually
    made.
    """
    expected_issue_commission_calls = []

    def tearDown(self):
        self.mocked_astakos.issue_commissions.mock_calls == \
            self.expected_issue_commission_calls
        super(CommonTestQuotaMixin, self).tearDown()

    def upload_object(self, user, account, container, obj, data=None,
                      length=None, type_='application/octet-stream',
                      permissions=None):
        if data is None:
            if length is None:
                length = length or random.randint(1, self.block_size)
            data = get_random_data(length)
        assert len(data) == length
        hashmap = [self.b.put_block(data)]
        self.b.update_object_hashmap(user, account, container, obj,
                                     length, type_, hashmap, checksum='',
                                     domain='pithos',
                                     permissions=permissions)
        if length != 0:
            self.expected_issue_commission_calls += [call.issue_one_commission(
                holder=account,
                source='system',
                provisions={'pithos.diskspace': len(data)},
                name='/'.join([account, container, obj]))]
        return data

    def create_folder(self, user, account, container, folder,
                      permissions=None):
        return self.upload_object(user, account, container, folder,
                                  data='', length=0,
                                  type_='application/directory',
                                  permissions=permissions)

    def put_container(self, user, account, container):
        self.b.put_container(user, account, container)

    def test_upload_object(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        self.upload_object(account, account, container, obj)

    def test_copy_object(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        data = self.upload_object(account, account, container, obj)

        other_obj = get_random_name()
        self.b.copy_object(account, account, container, obj,
                           account, container, other_obj,
                           'application/octet-stream',
                           domain='pithos')
        self.expected_issue_commission_calls += [call.issue_one_commission(
            holder=account,
            source='system',
            provisions={'pithos.diskspace': len(data)},
            name='/'.join([account, container, other_obj]))]

    def test_copy_object_to_other_container(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        data = self.upload_object(account, account, container, obj)

        other_container = get_random_name()
        self.put_container(account, account, other_container)
        other_obj = get_random_name()
        self.b.copy_object(account, account, container, obj,
                           account, other_container, other_obj,
                           'application/octet-stream',
                           domain='pithos')
        self.expected_issue_commission_calls += [
            call.issue_one_commission(holder=account,
                                      source='system',
                                      provisions={'pithos.diskspace':
                                                  len(data)},
                                      name='/'.join([account,
                                                     container,
                                                     other_obj]))]

    def test_copy_object_to_other_account(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)

        other_account = get_random_name()
        self.put_container(other_account, other_account, container)

        data = self.upload_object(account, account, container, obj,
                                  permissions={'read': [other_account]})

        self.b.copy_object(other_account,
                           account, container, obj,
                           other_account, container, obj,
                           'application/octet-stream',
                           domain='pithos')

        self.expected_issue_commission_calls = [call.issue_one_commission(
            holder=account,
            source='system',
            provisions={'pithos.diskspace': -len(data)},
            name='/'.join([account, container, obj]))]

    def test_copy_to_the_same_path(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        self.upload_object(account, account, container, obj)

        self.b.copy_object(account, account, container, obj,
                           account, container, obj,
                           'application/octet-stream',
                           domain='pithos')

    def test_copy_dir(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder)

        obj1 = '/'.join([folder, get_random_name()])
        data1 = self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        data2 = self.upload_object(account, account, container, obj2)

        other_folder = get_random_name()
        self.b.copy_object(account, account, container, folder,
                           account, container, other_folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

        self.expected_issue_commission_calls += [call.issue_one_commissions(
            holder=account,
            source='system',
            provisions={'pithos.diskspace': data1 + data2},
            name='/'.join([account, container, other_folder]))]

    def test_copy_dir_to_other_container(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder)

        container2 = get_random_name()
        self.put_container(account, account, container2)

        obj1 = '/'.join([folder, get_random_name()])
        data1 = self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        data2 = self.upload_object(account, account, container, obj2)

        self.b.copy_object(account, account, container, folder,
                           account, container2, folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

        self.expected_issue_commission_calls += [call.issue_one_commissions(
            holder=account,
            source='system',
            provisions={'pithos.diskspace': data1 + data2},
            name='/'.join([account, container2, folder]))]

    def test_copy_dir_to_other_account(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        other_account = get_random_name()
        self.put_container(other_account, other_account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder,
                           permissions={'read': [other_account]})

        obj1 = '/'.join([folder, get_random_name()])
        data1 = self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        data2 = self.upload_object(account, account, container, obj2)

        self.b.copy_object(other_account, account, container, folder,
                           other_account, container, folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

        self.expected_issue_commission_calls += [call.issue_one_commissions(
            holder=other_account,
            source='system',
            provisions={'pithos.diskspace': data1 + data2},
            name='/'.join([other_account, container, folder]))]

    def test_move_obj(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        self.upload_object(account, account, container, obj)

        other_obj = get_random_name()
        self.b.move_object(account, account, container, obj,
                           account, container, other_obj,
                           'application/octet-stream',
                           domain='pithos')

    def test_move_obj_to_other_container(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        self.upload_object(account, account, container, obj)

        other_container = get_random_name()
        self.put_container(account, account, other_container)
        other_obj = get_random_name()
        self.b.copy_object(account, account, container, obj,
                           account, other_container, other_obj,
                           'application/octet-stream',
                           domain='pithos')

    def test_move_object_to_other_account(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)

        other_account = get_random_name()
        self.put_container(other_account, other_account, container)

        folder = 'shared'
        self.create_folder(other_account, other_account, container, folder,
                           permissions={'write': [account]})

        data = self.upload_object(account, account, container, obj)

        other_obj = '/'.join([folder, obj])
        self.b.move_object(account,
                           account, container, obj,
                           other_account, container, other_obj,
                           'application/octet-stream',
                           domain='pithos')

        self.expected_issue_commission_calls = [
            call.issue_one_commission(
                holder=account,
                source='system',
                provisions={'pithos.diskspace': -len(data)},
                name='/'.join([account, container, obj])),
            call.issue_one_commission(
                holder=other_account,
                source='system',
                provisions={'pithos.diskspace': len(data)},
                name='/'.join([other_account, container, other_obj]))]

    def test_move_to_the_same_path(self):
        account = self.account
        container = get_random_name()
        obj = get_random_name()
        self.put_container(account, account, container)
        self.upload_object(account, account, container, obj)

        self.b.move_object(account, account, container, obj,
                           account, container, obj,
                           'application/octet-stream',
                           domain='pithos')

    def test_move_dir(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder)

        obj1 = '/'.join([folder, get_random_name()])
        self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        self.upload_object(account, account, container, obj2)

        other_folder = get_random_name()
        self.b.move_object(account, account, container, folder,
                           account, container, other_folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

    def test_move_dir_to_other_container(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder)

        container2 = get_random_name()
        self.put_container(account, account, container2)

        obj1 = '/'.join([folder, get_random_name()])
        self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        self.upload_object(account, account, container, obj2)

        self.b.copy_object(account, account, container, folder,
                           account, container2, folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

    def test_move_dir_to_other_account(self):
        account = self.account
        container = get_random_name()
        self.put_container(account, account, container)

        other_account = get_random_name()
        self.put_container(other_account, other_account, container)

        folder = get_random_name()
        self.create_folder(account, account, container, folder)
        self.create_folder(other_account, other_account, container, folder,
                           permissions={'write': [account]})

        obj1 = '/'.join([folder, get_random_name()])
        data1 = self.upload_object(account, account, container, obj1)

        obj2 = '/'.join([folder, get_random_name()])
        data2 = self.upload_object(account, account, container, obj2)

        self.b.move_object(account, account, container, folder,
                           other_account, container, folder,
                           'application/directory',
                           domain='pithos',
                           delimiter='/')

        released_space = len(data1) + len(data2)
        self.expected_issue_commission_calls += [
            call.issue_one_commissions(
                holder=account,
                source='system',
                provisions={'pithos.diskspace': -released_space},
                name='/'.join([account, container, folder])),
            call.issue_one_commissions(
                holder=other_account,
                source='system',
                provisions={'pithos.diskspace': released_space},
                name='/'.join([other_account, container, folder]))]


class TestBackend(unittest.TestCase):
    block_size = 1024
    hash_algorithm = 'sha256'
    block_path = '/tmp/data'
    account = 'user'
    free_versioning = True

    def setUp(self):
        self.b = connect_backend(db_connection=self.db_connection,
                                 db_module=self.db_module,
                                 block_path=self.block_path,
                                 block_size=self.block_size,
                                 hash_algorithm=self.hash_algorithm,
                                 free_versioning=self.free_versioning)
        self.mocked_astakos = MagicMock()
        self.b.commission_serials = MagicMock()


class TestSQLAlchemyBackend(TestBackend, CommonTestQuotaMixin):
    db_module = 'pithos.backends.lib.sqlalchemy'
    # db_connection = 'sqlite:///:memory:'
    db_connection = 'sqlite:////tmp/test_pithos_backend.db'

    def tearDown(self):
        account = self.account
        for c in self.b.list_containers(account, account):
            self.b.delete_container(account, account, c, delimiter='/')
            self.b.delete_container(account, account, c)


class TestSQLiteBackend(TestBackend, CommonTestQuotaMixin):
    db_module = 'pithos.backends.lib.sqlite'
    db_connection = ':memory:'


class TestSQLAlchemyChargeVersioningBackend(TestSQLiteBackend):
    free_versioning = False


class TestSQLiteChargeVersiongBackend(TestSQLiteBackend):
    free_versioning = False
