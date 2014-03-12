"""Change db schema

Revision ID: 16f9e048ef49
Revises: 4451e165da19
Create Date: 2014-02-17 12:38:20.318455

"""

# revision identifiers, used by Alembic.
revision = '16f9e048ef49'
down_revision = 'e6edec1b499'

from alembic import op
import sqlalchemy as sa

import time


def _split_path_column(t):
    account_path = sa.func.split_part(t.c.path, '/', 1)
    container_path = sa.func.split_part(t.c.path, '/', 2)
    object_path = sa.func.substr(t.c.path,
                                 sa.func.length(sa.func.concat(account_path,
                                                               '/',
                                                               container_path,
                                                               '/')) + 1)
    return (account_path, container_path, object_path)


def _reconstruct_path_expr(t):
    is_container = sa.and_(t.c.container != '', t.c.obj == '')
    is_obj = sa.and_(t.c.container != '', t.c.obj != '')
    container_path = sa.sql.func.concat(t.c.account, '/', t.c.container)
    object_path = sa.sql.func.concat(container_path, '/', t.c.obj)
    return sa.case([(is_obj, object_path), (is_container, container_path)],
                   else_=t.c.account)


def upgrade_permissions(connection):
    op.add_column('xfeatures', sa.Column('account', sa.String(256),
                                         default=''))
    op.add_column('xfeatures', sa.Column('container', sa.String(256),
                                         default=''))
    op.add_column('xfeatures', sa.Column('obj', sa.String(1024),
                                         default=''))
    op.add_column('xfeatures', sa.Column('key', sa.Integer,
                                         autoincrement=False))
    op.add_column('xfeatures', sa.Column('value', sa.String(256)))
    op.create_index('idx_xfeatures_path2', 'xfeatures',
                    ['account', 'container', 'obj', 'key', 'value'])

    xvals = sa.sql.table(
        'xfeaturevals',
        sa.sql.column('feature_id', sa.Integer),
        sa.sql.column('key', sa.String),
        sa.sql.column('value', sa.String))
    x = sa.sql.table(
        'xfeatures',
        sa.sql.column('feature_id', sa.Integer),
        sa.sql.column('path', sa.String),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String),
        sa.sql.column('key', sa.Integer),
        sa.sql.column('value', sa.String))

    # copy xfeaturevals data into xfeatures table
    s = sa.select([x.c.path, xvals.c.key, xvals.c.value],
                  x.c.feature_id == xvals.c.feature_id)
    rows = connection.execute(s).fetchall()
    values = [dict(account=r.path.split('/', 2)[0],
                   container=r.path.split('/', 2)[1],
                   obj=r.path.split('/', 2)[2],
                   key=r.key,
                   value=r.value) for r in rows]
    if values:
        ins = x.insert().values(account=sa.sql.bindparam('account'),
                                container=sa.sql.bindparam('container'),
                                obj=sa.sql.bindparam('obj'),
                                key=sa.sql.bindparam('key'),
                                value=sa.sql.bindparam('value'))
        connection.execute(ins, values)

    # drop xfeatures obsolete rows
    op.execute(x.delete().where(sa.sql.and_(x.c.account ==
                                            sa.sql.expression.null(),
                                            x.c.container ==
                                            sa.sql.expression.null(),
                                            x.c.obj ==
                                            sa.sql.expression.null())))

    op.alter_column('xfeatures', 'account', nullable=False)
    op.alter_column('xfeatures', 'container', nullable=False)
    op.alter_column('xfeatures', 'obj', nullable=False)

    op.drop_column('xfeatures', 'path')

    op.drop_table('xfeaturevals')


def upgrade_public(connection):
    op.add_column('public', sa.Column('account', sa.String(256), default=''))
    op.add_column('public', sa.Column('container', sa.String(256), default=''))
    op.add_column('public', sa.Column('obj', sa.String(1024), default=''))
    op.create_index('idx_public_path2', 'public',
                    ['account', 'container', 'obj'], unique=True)

    public = sa.sql.table(
        'public',
        sa.sql.column('public_id', sa.Integer),
        sa.sql.column('path', sa.String),
        sa.sql.column('active', sa.Boolean),
        sa.sql.column('url', sa.String),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String))

    # update public rows
    acc_expr, cont_expr, obj_expr = _split_path_column(public)
    u = public.update()
    u = u.values(account=acc_expr, container=cont_expr, obj=obj_expr)
    connection.execute(u)

    op.alter_column('public', 'account', nullable=False)
    op.alter_column('public', 'container', nullable=False)
    op.alter_column('public', 'obj', nullable=False)

    op.drop_column('public', 'path')


def upgrade_policy(connection):
    op.add_column('policy', sa.Column('account', sa.String(256), default=''))
    op.add_column('policy', sa.Column('container', sa.String(256), default=''))
    op.create_index('idx_policy_path2', 'policy', ['account', 'container'])

    policy = sa.sql.table(
        'policy',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('key', sa.String),
        sa.sql.column('value', sa.String),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String))

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String))

    # update new columns
    acc_expr, cont_expr, _ = _split_path_column(nodes)
    u = policy.update().where(sa.and_(policy.c.node == nodes.c.node))
    u = u.values(account=acc_expr, container=cont_expr)
    connection.execute(u)

    op.alter_column('policy', 'account', nullable=False)
    op.alter_column('policy', 'container', nullable=False)

    op.drop_column('policy', 'node')


def upgrade_attributes(connection):
    # rename attributes table to v_attributes
    op.rename_table('attributes', 'v_attributes')
    op.drop_index('idx_attributes_domain')
    op.drop_index('idx_attributes_serial_node')

    op.add_column('v_attributes', sa.Column('account', sa.String(256)))
    op.add_column('v_attributes', sa.Column('container', sa.String(256)))
    op.add_column('v_attributes', sa.Column('obj', sa.String(1024)))
    op.add_column('v_attributes', sa.Column('uuid', sa.String(64)))
    op.add_column('v_attributes', sa.Column('mtime',
                                            sa.DECIMAL(precision=16, scale=6)))
    op.create_index('idx_v_attributes_path2', 'v_attributes',
                    ['account', 'container', 'obj'])
    op.create_index('idx_v_attributes_domain', 'v_attributes', ['domain'])
    op.create_index('idx_v_attributes_uuid', 'v_attributes', ['uuid'])

    # populate new columns
    v_attrs = sa.sql.table('v_attributes',
                           sa.sql.column('serial', sa.Integer),
                           sa.sql.column('account', sa.String),
                           sa.sql.column('container', sa.String),
                           sa.sql.column('domain', sa.String),
                           sa.sql.column('obj', sa.String),
                           sa.sql.column('uuid', sa.String),
                           sa.sql.column('key', sa.String),
                           sa.sql.column('value', sa.String),
                           sa.sql.column('mtime', sa.DECIMAL))

    versions = sa.sql.table('versions',
                            sa.sql.column('node', sa.Integer),
                            sa.sql.column('serial', sa.Integer),
                            sa.sql.column('uuid', sa.String),
                            sa.sql.column('cluster', sa.Integer))

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String),
        sa.sql.column('latest_version', sa.Integer))

    acc_expr, cont_expr, obj_expr = _split_path_column(nodes)
    u = v_attrs.update().where(sa.and_(v_attrs.c.serial == versions.c.serial,
                                       versions.c.node == nodes.c.node))
    u = u.values(account=acc_expr,
                 container=cont_expr,
                 obj=obj_expr,
                 uuid=versions.c.uuid,
                 mtime=time.time())
    connection.execute(u)

    # create & populate attributes table
    op.create_table('attributes',
                    sa.Column('domain', sa.String(256), primary_key=True),
                    sa.Column('key', sa.String(128), primary_key=True),
                    sa.Column('value', sa.String(256)),
                    sa.Column('account', sa.String(256), default=''),
                    sa.Column('container', sa.String(256), default=''),
                    sa.Column('obj', sa.String(1026), default=''),
                    sa.Column('uuid', sa.String(64), primary_key=True))
    op.create_index('idx_attributes_path2', 'attributes',
                    ['account', 'container', 'obj'])
    op.create_index('idx_attributes_domain', 'attributes', ['domain'])

    attrs = sa.sql.table('attributes',
                         sa.sql.column('account', sa.String),
                         sa.sql.column('container', sa.String),
                         sa.sql.column('obj', sa.String),
                         sa.sql.column('domain', sa.String),
                         sa.sql.column('uuid', sa.String),
                         sa.sql.column('key', sa.String),
                         sa.sql.column('value', sa.String))
    s = sa.select([v_attrs], sa.and_(v_attrs.c.serial == versions.c.serial,
                                     versions.c.cluster == 0))
    rows = connection.execute(s).fetchall()
    values = [dict(account=r.account,
                   container=r.container,
                   obj=r.obj,
                   uuid=r.uuid,
                   domain=r.domain,
                   key=r.key,
                   value=r.value) for r in rows]
    if values:
        ins = attrs.insert().values(account=sa.sql.bindparam('account'),
                                    container=sa.sql.bindparam('container'),
                                    obj=sa.sql.bindparam('obj'),
                                    uuid=sa.sql.bindparam('uuid'),
                                    domain=sa.sql.bindparam('domain'),
                                    key=sa.sql.bindparam('key'),
                                    value=sa.sql.bindparam('value'))
        connection.execute(ins, values)

    op.alter_column('attributes', 'account', nullable=False)

    op.alter_column('v_attributes', 'account', nullable=False)
    op.alter_column('v_attributes', 'uuid', nullable=False)

    op.drop_column('v_attributes', 'serial')
    op.drop_column('v_attributes', 'node')
    op.drop_column('v_attributes', 'is_latest')


def upgrade_nodes(connection):
    # add columns
    op.add_column('nodes', sa.Column('account', sa.String(256), default=''))
    op.add_column('nodes', sa.Column('container', sa.String(256), default=''))
    op.add_column('nodes', sa.Column('obj', sa.String(1024), default=''))
    op.add_column('nodes', sa.Column('hash', sa.String(256)))
    op.add_column('nodes', sa.Column('size', sa.BigInteger, nullable=False,
                                     default=0, server_default='0'))
    op.add_column('nodes', sa.Column('type', sa.String(256), nullable=False,
                                     default='', server_default=''))
    op.add_column('nodes', sa.Column('source', sa.String(64)))
    op.add_column('nodes', sa.Column('mtime',
                                     sa.DECIMAL(precision=16, scale=6)))
    op.add_column('nodes', sa.Column('muser', sa.String(256), nullable=False,
                                     default='', server_default=''))
    op.add_column('nodes', sa.Column('uuid', sa.String(64), nullable=False,
                                     default='', server_default='',
                                     primary_key=True))
    op.add_column('nodes', sa.Column('checksum', sa.String(256),
                                     nullable=False, default='',
                                     server_default=''))
    op.add_column('nodes', sa.Column('available', sa.Boolean, nullable=False,
                                     default=True, server_default='True'))
    op.add_column('nodes', sa.Column('map_check_timestamp',
                                     sa.DECIMAL(precision=16, scale=6)))

    op.create_index('idx_nodes_path2', 'nodes',
                    ['account', 'container', 'obj'], unique=True)
    op.create_index('idx_nodes_uuid', 'nodes', ['uuid'])

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String),
        sa.sql.column('latest_version', sa.Integer),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String),
        sa.sql.column('hash', sa.String),
        sa.sql.column('size', sa.Integer),
        sa.sql.column('type', sa.String),
        sa.sql.column('source', sa.Integer),
        sa.sql.column('mtime', sa.DECIMAL),
        sa.sql.column('muser', sa.String),
        sa.sql.column('uuid', sa.String),
        sa.sql.column('checksum', sa.String),
        sa.sql.column('available', sa.Boolean),
        sa.sql.column('map_check_timestamp', sa.DECIMAL))

    versions = sa.sql.table('versions',
                            sa.sql.column('serial', sa.Integer),
                            sa.sql.column('hash', sa.String),
                            sa.sql.column('size', sa.BigInteger),
                            sa.sql.column('type', sa.String),
                            sa.sql.column('source', sa.Integer),
                            sa.sql.column('mtime', sa.DECIMAL),
                            sa.sql.column('muser', sa.String),
                            sa.sql.column('uuid', sa.String),
                            sa.sql.column('checksum', sa.String),
                            sa.sql.column('cluster', sa.Integer),
                            sa.sql.column('available', sa.Boolean),
                            sa.sql.column('map_check_timestamp', sa.DECIMAL))

    # delete obsolete columns (referred to historical or deleted objects)
    d = nodes.delete().where(nodes.c.latest_version.in_(
        sa.select([versions.c.serial], versions.c.cluster != 0)))
    connection.execute(d)

    # update remained rows
    acc_expr, cont_expr, obj_expr = _split_path_column(nodes)
    u = nodes.update().where(
        sa.and_(nodes.c.latest_version == versions.c.serial))
    u = u.values(account=acc_expr,
                 container=cont_expr,
                 obj=obj_expr,
                 hash=versions.c.hash,
                 size=versions.c.size,
                 type=versions.c.type,
                 source=versions.c.source,
                 mtime=versions.c.mtime,
                 muser=versions.c.muser,
                 uuid=versions.c.uuid,
                 checksum=versions.c.checksum,
                 available=versions.c.available,
                 map_check_timestamp=versions.c.map_check_timestamp)
    connection.execute(u)

    u = nodes.update().where(sa.and_(
        nodes.c.account == sa.sql.expression.null(),
        nodes.c.container == sa.sql.expression.null(),
        nodes.c.obj == sa.sql.expression.null()))
    u = u.values(account=acc_expr, container=cont_expr, obj=obj_expr)
    connection.execute(u)

    op.alter_column('nodes', 'account', nullable=False)
    op.alter_column('nodes', 'container', nullable=False)
    op.alter_column('nodes', 'obj', nullable=False)

    op.alter_column('nodes', 'size', server_default=None)
    op.alter_column('nodes', 'type', server_default=None)
    op.alter_column('nodes', 'muser', server_default=None)
    op.alter_column('nodes', 'uuid', server_default=None)
    op.alter_column('nodes', 'checksum', server_default=None)

    op.drop_column('nodes', 'parent')
    op.drop_column('nodes', 'path')
    op.drop_column('nodes', 'latest_version')


def upgrade_versions(connection):
    op.add_column('versions', sa.Column('account', sa.String(256), default=''))
    op.add_column('versions', sa.Column('container', sa.String(256),
                  default=''))
    op.add_column('versions', sa.Column('obj', sa.String(1024), default=''))

    op.alter_column('versions', 'source', type_=sa.String(64))
    op.create_index('idx_versions_path_mtime', 'versions',
                    ['account', 'container', 'obj', 'mtime'])

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('latest_version', sa.Integer),
        sa.sql.column('path', sa.String))

    versions = sa.sql.table('versions',
                            sa.sql.column('serial', sa.Integer),
                            sa.sql.column('cluster', sa.Integer),
                            sa.sql.column('uuid', sa.String),
                            sa.sql.column('node', sa.Integer),
                            sa.sql.column('account', sa.String),
                            sa.sql.column('container', sa.String),
                            sa.sql.column('obj', sa.String),
                            sa.sql.column('source', sa.String))

    acc_expr, cont_expr, obj_expr = _split_path_column(nodes)
    u = versions.update().where(sa.and_(versions.c.node == nodes.c.node))
    u = u.values(
        uuid=sa.case([(versions.c.cluster != 0,
                      sa.sql.expression.cast(
                          sa.text('uuid_in(md5(now()::text)::cstring)'),
                          sa.String))],
                     else_=versions.c.uuid),
        account=acc_expr,
        container=cont_expr,
        obj=obj_expr)
    connection.execute(u)

    v = versions.alias('v')
    u = versions.update().where(versions.c.source != sa.sql.expression.null())
    u = u.values(source=sa.select(
        [v.c.uuid],
        v.c.serial == sa.sql.expression.cast(versions.c.source, sa.Integer)))
    connection.execute(u)

    op.alter_column('versions', 'account', nullable=False)

    op.drop_constraint('versions_node_fkey', 'versions')


def upgrade_statistics(connection):
    # rename attributes table to v_attributes
    op.rename_table('statistics', 'v_statistics')

    op.add_column('v_statistics', sa.Column('account', sa.String(256)))
    op.add_column('v_statistics', sa.Column('container', sa.String(256)))

    v_statistics = sa.sql.table(
        'v_statistics',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('size', sa.BigInteger),
        sa.sql.column('population', sa.Integer),
        sa.sql.column('mtime', sa.DECIMAL),
        sa.sql.column('cluster', sa.Integer))

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String))

    acc_expr, cont_expr, _ = _split_path_column(nodes)
    u = v_statistics.update().where(v_statistics.c.node == nodes.c.node)
    u = u.values(account=acc_expr, container=cont_expr)
    connection.execute(u)

    # create & populate attributes table
    op.create_table('statistics',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('account', sa.String(256), primary_key=True),
                    sa.Column('container', sa.String(256), primary_key=True,
                              default=''),
                    sa.Column('population', sa.Integer, nullable=False,
                              default=0),
                    sa.Column('size', sa.BigInteger, nullable=False,
                              default=0),
                    sa.Column('mtime', sa.DECIMAL(precision=16, scale=6)))
    op.create_index('idx_statistics_path2', 'statistics',
                    ['account', 'container'], unique=True)

    statistics = sa.sql.table('statistics',
                              sa.sql.column('account', sa.String),
                              sa.sql.column('container', sa.String),
                              sa.sql.column('size', sa.BigInteger),
                              sa.sql.column('population', sa.Integer),
                              sa.sql.column('mtime', sa.DECIMAL))

    s = sa.select([v_statistics], v_statistics.c.cluster == 0)
    rows = connection.execute(s).fetchall()
    values = [dict(account=r.account,
                   container=r.container,
                   size=r.size,
                   population=r.population,
                   mtime=r.mtime) for r in rows]
    if values:
        ins = statistics.insert().values(
                account=sa.sql.bindparam('account'),
                container=sa.sql.bindparam('container'),
                size=sa.sql.bindparam('size'),
                population=sa.sql.bindparam('population'),
                mtime=sa.sql.bindparam('mtime'))
        connection.execute(ins, values)

    op.drop_column('statistics', 'id')

    op.alter_column('v_statistics', 'account', nullable=False)
    op.alter_column('v_statistics', 'container', nullable=False)

    op.drop_column('v_statistics', 'node')

    connection.execute(sa.text('ALTER TABLE v_statistics '
                               'ADD CONSTRAINT v_statistics_pkey1 '
                               'PRIMARY KEY (account, container, cluster)'))


def finalize(connection):
    op.drop_column('versions', 'node')
    op.drop_column('versions', 'serial')

    op.drop_column('nodes', 'node')

    connection.execute(sa.text('ALTER TABLE v_attributes '
                               'ADD CONSTRAINT v_attributes_pkey1 '
                               'PRIMARY KEY (domain, key, uuid, mtime)'))


def upgrade():
    op.drop_table('config')

    connection = op.get_bind()
    upgrade_permissions(connection)
    upgrade_public(connection)
    upgrade_policy(connection)
    upgrade_statistics(connection)
    upgrade_versions(connection)
    upgrade_attributes(connection)
    upgrade_nodes(connection)

    finalize(connection)


def downgrade_public(connection):
    # add public path column
    op.drop_index('idx_public_path2')
    op.add_column('public', sa.Column('path', sa.String(2048)))
    op.create_index('idx_public_path', 'public', ['path'])

    public = sa.sql.table(
        'public',
        sa.sql.column('public_id', sa.Integer),
        sa.sql.column('active', sa.Boolean),
        sa.sql.column('path', sa.String),
        sa.sql.column('url', sa.String),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String))

    # populate public path column
    p = public.alias('p')
    s = sa.select([sa.func.concat(p.c.account, '/', p.c.container, '/',
                                  p.c.obj)],
                  p.c.public_id == public.c.public_id)
    u = public.update().values({'path': s})
    connection.execute(u)

    # drop obsolete public columns
    op.drop_column('public', 'account')
    op.drop_column('public', 'container')
    op.drop_column('public', 'obj')


def downgrade_policy(connection):
    # create column
    op.drop_index('idx_policy_path2')
    op.add_column('policy', sa.Column('node', sa.Integer,
                                      sa.ForeignKey('nodes.node',
                                                    ondelete='CASCADE',
                                                    onupdate='CASCADE'),
                                      primary_key=True,
                                      nullable=True))

    policy = sa.sql.table(
        'policy',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('key', sa.String),
        sa.sql.column('path', sa.String),
        sa.sql.column('value', sa.String),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String))

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String))

    s = sa.select([nodes.c.node],
                  nodes.c.path == sa.func.concat(policy.c.account,
                                                 '/',
                                                 policy.c.container))
    u = policy.update().values({'node': s})
    connection.execute(u)

    op.drop_column('policy', 'account')
    op.drop_column('policy', 'container')


def downgrade_permissions(connection):
    # create xfeaturevals
    op.create_table('xfeaturevals',
                    sa.Column('feature_id', sa.Integer,
                              sa.ForeignKey('xfeatures.feature_id',
                                            ondelete='CASCADE'),
                              primary_key=True),
                    sa.Column('key', sa.Integer, primary_key=True,
                              autoincrement=False),
                    sa.Column('value', sa.String(256), primary_key=True))

    # create xfeatures path column
    op.drop_index('idx_xfeatures_path2')
    op.add_column('xfeatures', sa.Column('path', sa.String(2048)))
    op.create_index('idx_xfeatures_path', 'xfeatures', ['path'])

    # populate xfeaturevals
    xvals = sa.sql.table(
        'xfeaturevals',
        sa.sql.column('feature_id', sa.Integer),
        sa.sql.column('key', sa.String),
        sa.sql.column('value', sa.String))
    x = sa.sql.table(
        'xfeatures',
        sa.sql.column('feature_id', sa.Integer),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String),
        sa.sql.column('path', sa.String),
        sa.sql.column('key', sa.Integer),
        sa.sql.column('value', sa.String))
    x2 = x.alias('x2')
    s = sa.select([sa.sql.func.concat(x2.c.account,
                                      '/',
                                      x2.c.container,
                                      '/',
                                      x2.c.obj).label('path')]).distinct()
    paths = connection.execute(s).fetchall()
    if paths:
        connection.execute(x.insert(), paths)

    # populate xfeaturevals
    x2 = x.alias('x2')
    s = sa.select([x.c.feature_id, x2.c.key, x2.c.value],
                  x.c.path == sa.func.concat(x2.c.account,
                                             '/',
                                             x2.c.container,
                                             '/',
                                             x2.c.obj))
    rows = connection.execute(s).fetchall()
    values = [dict(feature_id=r.feature_id, key=r.key, value=r.value) for r in
              rows]
    if values:
        ins = xvals.insert().values(feature_id=sa.sql.bindparam('feature_id'),
                                    key=sa.sql.bindparam('key'),
                                    value=sa.sql.bindparam('value'))
        connection.execute(ins, values)

    # delete xfeatures obsolete rows
    op.execute(x.delete().where(x.c.path == sa.sql.expression.null()))

    # delete xfeatures obsolete columns
    op.drop_column('xfeatures', 'account')
    op.drop_column('xfeatures', 'container')
    op.drop_column('xfeatures', 'obj')
    op.drop_column('xfeatures', 'key')
    op.drop_column('xfeatures', 'value')


def downgrade_attributes(connection):
    op.drop_table('attributes')

    op.rename_table('v_attributes', 'attributes')

    op.add_column('attributes',
                  sa.Column('node', sa.Integer, nullable=False, default=0,
                            server_default='0'))
    op.add_column('attributes',
                  sa.Column('is_latest', sa.Boolean, nullable=False,
                            default=True, server_default='True'))

    a = sa.sql.table('attributes',
                     sa.sql.column('serial', sa.Integer),
                     sa.sql.column('node', sa.Integer),
                     sa.sql.column('is_latest', sa.Integer),
                     sa.sql.column('uuid', sa.Integer))

    v = sa.sql.table('versions',
                     sa.sql.column('serial', sa.Integer),
                     sa.sql.column('node', sa.Integer),
                     sa.sql.column('uuid', sa.String))

    n = sa.sql.table('nodes',
                     sa.sql.column('node', sa.Integer),
                     sa.sql.column('is_latest', sa.Boolean))

    u = a.update().where(sa.and_(a.c.serial == v.c.serial,
                                 v.c.node == n.c.node))
    u = u.values({'serial': v.c.serial,
                  'node': v.c.node,
                  'is_latest': n.c.is_latest})

    op.alter_column('attributes', 'node', server_default=None)
    op.alter_column('attributes', 'is_latest', server_default=None)

    op.drop_column('attributes', 'account')
    op.drop_column('attributes', 'container')
    op.drop_column('attributes', 'obj')
    op.drop_column('attributes', 'uuid')


def downgrade_nodes(connection):
    op.drop_column('nodes', 'hash')
    op.drop_column('nodes', 'size')
    op.drop_column('nodes', 'type')
    op.drop_column('nodes', 'source')
    op.drop_column('nodes', 'mtime')
    op.drop_column('nodes', 'muser')
    op.drop_column('nodes', 'uuid')
    op.drop_column('nodes', 'checksum')

    op.add_column('nodes', sa.Column('node', sa.Integer, primary_key=True))
    op.add_column('nodes', sa.Column('parent', sa.Integer,
                                     autoincrement=False))
    op.add_column('nodes', sa.Column('path', sa.String(2048), default='',
                                     server_default='', nullable=False))
    op.add_column('nodes', sa.Column('latest_version', sa.Integer))
    op.create_index('idx_nodes_parent', 'nodes', ['parent'])
    op.create_index('idx_latest_version', 'nodes', ['latest_version'])

    nodes = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('parent', sa.Integer),
        sa.sql.column('path', sa.String),
        sa.sql.column('latest_version', sa.Integer),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String),
        sa.sql.column('obj', sa.String))

    versions = sa.sql.table('versions',
                            sa.sql.column('serial', sa.Integer),
                            sa.sql.column('hash', sa.String),
                            sa.sql.column('size', sa.BigInteger),
                            sa.sql.column('type', sa.String),
                            sa.sql.column('source', sa.Integer),
                            sa.sql.column('mtime', sa.DECIMAL),
                            sa.sql.column('muser', sa.String),
                            sa.sql.column('uuid', sa.String),
                            sa.sql.column('checksum', sa.String),
                            sa.sql.column('cluster', sa.Integer),
                            sa.sql.column('account', sa.String),
                            sa.sql.column('container', sa.String),
                            sa.sql.column('obj', sa.Integer))

    # add missing (non active paths)
    v = versions.alias('v')
    s = sa.select([v.c.account, v.c.container, v.c.obj],
                  sa.select([sa.func.count(nodes.c.node)],
                            sa.and_(nodes.c.account == v.c.account,
                                    nodes.c.container == v.c.container,
                                    nodes.c.obj == v.c.obj)).as_scalar() == 0)
    rows = connection.execute(s).fetchall()
    values = [dict(account=r.account,
                   container=r.container,
                   obj=r.obj) for r in rows]
    if values:
        ins = nodes.insert().values(account=sa.sql.bindparam('account'),
                                    container=sa.sql.bindparam('container'),
                                    obj=sa.sql.bindparam('obj'))
        connection.execute(ins, values)

    # update existing
    n = nodes.alias('n')
    u = nodes.update()
    u = u.values(path=_reconstruct_path_expr(nodes),
                 parent=sa.select([n.c.node],
                                  sa.and_(nodes.c.account == n.c.account,
                                          nodes.c.container == n.c.container,
                                          nodes.c.obj == '')))
    connection.execute(u)

    op.alter_column('nodes', 'parent', sa.ForeignKey('nodes.node',
                                                     ondelete='CASCADE',
                                                     onupdate='CASCADE'))

    op.alter_column('nodes', 'path', server_default=None)

    op.create_index('idx_nodes_path', 'nodes', ['path'], unique=True)

    op.drop_column('nodes', 'account')
    op.drop_column('nodes', 'container')
    op.drop_column('nodes', 'obj')


def downgrade_versions(connection):
    op.add_column('versions', sa.Column('node', sa.Integer))
    op.add_column('versions', sa.Column('serial', sa.Integer,
                                        primary_key=True))
    n = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String))

    v = sa.sql.table('versions',
                     sa.sql.column('node', sa.Integer),
                     sa.sql.column('account', sa.String),
                     sa.sql.column('container', sa.String),
                     sa.sql.column('obj', sa.Integer))

    acc_expr, cont_expr, obj_expr = _split_path_column(n)
    u = v.update()
    u = u.values({'node': sa.select(
        [n.c.node],
        sa.and_(v.c.account == acc_expr,
                v.c.container == cont_expr,
                v.c.obj == obj_expr))})
    connection.execute(u)

    op.alter_column('versions', 'node', sa.ForeignKey('nodes.node',
                                                      ondelete='CASCADE',

                                                      onupdate='CASCADE'))

    op.drop_column('versions', 'account')
    op.drop_column('versions', 'container')
    op.drop_column('versions', 'obj')


def downgrade_statistics(connection):
    op.add_column('statistics', sa.Column('node', sa.Integer,
                                          primary_key=True,
                                          nullable=True))

    st = sa.sql.table(
        'statistics',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('account', sa.String),
        sa.sql.column('container', sa.String))

    n = sa.sql.table(
        'nodes',
        sa.sql.column('node', sa.Integer),
        sa.sql.column('path', sa.String))

    acc_expr, cont_expr, obj_expr = _split_path_column(n)
    u = st.update()
    u = u.values({'node': sa.select(
        [n.c.node],
        sa.and_(acc_expr == st.c.account,
                cont_expr == st.c.container,
                obj_expr == ''))})
    connection.execute(u)

    op.alter_column('statistics', 'node', sa.ForeignKey('nodes.node',
                                                        ondelete='CASCADE',
                                                        onupdate='CASCADE'),
                    nullble=False)

    op.drop_column('statistics', 'account')
    op.drop_column('statistics', 'container')


def downgrade():
    raise NotImplementedError('There is no way back!')
