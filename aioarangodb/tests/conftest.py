from __future__ import absolute_import, unicode_literals, division

import pytest
import asyncio
import os

from aioarangodb import ArangoClient
from aioarangodb.database import StandardDatabase
from aioarangodb import formatter

from aioarangodb.arangodocker import arango_image
from aioarangodb.helpers import (
    generate_db_name,
    generate_col_name,
    generate_string,
    generate_username,
    generate_graph_name,
    empty_collection,
    generate_jwt
)
from aioarangodb.executors import (
    TestAsyncExecutor,
    TestBatchExecutor,
    TestTransactionExecutor
)

global_data = dict()


def pytest_addoption(parser):
    parser.addoption('--passwd', action='store', default='secret')
    parser.addoption('--complete', action='store_true')
    parser.addoption('--cluster', action='store_true')
    parser.addoption('--replication', action='store_true')
    parser.addoption('--secret', action='store', default='secret')
    parser.addoption('--tst_db_name', action='store', default=generate_db_name())

def pytest_configure(config):
    global_data['secret'] = config.getoption('secret')
    global_data['tst_db_name'] = config.getoption('tst_db_name')
    global_data['replication'] = config.getoption('replication')
    global_data['cluster'] = config.getoption('cluster')
    global_data['complete'] = config.getoption('complete')
    global_data['passwd'] = config.getoption('passwd')
    global_data['username'] = 'root'

@pytest.fixture(scope='session')
def arango():
    if os.environ.get('TRAVIS', 'false') == 'true':
        host, port = arango_image.run()
    yield
    if os.environ.get('TRAVIS', 'false') == 'true':
        arango_image.stop()

@pytest.fixture(scope='function')
async def client(arango):
    url = 'http://{}:{}'.format(
        'localhost',
        arango_image.get_port()
    )
    client = ArangoClient(hosts=[url, url, url])
    global_data['url'] = url
    global_data['client'] = client
    yield client
    await client.close()

@pytest.fixture(scope='function')
async def sys_db(client):
    sys_db = await client.db(
        name='_system',
        username='root',
        password=global_data.get('passwd'),
        superuser_token=generate_jwt(global_data.get('secret'))
    )
    username = generate_username()
    password = generate_string()

    global_data['username'] = username
    global_data['password'] = password
    await sys_db.create_database(
        name=global_data.get('tst_db_name'),
        users=[{
            'active': True,
            'username': username,
            'password': password,
        }]
    )
    yield sys_db
    await sys_db.clear_async_jobs()
    # Remove all test tasks.
    for task in await sys_db.tasks():
        task_name = task['name']
        if task_name.startswith('test_task'):
            await sys_db.delete_task(task_name, ignore_missing=True)

    # Remove all test users.
    for user in await sys_db.users():
        username = user['username']
        if username.startswith('test_user'):
            await sys_db.delete_user(username, ignore_missing=True)

    # Remove all test databases.
    for db_name in await sys_db.databases():
        if db_name.startswith('test_database'):
            await sys_db.delete_database(db_name, ignore_missing=True)

    # Remove all test collections.
    for collection in await sys_db.collections():
        col_name = collection['name']
        if col_name.startswith('test_collection'):
            await sys_db.delete_collection(col_name, ignore_missing=True)

@pytest.fixture(scope='function')
async def db(sys_db, client):
    tst_db = await client.db(global_data.get('tst_db_name'), global_data.get('username'), global_data.get('password'))

    # Create a standard collection for testing.
    col_name = generate_col_name()
    tst_col = await tst_db.create_collection(col_name, edge=False)
    await tst_col.add_skiplist_index(['val'])
    await tst_col.add_fulltext_index(['text'])
    geo_index = await tst_col.add_geo_index(['loc'])

    # Create a legacy edge collection for testing.
    icol_name = generate_col_name()
    await tst_db.create_collection(icol_name, edge=True)

    # Create test vertex & edge collections and graph.
    graph_name = generate_graph_name()
    ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    tst_graph = await tst_db.create_graph(graph_name)
    await tst_graph.create_vertex_collection(fvcol_name)
    await tst_graph.create_vertex_collection(tvcol_name)
    await tst_graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name]
    )

    global_data.update({
        'geo_index': geo_index,
        'col_name': col_name,
        'icol_name': icol_name,
        'graph_name': graph_name,
        'ecol_name': ecol_name,
        'fvcol_name': fvcol_name,
        'tvcol_name': tvcol_name
    })

    return tst_db

@pytest.fixture(scope='function')
async def bad_db(sys_db, client):
    bad_db_name = generate_db_name()
    # Create a user and non-system database for testing.
    bad_db = await client.db(bad_db_name, global_data['username'], global_data['password'])
    return bad_db



# # noinspection PyProtectedMember
# def pytest_generate_tests(metafunc):
#     tst_db = global_data['tst_db']
#     bad_db = global_data['bad_db']

#     tst_dbs = [tst_db]
#     bad_dbs = [bad_db]

#     if global_data['complete']:
#         test = metafunc.module.__name__.split('.test_', 1)[-1]
#         tst_conn = tst_db._conn
#         bad_conn = bad_db._conn

#         if test in {'aql', 'collection', 'document', 'index'}:
#             # Add test transaction databases
#             tst_txn_db = StandardDatabase(tst_conn)
#             tst_txn_db._executor = TestTransactionExecutor(tst_conn)
#             tst_dbs.append(tst_txn_db)
#             bad_txn_db = StandardDatabase(bad_conn)
#             bad_txn_db._executor = TestTransactionExecutor(bad_conn)
#             bad_dbs.append(bad_txn_db)

#             # Add test async databases
#             tst_async_db = StandardDatabase(tst_conn)
#             tst_async_db._executor = TestAsyncExecutor(tst_conn)
#             tst_dbs.append(tst_async_db)
#             bad_async_db = StandardDatabase(bad_conn)
#             bad_async_db._executor = TestAsyncExecutor(bad_conn)
#             bad_dbs.append(bad_async_db)

#             # Add test batch databases
#             tst_batch_db = StandardDatabase(tst_conn)
#             tst_batch_db._executor = TestBatchExecutor(tst_conn)
#             tst_dbs.append(tst_batch_db)
#             bad_batch_bdb = StandardDatabase(bad_conn)
#             bad_batch_bdb._executor = TestBatchExecutor(bad_conn)
#             bad_dbs.append(bad_batch_bdb)

#     if 'db' in metafunc.fixturenames and 'bad_db' in metafunc.fixturenames:
#         metafunc.parametrize('db,bad_db', zip(tst_dbs, bad_dbs))

#     elif 'db' in metafunc.fixturenames:
#         metafunc.parametrize('db', tst_dbs)

#     elif 'bad_db' in metafunc.fixturenames:
#         metafunc.parametrize('bad_db', bad_dbs)


@pytest.fixture(autouse=True)
def mock_formatters(monkeypatch):

    def mock_verify_format(body, result):
        body.pop('error', None)
        body.pop('code', None)
        result.pop('edge', None)
        if len(body) != len(result):
            raise ValueError(
                '\nIN: {}\nOUT: {}'.format(sorted(body), sorted(result))
            )
        return result

    monkeypatch.setattr(formatter, 'verify_format', mock_verify_format)

@pytest.fixture(autouse=False)
def url():
    return global_data['url']


@pytest.fixture(autouse=False)
def db_name():
    return global_data['tst_db_name']


@pytest.fixture(autouse=False)
def username():
    return global_data['username']


@pytest.fixture(autouse=False)
def password():
    return global_data['password']


@pytest.fixture(autouse=False)
def root_password():
    return global_data['passwd']


@pytest.fixture(autouse=False)
def conn(db):
    return getattr(db, '_conn')


@pytest.fixture(autouse=False)
async def col(db):
    collection = db.collection(global_data['col_name'])
    await empty_collection(collection)
    return collection


@pytest.fixture(autouse=False)
async def bad_col(bad_db):
    return bad_db.collection(global_data['col_name'])


@pytest.fixture(autouse=False)
def geo():
    return global_data['geo_index']


@pytest.fixture(autouse=False)
async def icol(db):
    collection = db.collection(global_data['icol_name'])
    await empty_collection(collection)
    return collection


@pytest.fixture(autouse=False)
async def graph(db):
    return db.graph(global_data['graph_name'])


@pytest.fixture(autouse=False)
async def bad_graph(bad_db):
    return bad_db.graph(global_data['graph_name'])


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
async def fvcol(graph):
    collection = graph.vertex_collection(global_data['fvcol_name'])
    await empty_collection(collection)
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
async def tvcol(graph):
    collection = graph.vertex_collection(global_data['tvcol_name'])
    await empty_collection(collection)
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
async def bad_fvcol(bad_graph):
    return bad_graph.vertex_collection(global_data['fvcol_name'])


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
async def ecol(graph):
    collection = graph.edge_collection(global_data['ecol_name'])
    await empty_collection(collection)
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
async def bad_ecol(bad_graph):
    return bad_graph.edge_collection(global_data['ecol_name'])


@pytest.fixture(autouse=False)
def docs():
    return [
        {'_key': '1', 'val': 1, 'text': 'foo', 'loc': [1, 1]},
        {'_key': '2', 'val': 2, 'text': 'foo', 'loc': [2, 2]},
        {'_key': '3', 'val': 3, 'text': 'foo', 'loc': [3, 3]},
        {'_key': '4', 'val': 4, 'text': 'bar', 'loc': [4, 4]},
        {'_key': '5', 'val': 5, 'text': 'bar', 'loc': [5, 5]},
        {'_key': '6', 'val': 6, 'text': 'bar', 'loc': [5, 5]},
    ]


@pytest.fixture(autouse=False)
def fvdocs():
    return [
        {'_key': '1', 'val': 1},
        {'_key': '2', 'val': 2},
        {'_key': '3', 'val': 3},
    ]


@pytest.fixture(autouse=False)
def tvdocs():
    return [
        {'_key': '4', 'val': 4},
        {'_key': '5', 'val': 5},
        {'_key': '6', 'val': 6},
    ]


@pytest.fixture(autouse=False)
def edocs():
    fv = global_data['fvcol_name']
    tv = global_data['tvcol_name']
    return [
        {'_key': '1', '_from': '{}/1'.format(fv), '_to': '{}/4'.format(tv)},
        {'_key': '2', '_from': '{}/1'.format(fv), '_to': '{}/5'.format(tv)},
        {'_key': '3', '_from': '{}/6'.format(fv), '_to': '{}/2'.format(tv)},
        {'_key': '4', '_from': '{}/8'.format(fv), '_to': '{}/7'.format(tv)},
    ]


@pytest.fixture(autouse=False)
def cluster():
    return global_data['cluster']


@pytest.fixture(autouse=False)
def replication():
    return global_data['replication']


@pytest.fixture(autouse=False)
def secret():
    return global_data['secret']
