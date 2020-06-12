from aioarangodb.tests import global_data
from aioarangodb.tests.helpers import generate_db_name
pytest_plugins = ['aioarangodb.tests.fixtures']


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
