from __future__ import absolute_import, unicode_literals

import json

import pytest

from aioarangodb.client import ArangoClient
from aioarangodb.database import StandardDatabase
from aioarangodb.exceptions import ServerConnectionError
from aioarangodb.http import DefaultHTTPClient
from aioarangodb.resolver import (
    SingleHostResolver,
    RandomHostResolver,
    RoundRobinHostResolver
)
from aioarangodb.version import __version__
from aioarangodb.tests.arangodocker import arango_image
from aioarangodb.tests.helpers import (
    generate_db_name,
    generate_username,
    generate_string
)
pytestmark = pytest.mark.asyncio


async def test_client_attributes(client):
    http_client = DefaultHTTPClient()

    client = ArangoClient(
        hosts=f'http://127.0.0.1:{arango_image.get_port()}',
        http_client=http_client
    )
    assert client.version == __version__
    assert client.hosts == [f'http://127.0.0.1:{arango_image.get_port()}']

    assert repr(client) == f'<ArangoClient http://127.0.0.1:{arango_image.get_port()}>'
    assert isinstance(client._host_resolver, SingleHostResolver)

    client_repr = f'<ArangoClient http://127.0.0.1:{arango_image.get_port()},http://localhost:{arango_image.get_port()}>'
    client_hosts = [f'http://127.0.0.1:{arango_image.get_port()}', f'http://localhost:{arango_image.get_port()}']

    await client.close()
    client = ArangoClient(
        hosts=f'http://127.0.0.1:{arango_image.get_port()},http://localhost'
              f':{arango_image.get_port()}',
        http_client=http_client,
        serializer=json.dumps,
        deserializer=json.loads,
    )
    assert client.version == __version__
    assert client.hosts == client_hosts
    assert repr(client) == client_repr
    assert isinstance(client._host_resolver, RoundRobinHostResolver)

    await client.close()
    client = ArangoClient(
        hosts=client_hosts,
        host_resolver='random',
        http_client=http_client,
        serializer=json.dumps,
        deserializer=json.loads,
    )
    assert client.version == __version__
    assert client.hosts == client_hosts
    assert repr(client) == client_repr
    assert isinstance(client._host_resolver, RandomHostResolver)
    await client.close()


async def test_client_good_connection(db, username, password):
    client = ArangoClient(hosts=f'http://127.0.0.1:{arango_image.get_port()}')

    # Test connection with verify flag on and off
    for verify in (True, False):
        db = await client.db(db.name, username, password, verify=verify)
        assert isinstance(db, StandardDatabase)
        assert db.name == db.name
        assert db.username == username
        assert db.context == 'default'
    await client.close()



async def test_client_bad_connection(db, username, password, cluster):
    client = ArangoClient(hosts=f'http://127.0.0.1:{arango_image.get_port()}')

    bad_db_name = generate_db_name()
    bad_username = generate_username()
    bad_password = generate_string()

    if not cluster:
        # Test connection with bad username password
        with pytest.raises(ServerConnectionError):
            await client.db(db.name, bad_username, bad_password, verify=True)

    # Test connection with missing database
    with pytest.raises(ServerConnectionError):
        await client.db(bad_db_name, bad_username, bad_password, verify=True)
    await client.close()

    # Test connection with invalid host URL
    client = ArangoClient(hosts='http://127.0.0.1:8500')
    with pytest.raises(ServerConnectionError) as err:
        await client.db(db.name, username, password, verify=True)
    assert 'bad connection' in str(err.value)
    await client.close()


async def test_client_custom_http_client(db, username, password):

    # Define custom HTTP client which increments the counter on any API call.
    class MyHTTPClient(DefaultHTTPClient):

        def __init__(self):
            super(MyHTTPClient, self).__init__()
            self.counter = 0

        async def send_request(
                self,
                session,
                method,
                url,
                headers=None,
                params=None,
                data=None,
                auth=None):
            self.counter += 1
            return await super(MyHTTPClient, self).send_request(
                session, method, url, headers, params, data, auth
            )

    http_client = MyHTTPClient()
    client = ArangoClient(
        hosts=f'http://127.0.0.1:{arango_image.get_port()}',
        http_client=http_client
    )
    # Set verify to True to send a test API call on initialization.
    await client.db(db.name, username, password, verify=True)
    assert http_client.counter == 1
    await client.close()
