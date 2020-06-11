from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from aioarangodb.exceptions import (
    DatabasePropertiesError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
)
from aioarangodb.tests.helpers import (
    assert_raises,
    extract,
    generate_db_name,
    generate_username,
    generate_string,
)
pytestmark = pytest.mark.asyncio


async def test_user_management(sys_db, bad_db):
    # Test create user
    username = generate_username()
    password = generate_string()
    assert not await sys_db.has_user(username)

    new_user = await sys_db.create_user(
        username=username,
        password=password,
        active=True,
        extra={'foo': 'bar'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is True
    assert new_user['extra'] == {'foo': 'bar'}
    assert await sys_db.has_user(username)

    # Test create duplicate user
    with assert_raises(UserCreateError) as err:
        await sys_db.create_user(
            username=username,
            password=password
        )
    assert err.value.error_code == 1702

    # Test list users
    for user in await sys_db.users():
        assert isinstance(user['username'], string_types)
        assert isinstance(user['active'], bool)
        assert isinstance(user['extra'], dict)
    assert await sys_db.user(username) == new_user

    # Test list users with bad database
    with assert_raises(UserListError) as err:
        await bad_db.users()
    assert err.value.error_code in {11, 1228}

    # Test get user
    users = await sys_db.users()
    for user in users:
        assert 'active' in user
        assert 'extra' in user
        assert 'username' in user
    assert username in await extract('username', await sys_db.users())

    # Test get missing user
    with assert_raises(UserGetError) as err:
        await sys_db.user(generate_username())
    assert err.value.error_code == 1703

    # Update existing user
    new_user = await sys_db.update_user(
        username=username,
        password=password,
        active=False,
        extra={'bar': 'baz'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is False
    assert new_user['extra'] == {'bar': 'baz'}
    assert await sys_db.user(username) == new_user

    # Update missing user
    with assert_raises(UserUpdateError) as err:
        await sys_db.update_user(
            username=generate_username(),
            password=generate_string()
        )
    assert err.value.error_code == 1703

    # Replace existing user
    new_user = await sys_db.replace_user(
        username=username,
        password=password,
        active=False,
        extra={'baz': 'qux'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is False
    assert new_user['extra'] == {'baz': 'qux'}
    assert await sys_db.user(username) == new_user

    # Replace missing user
    with assert_raises(UserReplaceError) as err:
        await sys_db.replace_user(
            username=generate_username(),
            password=generate_string()
        )
    assert err.value.error_code == 1703

    # Delete an existing user
    assert await sys_db.delete_user(username) is True

    # Delete a missing user
    with assert_raises(UserDeleteError) as err:
        await sys_db.delete_user(username, ignore_missing=False)
    assert err.value.error_code == 1703
    assert await sys_db.delete_user(username, ignore_missing=True) is False


async def test_user_change_password(client, sys_db, cluster):
    if cluster:
        pytest.skip('Not tested in a cluster setup')

    username = generate_username()
    password1 = generate_string()
    password2 = generate_string()

    await sys_db.create_user(username, password1)
    await sys_db.update_permission(username, 'rw', sys_db.name)

    db1 = await client.db(sys_db.name, username, password1)
    db2 = await client.db(sys_db.name, username, password2)

    # Check authentication
    assert isinstance(await db1.properties(), dict)
    with assert_raises(DatabasePropertiesError) as err:
        await db2.properties()
    assert err.value.http_code == 401

    # Update the user password and check again
    await sys_db.update_user(username, password2)
    assert isinstance(await db2.properties(), dict)
    with assert_raises(DatabasePropertiesError) as err:
        await db1.properties()
    assert err.value.http_code == 401

    # Replace the user password back and check again
    await sys_db.update_user(username, password1)
    assert isinstance(await db1.properties(), dict)
    with assert_raises(DatabasePropertiesError) as err:
        await db2.properties()
    assert err.value.http_code == 401


async def test_user_create_with_new_database(client, sys_db, cluster):
    if cluster:
        pytest.skip('Not tested in a cluster setup')

    db_name = generate_db_name()

    username1 = generate_username()
    username2 = generate_username()
    username3 = generate_username()

    password1 = generate_string()
    password2 = generate_string()
    password3 = generate_string()

    result = await sys_db.create_database(
        name=db_name,
        users=[
            {'username': username1, 'password': password1, 'active': True},
            {'username': username2, 'password': password2, 'active': True},
            {'username': username3, 'password': password3, 'active': False},
        ]
    )
    assert result is True

    await sys_db.update_permission(username1, permission='rw', database=db_name)
    await sys_db.update_permission(username2, permission='rw', database=db_name)
    await sys_db.update_permission(username3, permission='rw', database=db_name)

    # Test if the users were created properly
    usernames = await extract('username', await sys_db.users())
    assert all(u in usernames for u in [username1, username2, username3])

    # Test if the first user has access to the database
    db = await client.db(db_name, username1, password1)
    await db.properties()

    # Test if the second user also has access to the database
    db = await client.db(db_name, username2, password2)
    await db.properties()

    # Test if the third user has access to the database (should not)
    db = await client.db(db_name, username3, password3)
    with assert_raises(DatabasePropertiesError) as err:
        await db.properties()
    assert err.value.http_code == 401
