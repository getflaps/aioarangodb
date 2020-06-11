from __future__ import absolute_import, unicode_literals

import pytest
from aioarangodb.exceptions import (
    IndexListError,
    IndexCreateError,
    IndexDeleteError,
    IndexLoadError
)
from aioarangodb.tests.helpers import assert_raises, extract
pytestmark = pytest.mark.asyncio


async def test_list_indexes(icol, bad_col):
    indexes = await icol.indexes()
    assert isinstance(indexes, list)
    assert len(indexes) > 0
    assert 'id' in indexes[0]
    assert 'type' in indexes[0]
    assert 'fields' in indexes[0]
    assert 'selectivity' in indexes[0]
    assert 'sparse' in indexes[0]
    assert 'unique' in indexes[0]

    with assert_raises(IndexListError) as err:
        await bad_col.indexes()
    assert err.value.error_code in {11, 1228}


async def test_add_hash_index(icol):
    icol = icol

    fields = ['attr1', 'attr2']
    result = await icol.add_hash_index(
        fields=fields,
        unique=True,
        sparse=True,
        deduplicate=True,
        name='hash_index',
        in_background=False
    )

    expected_index = {
        'sparse': True,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': True,
        'deduplicate': True,
        'name': 'hash_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_add_skiplist_index(icol):
    fields = ['attr1', 'attr2']
    result = await icol.add_skiplist_index(
        fields=fields,
        unique=True,
        sparse=True,
        deduplicate=True,
        name='skiplist_index',
        in_background=False
    )

    expected_index = {
        'sparse': True,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': True,
        'deduplicate': True,
        'name': 'skiplist_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_add_geo_index(icol):
    # Test add geo index with one attribute
    result = await icol.add_geo_index(
        fields=['attr1'],
        ordered=False,
        name='geo_index',
        in_background=True
    )

    expected_index = {
        'sparse': True,
        'type': 'geo',
        'fields': ['attr1'],
        'unique': False,
        'geo_json': False,
        'name': 'geo_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Test add geo index with two attributes
    result = await icol.add_geo_index(
        fields=['attr1', 'attr2'],
        ordered=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo',
        'fields': ['attr1', 'attr2'],
        'unique': False,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Test add geo index with more than two attributes (should fail)
    with assert_raises(IndexCreateError) as err:
        await icol.add_geo_index(fields=['attr1', 'attr2', 'attr3'])
    assert err.value.error_code == 10

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_add_fulltext_index(icol):
    # Test add fulltext index with one attributes
    result = await icol.add_fulltext_index(
        fields=['attr1'],
        min_length=10,
        name='fulltext_index',
        in_background=True
    )
    expected_index = {
        'sparse': True,
        'type': 'fulltext',
        'fields': ['attr1'],
        'min_length': 10,
        'unique': False,
        'name': 'fulltext_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Test add fulltext index with two attributes (should fail)
    with assert_raises(IndexCreateError) as err:
        await icol.add_fulltext_index(fields=['attr1', 'attr2'])
    assert err.value.error_code == 10

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_add_persistent_index(icol):
    # Test add persistent index with two attributes
    result = await icol.add_persistent_index(
        fields=['attr1', 'attr2'],
        unique=True,
        sparse=True,
        name='persistent_index',
        in_background=True
    )
    expected_index = {
        'sparse': True,
        'type': 'persistent',
        'fields': ['attr1', 'attr2'],
        'unique': True,
        'name': 'persistent_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_add_ttl_index(icol):
    # Test add persistent index with two attributes
    result = await icol.add_ttl_index(
        fields=['attr1'],
        expiry_time=1000,
        name='ttl_index',
        in_background=True
    )
    expected_index = {
        'type': 'ttl',
        'fields': ['attr1'],
        'expiry_time': 1000,
        'name': 'ttl_index'
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result['id'] in await extract('id', await icol.indexes())

    # Clean up the index
    await icol.delete_index(result['id'])


async def test_delete_index(icol, bad_col):
    old_indexes = set(await extract('id', await icol.indexes()))
    icol.add_hash_index(['attr3', 'attr4'], unique=True)
    icol.add_skiplist_index(['attr3', 'attr4'], unique=True)
    icol.add_fulltext_index(fields=['attr3'], min_length=10)

    new_indexes = set(await extract('id', await icol.indexes()))
    assert new_indexes.issuperset(old_indexes)

    indexes_to_delete = new_indexes - old_indexes
    for index_id in indexes_to_delete:
        assert await icol.delete_index(index_id) is True

    new_indexes = set(await extract('id', await icol.indexes()))
    assert new_indexes == old_indexes

    # Test delete missing indexes
    for index_id in indexes_to_delete:
        assert await icol.delete_index(index_id, ignore_missing=True) is False
    for index_id in indexes_to_delete:
        with assert_raises(IndexDeleteError) as err:
            await icol.delete_index(index_id, ignore_missing=False)
        assert err.value.error_code == 1212

    # Test delete indexes with bad collection
    for index_id in indexes_to_delete:
        with assert_raises(IndexDeleteError) as err:
            await bad_col.delete_index(index_id, ignore_missing=False)
        assert err.value.error_code in {11, 1228}


async def test_load_indexes(icol, bad_col):
    # Test load indexes
    assert await icol.load_indexes() is True

    # Test load indexes with bad collection
    with assert_raises(IndexLoadError) as err:
        await bad_col.load_indexes()
    assert err.value.error_code in {11, 1228}
