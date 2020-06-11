from __future__ import absolute_import, unicode_literals

from six import string_types
import pytest

from aioarangodb.collection import StandardCollection
from aioarangodb.exceptions import (
    CollectionChecksumError,
    CollectionConfigureError,
    CollectionLoadError,
    CollectionPropertiesError,
    CollectionRenameError,
    CollectionRevisionError,
    CollectionRotateJournalError,
    CollectionStatisticsError,
    CollectionTruncateError,
    CollectionUnloadError,
    CollectionCreateError,
    CollectionListError,
    CollectionDeleteError,
    CollectionRecalculateCountError
)
from aioarangodb.tests.helpers import assert_raises, extract, generate_col_name
pytestmark = pytest.mark.asyncio


async def test_collection_attributes(db, col, username):
    assert col.context in ['default', 'async', 'batch', 'transaction']
    assert col.username == username
    assert col.db_name == db.name
    assert col.name.startswith('test_collection')
    assert repr(col) == '<StandardCollection {}>'.format(col.name)


async def test_collection_misc_methods(col, bad_col, cluster):
    # Test get properties
    properties = await col.properties()
    assert properties['name'] == col.name
    assert properties['system'] is False

    # Test get properties with bad collection
    with assert_raises(CollectionPropertiesError) as err:
        await bad_col.properties()
    assert err.value.error_code in {11, 1228}

    # Test configure properties
    prev_sync = properties['sync']
    properties = await col.configure(
        sync=not prev_sync,
        journal_size=10000000
    )
    assert properties['name'] == col.name
    assert properties['system'] is False
    assert properties['sync'] is not prev_sync

    # Test configure properties with bad collection
    with assert_raises(CollectionConfigureError) as err:
        await bad_col.configure(sync=True, journal_size=10000000)
    assert err.value.error_code in {11, 1228}

    # Test get statistics
    stats = await col.statistics()
    assert isinstance(stats, dict)
    assert 'indexes' in stats

    # Test get statistics with bad collection
    with assert_raises(CollectionStatisticsError) as err:
        await bad_col.statistics()
    assert err.value.error_code in {11, 1228}

    # Test get revision
    assert isinstance(await col.revision(), string_types)

    # Test get revision with bad collection
    with assert_raises(CollectionRevisionError) as err:
        await bad_col.revision()
    assert err.value.error_code in {11, 1228}

    # Test load into memory
    assert await col.load() is True

    # Test load with bad collection
    with assert_raises(CollectionLoadError) as err:
        await bad_col.load()
    assert err.value.error_code in {11, 1228}

    # Test unload from memory
    assert await col.unload() is True

    # Test unload with bad collection
    with assert_raises(CollectionUnloadError) as err:
        await bad_col.unload()
    assert err.value.error_code in {11, 1228}

    # Test rotate journal
    try:
        assert isinstance(await col.rotate(), bool)
    except CollectionRotateJournalError as err:
        assert err.error_code in {404, 1105}

    # Test rotate journal with bad collection
    with assert_raises(CollectionRotateJournalError) as err:
        await bad_col.rotate()
    assert err.value.error_code in {11, 1228}

    if cluster:
        await col.insert({})
    else:
        # Test checksum with empty collection
        assert int(await col.checksum(with_rev=True, with_data=False)) == 0
        assert int(await col.checksum(with_rev=True, with_data=True)) == 0
        assert int(await col.checksum(with_rev=False, with_data=False)) == 0
        assert int(await col.checksum(with_rev=False, with_data=True)) == 0

        # Test checksum with non-empty collection
        await col.insert({})
        assert int(await col.checksum(with_rev=True, with_data=False)) > 0
        assert int(await col.checksum(with_rev=True, with_data=True)) > 0
        assert int(await col.checksum(with_rev=False, with_data=False)) > 0
        assert int(await col.checksum(with_rev=False, with_data=True)) > 0

        # Test checksum with bad collection
        with assert_raises(CollectionChecksumError) as err:
            await bad_col.checksum()
        assert err.value.error_code in {11, 1228}

    # Test preconditions
    assert await col.count() == 1

    # Test truncate collection
    assert await col.truncate() is True
    assert await col.count() == 0

    # Test truncate with bad collection
    with assert_raises(CollectionTruncateError) as err:
        await bad_col.truncate()
    assert err.value.error_code in {11, 1228}

    # Test recalculate count
    assert await col.recalculate_count() is True

    # Test recalculate count with bad collection
    with assert_raises(CollectionRecalculateCountError) as err:
        await bad_col.recalculate_count()
    assert err.value.error_code in {11, 1228}


async def test_collection_management(db, bad_db, cluster):
    # Test create collection
    col_name = generate_col_name()
    assert await db.has_collection(col_name) is False

    col = await db.create_collection(
        name=col_name,
        sync=True,
        compact=False,
        journal_size=7774208,
        system=False,
        volatile=False,
        key_generator='traditional',
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=['test_attr'],
        index_bucket_count=10,
        replication_factor=1,
        shard_like='',
        sync_replication=False,
        enforce_replication_factor=False,
        sharding_strategy='community-compat',
        smart_join_attribute='test',
        write_concern=1
    )
    assert await db.has_collection(col_name) is True

    properties = await col.properties()
    assert 'key_options' in properties
    assert properties['name'] == col_name
    assert properties['sync'] is True
    assert properties['system'] is False

    # Test create duplicate collection
    with assert_raises(CollectionCreateError) as err:
        await db.create_collection(col_name)
    assert err.value.error_code == 1207

    # Test list collections
    assert all(
        entry['name'].startswith('test_collection')
        or entry['name'].startswith('_')
        for entry in await db.collections()
    )

    # Test list collections with bad database
    with assert_raises(CollectionListError) as err:
        await bad_db.collections()
    assert err.value.error_code in {11, 1228}

    # Test get collection object
    test_col = db.collection(col.name)
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    test_col = db[col.name]
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    # Test delete collection
    assert await db.delete_collection(col_name, system=False) is True
    assert col_name not in await extract('name', await db.collections())

    # Test drop missing collection
    with assert_raises(CollectionDeleteError) as err:
        await db.delete_collection(col_name)
    assert err.value.error_code == 1203
    assert await db.delete_collection(col_name, ignore_missing=True) is False

    if not cluster:
        # Test rename collection
        new_name = generate_col_name()
        col = await db.create_collection(new_name)
        assert await col.rename(new_name) is True
        assert col.name == new_name
        assert repr(col) == '<StandardCollection {}>'.format(new_name)

        # Try again (the operation should be idempotent)
        assert await col.rename(new_name) is True
        assert col.name == new_name
        assert repr(col) == '<StandardCollection {}>'.format(new_name)

        # Test rename with bad collection
        with assert_raises(CollectionRenameError) as err:
            await bad_db.collection(new_name).rename(new_name)
        assert err.value.error_code in {11, 1228}
