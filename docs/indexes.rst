Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge collection has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`ArangoDB manual`_.

.. _ArangoDB manual: https://docs.arangodb.com

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Create a new collection named "cities".
    cities = await db.create_collection('cities')

    # List the indexes in the collection.
    await cities.indexes()

    # Add a new hash index on document fields "continent" and "country".
    index = await cities.add_hash_index(fields=['continent', 'country'], unique=True)

    # Add new fulltext indexes on fields "continent" and "country".
    index = await cities.add_fulltext_index(fields=['continent'])
    index = await cities.add_fulltext_index(fields=['country'])

    # Add a new skiplist index on field 'population'.
    index = await cities.add_skiplist_index(fields=['population'], sparse=False)

    # Add a new geo-spatial index on field 'coordinates'.
    index = await cities.add_geo_index(fields=['coordinates'])

    # Add a new persistent index on field 'currency'.
    index = await cities.add_persistent_index(fields=['currency'], sparse=True)

    # Add a new TTL (time-to-live) index on field 'currency'.
    index = await cities.add_ttl_index(fields=['ttl'], expiry_time=200)

    # Indexes may be added with a name that can be referred to in AQL queries.
    index = await cities.add_hash_index(fields=['country'], name='my_hash_index')

    # Delete the last index from the collection.
    await cities.delete_index(index['id'])

See :ref:`StandardCollection` for API specification.
