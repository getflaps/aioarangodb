Analyzers
---------

Python-arango supports **analyzers**. For more information on analyzers, refer
to `ArangoDB manual`_.

.. _ArangoDB manual: https://docs.arangodb.com

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Retrieve list of analyzers.
    await db.analyzers()

    # Create an analyzer.
    await db.create_analyzer(
        name='test_analyzer',
        analyzer_type='identity',
        properties={},
        features=[]
    )

    # Delete an analyzer.
    await db.delete_analyzer('test_analyzer', ignore_missing=True)

Refer to :ref:`StandardDatabase` class for API specification.