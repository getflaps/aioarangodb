Views and ArangoSearch
----------------------

Python-arango supports **view** management. For more information on view
properties, refer to `ArangoDB manual`_.

.. _ArangoDB manual: https://docs.arangodb.com

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Retrieve list of views.
    await db.views()

    # Create a view.
    await db.create_view(
        name='foo',
        view_type='arangosearch',
        properties={
            'cleanupIntervalStep': 0,
            'consolidationIntervalMsec': 0
        }
    )

    # Rename a view.
    await db.rename_view('foo', 'bar')

    # Retrieve view properties.
    await db.view('bar')

    # Partially update view properties.
    await db.update_view(
        name='bar',
        properties={
            'cleanupIntervalStep': 1000,
            'consolidationIntervalMsec': 200
        }
    )

    # Replace view properties. Unspecified ones are reset to default.
    await db.replace_view(
        name='bar',
        properties={'cleanupIntervalStep': 2000}
    )

    # Delete a view.
    await db.delete_view('bar')


Python-arango also supports **ArangoSearch** views.

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Create an ArangoSearch view.
    await db.create_arangosearch_view(
        name='arangosearch_view',
        properties={'cleanupIntervalStep': 0}
    )

    # Partially update an ArangoSearch view.
    await db.update_arangosearch_view(
        name='arangosearch_view',
        properties={'cleanupIntervalStep': 1000}
    )

    # Replace an ArangoSearch view.
    await db.replace_arangosearch_view(
        name='arangosearch_view',
        properties={'cleanupIntervalStep': 2000}
    )

    # ArangoSearch views can be retrieved or deleted using regular view API
    await db.view('arangosearch_view')
    await db.delete_view('arangosearch_view')


For more information on the content of view **properties**, see
https://www.arangodb.com/docs/stable/http/views-arangosearch.html

Refer to :ref:`StandardDatabase` class for API specification.
