Databases
---------

ArangoDB server can have an arbitrary number of **databases**. Each database
has its own set of :doc:`collections <collection>` and :doc:`graphs <graph>`.
There is a special database named ``_system``, which cannot be dropped and
provides operations for managing users, permissions and other databases. Most
of the operations can only be executed by admin users. See :doc:`user` for more
information.

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "_system" database as root user.
    # This returns an API wrapper for "_system" database.
    sys_db = client.db('_system', username='root', password='passwd')

    # List all databases.
    await sys_db.databases()

    # Create a new database named "test" if it does not exist.
    # Only root user has access to it at time of its creation.
    if not await sys_db.has_database('test'):
        await sys_db.create_database('test')

    # Delete the database.
    await sys_db.delete_database('test')

    # Create a new database named "test" along with a new set of users.
    # Only "jane", "john", "jake" and root user have access to it.
    if not await sys_db.has_database('test'):
        await sys_db.create_database(
            name='test',
            users=[
                {'username': 'jane', 'password': 'foo', 'active': True},
                {'username': 'john', 'password': 'bar', 'active': True},
                {'username': 'jake', 'password': 'baz', 'active': True},
            ],
        )

    # Connect to the new "test" database as user "jane".
    db = await client.db('test', username='jane', password='foo')

    # Make sure that user "jane" has read and write permissions.
    await sys_db.update_permission(username='jane', permission='rw', database='test')

    # Retrieve various database and server information.
    db.name
    db.username
    await db.version()
    await db.status()
    await db.details()
    await db.collections()
    await db.graphs()
    await db.engine()

    # Delete the database. Note that the new users will remain.
    await sys_db.delete_database('test')

See :ref:`ArangoClient` and :ref:`StandardDatabase` for API specification.
