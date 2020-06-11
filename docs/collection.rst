Collections
-----------

A **collection** contains :doc:`documents <document>`. It is uniquely identified
by its name which must consist only of hyphen, underscore and alphanumeric
characters. There are three types of collections in aioarangodb:

* **Standard Collection:** contains regular documents.
* **Vertex Collection:** contains vertex documents for graphs. See
  :ref:`here <vertex-collections>` for more details.
* **Edge Collection:** contains edge documents for graphs. See
  :ref:`here <edge-collections>` for more details.

Here is an example showing how you can manage standard collections:

.. testcode::

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # List all collections in the database.
    await db.collections()

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if db.has_collection('students'):
        students = db.collection('students')
    else:
        students = await db.create_collection('students')

    # Retrieve collection properties.
    students.name
    students.db_name
    await students.properties()
    await students.revision()
    await students.statistics()
    await students.checksum()
    await students.count()

    # Perform various operations.
    await students.load()
    await students.unload()
    await students.truncate()
    await students.configure(journal_size=3000000)

    # Delete the collection.
    await db.delete_collection('students')

See :ref:`StandardDatabase` and :ref:`StandardCollection` for API specification.
