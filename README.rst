Introduction
============


.. image:: https://travis-ci.org/bloodbare/aioarangodb.svg?branch=master
    :target: https://travis-ci.org/bloodbare/aioarangodb
    :alt: Travis Build Status

.. image:: https://readthedocs.org/projects/aioarangodb/badge/?version=master
    :target: http://aioarangodb.readthedocs.io/en/master/?badge=master
    :alt: Documentation Status

.. image:: https://badge.fury.io/py/aioarangodb.svg
    :target: https://badge.fury.io/py/aioarangodb
    :alt: Package Version

.. image:: https://img.shields.io/badge/python-3.5%2C%203.6%2C%203.7-blue.svg
    :target: https://github.com/bloodbare/aioarangodb
    :alt: Python Versions

.. image:: https://coveralls.io/repos/github/bloodbare/aioarangodb/badge.svg?branch=master
    :target: https://coveralls.io/github/bloodbare/aioarangodb?branch=master
    :alt: Test Coverage

.. image:: https://img.shields.io/github/issues/bloodbare/aioarangodb.svg
    :target: https://github.com/bloodbare/aioarangodb/issues
    :alt: Issues Open

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
    :target: https://raw.githubusercontent.com/bloodbare/aioarangodb/master/LICENSE
    :alt: MIT License

|

Welcome to the GitHub page for **aioarangodb**, a Python driver for ArangoDB_ Asyncio only.

Announcements
=============

- This project is a fork of https://github.com/joowani/python-arango with only asyncio python>=3.5 support. Many thanks for the great job.

Features
========

- Pythonic interface
- Lightweight
- High API coverage

Compatibility
=============

- Python versions 3.5, 3.6 and 3.7 are supported
- aioArangoDB supports ArangoDB 3.5+

Installation
============

To install a stable version from PyPi_:

.. code-block:: bash

    ~$ pip install aioarangodb


To install the latest version directly from GitHub_:

.. code-block:: bash

    ~$ pip install -e git+git@github.com:bloodbare/aioarangodb.git@master#egg=aioarangodb

You may need to use ``sudo`` depending on your environment.

Getting Started
===============

Here is a simple usage example:

.. code-block:: python

    from aioarangodb import ArangoClient

    # Initialize the client for ArangoDB.
    client = ArangoClient(hosts='http://localhost:8529')

    # Connect to "_system" database as root user.
    sys_db = await client.db('_system', username='root', password='passwd')

    # Create a new database named "test".
    await await sys_db.create_database('test')

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Create a new collection named "students".
    students = await db.create_collection('students')

    # Add a hash index to the collection.
    await students.add_hash_index(fields=['name'], unique=True)

    # Insert new documents into the collection.
    await students.insert({'name': 'jane', 'age': 39})
    await students.insert({'name': 'josh', 'age': 18})
    await students.insert({'name': 'judy', 'age': 21})

    # Execute an AQL query and iterate through the result cursor.
    cursor = await db.aql.execute('FOR doc IN students RETURN doc')
    student_names = [document['name'] async for document in cursor]


Here is another example with graphs:

.. code-block:: python

    from aioarangodb import ArangoClient

    # Initialize the client for ArangoDB.
    client = ArangoClient(hosts='http://localhost:8529')

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Create a new graph named "school".
    graph = await db.create_graph('school')

    # Create vertex collections for the graph.
    students = await graph.create_vertex_collection('students')
    lectures = await graph.create_vertex_collection('lectures')

    # Create an edge definition (relation) for the graph.
    register = await graph.create_edge_definition(
        edge_collection='register',
        from_vertex_collections=['students'],
        to_vertex_collections=['lectures']
    )

    # Insert vertex documents into "students" (from) vertex collection.
    await students.insert({'_key': '01', 'full_name': 'Anna Smith'})
    await students.insert({'_key': '02', 'full_name': 'Jake Clark'})
    await students.insert({'_key': '03', 'full_name': 'Lisa Jones'})

    # Insert vertex documents into "lectures" (to) vertex collection.
    await lectures.insert({'_key': 'MAT101', 'title': 'Calculus'})
    await lectures.insert({'_key': 'STA101', 'title': 'Statistics'})
    await lectures.insert({'_key': 'CSC101', 'title': 'Algorithms'})

    # Insert edge documents into "register" edge collection.
    await register.insert({'_from': 'students/01', '_to': 'lectures/MAT101'})
    await register.insert({'_from': 'students/01', '_to': 'lectures/STA101'})
    await register.insert({'_from': 'students/01', '_to': 'lectures/CSC101'})
    await register.insert({'_from': 'students/02', '_to': 'lectures/MAT101'})
    await register.insert({'_from': 'students/02', '_to': 'lectures/STA101'})
    await register.insert({'_from': 'students/03', '_to': 'lectures/CSC101'})

    # Traverse the graph in outbound direction, breadth-first.
    result = await graph.traverse(
        start_vertex='students/01',
        direction='outbound',
        strategy='breadthfirst'
    )

Check out the documentation_ for more information.

Contributing
============

Please take a look at this page_ before submitting a pull request. Thanks!

.. _ArangoDB: https://www.arangodb.com
.. _releases: https://github.com/bloodbare/aioarangodb/releases
.. _PyPi: https://pypi.python.org/pypi/aioarangodb
.. _GitHub: https://github.com/bloodbare/aioarangodb
.. _documentation:
    http://aioarangodb.readthedocs.io/en/master/index.html
.. _page:
    http://aioarangodb.readthedocs.io/en/master/contributing.html