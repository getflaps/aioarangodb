.. image:: /static/logo.png

|

Welcome to the documentation for **aioarangodb**, a Python driver for ArangoDB_ with AsyncIO.


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

.. _ArangoDB: https://www.arangodb.com
.. _PyPi: https://pypi.python.org/pypi/aioarangodb
.. _GitHub: https://github.com/bloodbare/aioarangodb


Contents
========

.. toctree::
    :maxdepth: 1

    overview
    database
    collection
    document
    indexes
    graph
    aql
    cursor
    async
    batch
    transaction
    admin
    user
    task
    wal
    pregel
    foxx
    view
    analyzer
    threading
    errors
    replication
    cluster
    serializer
    errno
    contributing
    specs
