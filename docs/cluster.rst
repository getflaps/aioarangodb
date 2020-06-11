Clusters
--------

aioarangodb provides APIs for working with ArangoDB clusters. For more
information on the design and architecture, refer to `ArangoDB manual`_.

.. _ArangoDB manual: https://docs.arangodb.com

Coordinators
============

To connect to multiple ArangoDB coordinators, you must provide either a list of
host strings or a comma-separated string during client initialization.

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    # Single host
    client = ArangoClient(hosts='http://localhost:8529')

    # Multiple hosts (option 1: list)
    client = ArangoClient(hosts=['http://host1:8529', 'http://host2:8529'])

    # Multiple hosts (option 2: comma-separated string)
    client = ArangoClient(hosts='http://host1:8529,http://host2:8529')

By default, a `aiohttp.ClientSession`_ instance is created per coordinator. HTTP
requests to a host are sent using only its corresponding session. For more
information on how to override this behaviour, see :doc:`http`.

Load-Balancing Strategies
=========================

There are two load-balancing strategies available: "roundrobin" and "random"
(defaults to "roundrobin" if unspecified).

**Example:**

.. testcode::

    from aioarangodb import ArangoClient

    hosts = ['http://host1:8529', 'http://host2:8529']

    # Round-robin
    client = ArangoClient(hosts=hosts, host_resolver='roundrobin')

    # Random
    client = ArangoClient(hosts=hosts, host_resolver='random')

Administration
==============

Below is an example on how to manage clusters using aioarangodb.

.. code-block:: python

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "_system" database as root user.
    sys_db = await client.db('_system', username='root', password='passwd')

    # Get the Cluster API wrapper.
    cluster = sys_db.cluster

    # Get this server's ID.
    await cluster.server_id()

    # Get this server's role.
    await cluster.server_role()

    # Get the cluster health.
    await cluster.health()

    # Get statistics for a specific server.
    server_id = await cluster.server_id()
    await cluster.statistics(server_id)

    # Toggle maintenance mode (allowed values are "on" and "off").
    await cluster.toggle_maintenance_mode('on')
    await cluster.toggle_maintenance_mode('off')

See :ref:`ArangoClient` and :ref:`Cluster` for API specification.
