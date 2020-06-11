Replication
-----------

**Replication** allows you to replicate data onto another machine. It forms the
basis of all disaster recovery and failover features ArangoDB offers. For more
information, refer to `ArangoDB manual`_.

.. _ArangoDB manual: https://www.arangodb.com/docs/stable/architecture-replication.html


**Example:**

.. code-block:: python

    from aioarangodb import ArangoClient

    # Initialize the ArangoDB client.
    client = ArangoClient()

    # Connect to "test" database as root user.
    db = await client.db('test', username='root', password='passwd')

    # Get the Replication API wrapper.
    replication = db.replication

    # Create a new dump batch.
    batch = await replication.create_dump_batch(ttl=1000)

    # Extend an existing dump batch.
    await replication.extend_dump_batch(batch['id'], ttl=1000)

    # Get an overview of collections and indexes.
    await replication.inventory(
        batch_id=batch['id'],
        include_system=True,
        all_databases=True
    )

    # Get an overview of collections and indexes in a cluster.
    await replication.cluster_inventory(include_system=True)

    # Get the events data for given collection.
    await replication.dump(
        collection='students',
        batch_id=batch['id'],
        lower=0,
        upper=1000000,
        chunk_size=0,
        include_system=True,
        ticks=0,
        flush=True,
    )

    # Delete an existing dump batch.
    await replication.delete_dump_batch(batch['id'])

    # Get the logger state.
    await replication.logger_state()

    # Get the logger first tick value.
    await replication.logger_first_tick()

    # Get the replication applier configuration.
    await replication.applier_config()

    # Update the replication applier configuration.
    result = await replication.set_applier_config(
        endpoint='http://127.0.0.1:8529',
        database='test',
        username='root',
        password='passwd',
        max_connect_retries=120,
        connect_timeout=15,
        request_timeout=615,
        chunk_size=0,
        auto_start=True,
        adaptive_polling=False,
        include_system=True,
        auto_resync=True,
        auto_resync_retries=3,
        initial_sync_max_wait_time=405,
        connection_retry_wait_time=25,
        idle_min_wait_time=2,
        idle_max_wait_time=3,
        require_from_present=False,
        verbose=True,
        restrict_type='include',
        restrict_collections=['students']
    )

    # Get the replication applier state.
    await replication.applier_state()

    # Start the replication applier.
    await replication.start_applier()

    # Stop the replication applier.
    await replication.stop_applier()

    # Get the server ID.
    await replication.server_id()

    # Synchronize data from a remote (master) endpoint
    await replication.synchronize(
        endpoint='tcp://master:8500',
        database='test',
        username='root',
        password='passwd',
        include_system=False,
        incremental=False,
        restrict_type='include',
        restrict_collections=['students']
    )

See :ref:`Replication` for API specification.
