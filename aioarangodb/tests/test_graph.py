from __future__ import absolute_import, unicode_literals

from six import string_types

import pytest
from aioarangodb.collection import EdgeCollection
from aioarangodb.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    EdgeDefinitionListError,
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionReplaceError,
    EdgeListError,
    GraphListError,
    GraphCreateError,
    GraphDeleteError,
    GraphPropertiesError,
    GraphTraverseError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError
)
from aioarangodb.tests.helpers import (
    assert_raises,
    clean_doc,
    empty_collection,
    extract,
    generate_col_name,
    generate_graph_name,
    generate_doc_key,
)
pytestmark = pytest.mark.asyncio


async def test_graph_properties(graph, bad_graph, db):
    assert repr(graph) == '<Graph {}>'.format(graph.name)

    properties = await graph.properties()

    assert properties['id'] == '_graphs/{}'.format(graph.name)
    assert properties['name'] == graph.name
    assert len(properties['edge_definitions']) == 1
    assert 'orphan_collections' in properties
    assert isinstance(properties['revision'], string_types)

    # Test properties with bad database
    with assert_raises(GraphPropertiesError):
        await bad_graph.properties()

    new_graph_name = generate_graph_name()
    new_graph = await db.create_graph(new_graph_name)
    properties = await new_graph.properties()
    assert properties['id'] == '_graphs/{}'.format(new_graph_name)
    assert properties['name'] == new_graph_name
    assert properties['edge_definitions'] == []
    assert properties['orphan_collections'] == []
    assert isinstance(properties['revision'], string_types)


async def test_graph_management(db, bad_db):
    # Test create graph
    graph_name = generate_graph_name()
    assert await db.has_graph(graph_name) is False

    graph = await db.create_graph(graph_name)
    assert await db.has_graph(graph_name) is True
    assert graph.name == graph_name
    assert graph.db_name == db.name

    # Test create duplicate graph
    with assert_raises(GraphCreateError) as err:
        await db.create_graph(graph_name)
    assert err.value.error_code == 1925

    # Test get graph
    result = db.graph(graph_name)
    assert result.name == graph.name
    assert result.db_name == graph.db_name

    # Test get graphs
    result = await db.graphs()
    for entry in result:
        assert 'revision' in entry
        assert 'edge_definitions' in entry
        assert 'orphan_collections' in entry
    assert graph_name in await extract('name', await db.graphs())

    # Test get graphs with bad database
    with assert_raises(GraphListError) as err:
        await bad_db.graphs()
    assert err.value.error_code in {11, 1228}

    # Test delete graph
    assert await db.delete_graph(graph_name) is True
    assert graph_name not in await extract('name', await db.graphs())

    # Test delete missing graph
    with assert_raises(GraphDeleteError) as err:
        await db.delete_graph(graph_name)
    assert err.value.error_code == 1924
    assert await db.delete_graph(graph_name, ignore_missing=True) is False

    # Create a graph with vertex and edge collections and delete the graph
    graph = await db.create_graph(graph_name)
    ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()

    await graph.create_vertex_collection(fvcol_name)
    await graph.create_vertex_collection(tvcol_name)
    await graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name]
    )
    collections = await extract('name', await db.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    await db.delete_graph(graph_name)
    collections = await extract('name', await db.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    # Create a graph with vertex and edge collections and delete all
    graph = await db.create_graph(graph_name)
    await graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name]
    )
    await db.delete_graph(graph_name, drop_collections=True)
    collections = await extract('name', await db.collections())
    assert fvcol_name not in collections
    assert tvcol_name not in collections
    assert ecol_name not in collections


async def test_vertex_collection_management(db, graph, bad_graph):
    # Test create valid "from" vertex collection
    fvcol_name = generate_col_name()
    assert not await graph.has_vertex_collection(fvcol_name)
    assert not await db.has_collection(fvcol_name)

    fvcol = await graph.create_vertex_collection(fvcol_name)
    assert await graph.has_vertex_collection(fvcol_name)
    assert await db.has_collection(fvcol_name)
    assert fvcol.name == fvcol_name
    assert fvcol.graph == graph.name
    assert fvcol_name in repr(fvcol)
    assert fvcol_name in await graph.vertex_collections()
    assert fvcol_name in await extract('name', await db.collections())

    # Test create duplicate vertex collection
    with assert_raises(VertexCollectionCreateError) as err:
        await graph.create_vertex_collection(fvcol_name)
    assert err.value.error_code == 1938
    assert fvcol_name in await graph.vertex_collections()
    assert fvcol_name in await extract('name', await db.collections())

    # Test create valid "to" vertex collection
    tvcol_name = generate_col_name()
    assert not await graph.has_vertex_collection(tvcol_name)
    assert not await db.has_collection(tvcol_name)

    tvcol = await graph.create_vertex_collection(tvcol_name)
    assert await graph.has_vertex_collection(tvcol_name)
    assert await db.has_collection(tvcol_name)
    assert tvcol_name == tvcol_name
    assert tvcol.graph == graph.name
    assert tvcol_name in repr(tvcol)
    assert tvcol_name in await graph.vertex_collections()
    assert tvcol_name in await extract('name', await db.collections())

    # Test list vertex collection via bad database
    with assert_raises(VertexCollectionListError) as err:
        await bad_graph.vertex_collections()
    assert err.value.error_code in {11, 1228}

    # Test delete missing vertex collection
    with assert_raises(VertexCollectionDeleteError) as err:
        await graph.delete_vertex_collection(generate_col_name())
    assert err.value.error_code in {1926, 1928}

    # Test delete "to" vertex collection with purge option
    assert await graph.delete_vertex_collection(tvcol_name, purge=True) is True
    assert tvcol_name not in await graph.vertex_collections()
    assert fvcol_name in await extract('name', await db.collections())
    assert tvcol_name not in await extract('name', await db.collections())
    assert not await graph.has_vertex_collection(tvcol_name)

    # Test delete "from" vertex collection without purge option
    assert await graph.delete_vertex_collection(fvcol_name, purge=False) is True
    assert fvcol_name not in await graph.vertex_collections()
    assert fvcol_name in await extract('name', await db.collections())
    assert not await graph.has_vertex_collection(fvcol_name)


async def test_edge_definition_management(db, graph, bad_graph):
    ecol_name = generate_col_name()
    assert not await graph.has_edge_definition(ecol_name)
    assert not await graph.has_edge_collection(ecol_name)
    assert not await db.has_collection(ecol_name)

    # Test create edge definition with existing vertex collections
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    ecol_name = generate_col_name()
    ecol = await graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name]
    )
    assert ecol.name == ecol_name
    assert ecol.graph == graph.name
    assert repr(ecol) == '<EdgeCollection {}>'.format(ecol.name)
    assert {
        'edge_collection': ecol_name,
        'from_vertex_collections': [fvcol_name],
        'to_vertex_collections': [tvcol_name]
    } in await graph.edge_definitions()
    assert ecol_name in await extract('name', await db.collections())

    vertex_collections = await graph.vertex_collections()
    assert fvcol_name in vertex_collections
    assert tvcol_name in vertex_collections

    # Test create duplicate edge definition
    with assert_raises(EdgeDefinitionCreateError) as err:
        await graph.create_edge_definition(
            edge_collection=ecol_name,
            from_vertex_collections=[fvcol_name],
            to_vertex_collections=[tvcol_name]
        )
    assert err.value.error_code == 1920

    # Test create edge definition with missing vertex collection
    bad_vcol_name = generate_col_name()
    ecol_name = generate_col_name()
    ecol = await graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[bad_vcol_name],
        to_vertex_collections=[bad_vcol_name]
    )
    assert await graph.has_edge_definition(ecol_name)
    assert await graph.has_edge_collection(ecol_name)
    assert ecol.name == ecol_name
    assert {
        'edge_collection': ecol_name,
        'from_vertex_collections': [bad_vcol_name],
        'to_vertex_collections': [bad_vcol_name]
    } in await graph.edge_definitions()
    assert bad_vcol_name in await graph.vertex_collections()
    assert bad_vcol_name in await extract('name', await db.collections())
    assert bad_vcol_name in await extract('name', await db.collections())

    # Test list edge definition with bad database
    with assert_raises(EdgeDefinitionListError) as err:
        await bad_graph.edge_definitions()
    assert err.value.error_code in {11, 1228}

    # Test replace edge definition (happy path)
    ecol = await graph.replace_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[tvcol_name],
        to_vertex_collections=[fvcol_name]
    )
    assert isinstance(ecol, EdgeCollection)
    assert ecol.name == ecol_name
    assert {
        'edge_collection': ecol_name,
        'from_vertex_collections': [tvcol_name],
        'to_vertex_collections': [fvcol_name]
    } in await graph.edge_definitions()

    # Test replace missing edge definition
    bad_ecol_name = generate_col_name()
    with assert_raises(EdgeDefinitionReplaceError):
        await graph.replace_edge_definition(
            edge_collection=bad_ecol_name,
            from_vertex_collections=[],
            to_vertex_collections=[fvcol_name]
        )

    # Test delete missing edge definition
    with assert_raises(EdgeDefinitionDeleteError) as err:
        await graph.delete_edge_definition(bad_ecol_name)
    assert err.value.error_code == 1930

    # Test delete existing edge definition with purge
    assert await graph.delete_edge_definition(ecol_name, purge=True) is True
    assert ecol_name not in \
        await extract('edge_collection', await graph.edge_definitions())
    assert not await graph.has_edge_definition(ecol_name)
    assert not await graph.has_edge_collection(ecol_name)
    assert ecol_name not in await extract('name', await db.collections())


async def test_create_graph_with_edge_definition(db):
    new_graph_name = generate_graph_name()
    new_ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    ovcol_name = generate_col_name()

    edge_definition = {
        'edge_collection': new_ecol_name,
        'from_vertex_collections': [fvcol_name],
        'to_vertex_collections': [tvcol_name]
    }
    new_graph = await db.create_graph(
        new_graph_name,
        edge_definitions=[edge_definition],
        orphan_collections=[ovcol_name]
    )
    assert edge_definition in await new_graph.edge_definitions()


async def test_vertex_management(fvcol, bad_fvcol, fvdocs):
    # Test insert vertex with no key
    result = await fvcol.insert({})
    assert await fvcol.has(result['_key'])
    assert await fvcol.count() == 1
    await empty_collection(fvcol)

    # Test insert vertex with ID
    vertex_id = fvcol.name + '/' + 'foo'
    await fvcol.insert({'_id': vertex_id})
    assert await fvcol.has('foo')
    assert await fvcol.has(vertex_id)
    assert await fvcol.count() == 1
    await empty_collection(fvcol)

    # Test insert vertex with return_new set to True
    result = await fvcol.insert({'_id': vertex_id}, return_new=True)
    assert 'new' in result
    assert 'vertex' in result
    assert await fvcol.count() == 1
    await empty_collection(fvcol)

    with assert_raises(DocumentParseError) as err:
        await fvcol.insert({'_id': generate_col_name() + '/' + 'foo'})
    assert 'bad collection name' in err.value.message

    vertex = fvdocs[0]
    key = vertex['_key']

    # Test insert first valid vertex
    result = await fvcol.insert(vertex, sync=True)
    assert result['_key'] == key
    assert '_rev' in result
    assert await fvcol.has(vertex) and await fvcol.has(key)
    assert await fvcol.count() == 1
    assert (await fvcol.get(key))['val'] == vertex['val']

    # Test insert duplicate vertex
    with assert_raises(DocumentInsertError) as err:
        await fvcol.insert(vertex)
    assert err.value.error_code in {1202, 1210}
    assert await fvcol.count() == 1

    vertex = fvdocs[1]
    key = vertex['_key']

    # Test insert second valid vertex
    result = await fvcol.insert(vertex)
    assert result['_key'] == key
    assert '_rev' in result
    assert await fvcol.has(vertex) and await fvcol.has(key)
    assert await fvcol.count() == 2
    assert (await fvcol.get(key))['val'] == vertex['val']

    vertex = fvdocs[2]
    key = vertex['_key']

    # Test insert third valid vertex with silent set to True
    assert await fvcol.insert(vertex, silent=True) is True
    assert await fvcol.count() == 3
    assert (await fvcol.get(key))['val'] == vertex['val']

    # Test get missing vertex
    if fvcol.context != 'transaction':
        assert await fvcol.get(generate_doc_key()) is None

    # Test get existing edge by body with "_key" field
    result = await fvcol.get({'_key': key})
    assert await clean_doc(result) == vertex

    # Test get existing edge by body with "_id" field
    result = await fvcol.get({'_id': fvcol.name + '/' + key})
    assert await clean_doc(result) == vertex

    # Test get existing vertex by key
    result = await fvcol.get(key)
    assert await clean_doc(result) == vertex

    # Test get existing vertex by ID
    result = await fvcol.get(fvcol.name + '/' + key)
    assert await clean_doc(result) == vertex

    # Test get existing vertex with bad revision
    old_rev = result['_rev']
    with assert_raises(DocumentRevisionError) as err:
        await fvcol.get(key, rev=old_rev + '1', check_rev=True)
    assert err.value.error_code in {1903, 1200}

    # Test get existing vertex with bad database
    with assert_raises(DocumentGetError) as err:
        await bad_fvcol.get(key)
    assert err.value.error_code in {11, 1228}

    # Test update vertex with a single field change
    assert 'foo' not in await fvcol.get(key)
    result = await fvcol.update({'_key': key, 'foo': 100})
    assert result['_key'] == key
    assert (await fvcol.get(key))['foo'] == 100
    old_rev = (await fvcol.get(key))['_rev']

    # Test update vertex with return_new and return_old set to True
    result = await fvcol.update(
        {'_key': key, 'foo': 100},
        return_old=True,
        return_new=True
    )
    assert 'old' in result
    assert 'new' in result
    assert 'vertex' in result

    # Test update vertex with silent set to True
    assert 'bar' not in await fvcol.get(vertex)
    assert await fvcol.update({'_key': key, 'bar': 200}, silent=True) is True
    assert (await fvcol.get(vertex))['bar'] == 200
    assert (await fvcol.get(vertex))['_rev'] != old_rev
    old_rev = (await fvcol.get(key))['_rev']

    # Test update vertex with multiple field changes
    result = await fvcol.update({'_key': key, 'foo': 200, 'bar': 300})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] == 300
    old_rev = result['_rev']

    # Test update vertex with correct revision
    result = await fvcol.update({'_key': key, '_rev': old_rev, 'bar': 400})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] == 400
    old_rev = result['_rev']

    # Test update vertex with bad revision
    if fvcol.context != 'transaction':
        new_rev = old_rev + '1'
        with assert_raises(DocumentRevisionError) as err:
            await fvcol.update({'_key': key, '_rev': new_rev, 'bar': 500})
        assert err.value.error_code in {1200, 1903}
        assert (await fvcol.get(key))['foo'] == 200
        assert (await fvcol.get(key))['bar'] == 400

    # Test update vertex in missing vertex collection
    with assert_raises(DocumentUpdateError) as err:
        await bad_fvcol.update({'_key': key, 'bar': 500})
    assert err.value.error_code in {11, 1228}
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] == 400

    # Test update vertex with sync set to True
    result = await fvcol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] == 500
    old_rev = result['_rev']

    # Test update vertex with keep_none set to True
    result = await fvcol.update({'_key': key, 'bar': None}, keep_none=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] is None
    old_rev = result['_rev']

    # Test update vertex with keep_none set to False
    result = await fvcol.update({'_key': key, 'foo': None}, keep_none=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert 'foo' not in (await fvcol.get(key))
    assert (await fvcol.get(key))['bar'] is None

    # Test replace vertex with a single field change
    result = await fvcol.replace({'_key': key, 'baz': 100})
    assert result['_key'] == key
    assert 'foo' not in await fvcol.get(key)
    assert 'bar' not in await fvcol.get(key)
    assert (await fvcol.get(key))['baz'] == 100
    old_rev = result['_rev']

    # Test replace vertex with return_new and return_old set to True
    result = await fvcol.replace(
        {'_key': key, 'baz': 100},
        return_old=True,
        return_new=True
    )
    assert 'old' in result
    assert 'new' in result
    assert 'vertex' in result

    # Test replace vertex with silent set to True
    assert await fvcol.replace({'_key': key, 'bar': 200}, silent=True) is True
    assert 'foo' not in await fvcol.get(key)
    assert 'baz' not in await fvcol.get(vertex)
    assert (await fvcol.get(vertex))['bar'] == 200
    assert await fvcol.count() == 3
    assert (await fvcol.get(vertex))['_rev'] != old_rev
    old_rev = (await fvcol.get(vertex))['_rev']

    # Test replace vertex with multiple field changes
    vertex = {'_key': key, 'foo': 200, 'bar': 300}
    result = await fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert await clean_doc(await fvcol.get(key)) == vertex
    old_rev = result['_rev']

    # Test replace vertex with correct revision
    vertex = {'_key': key, '_rev': old_rev, 'bar': 500}
    result = await fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert await clean_doc(await fvcol.get(key)) == await clean_doc(vertex)
    old_rev = result['_rev']

    # Test replace vertex with bad revision
    if fvcol.context != 'transaction':
        new_rev = old_rev + '10'
        vertex = {'_key': key, '_rev': new_rev, 'bar': 600}
        with assert_raises(DocumentRevisionError, DocumentReplaceError) as err:
            await fvcol.replace(vertex)
        assert err.value.error_code in {1200, 1903}
        assert (await fvcol.get(key))['bar'] == 500
        assert 'foo' not in await fvcol.get(key)

    # Test replace vertex with bad database
    with assert_raises(DocumentReplaceError) as err:
        await bad_fvcol.replace({'_key': key, 'bar': 600})
    assert err.value.error_code in {11, 1228}
    assert (await fvcol.get(key))['bar'] == 500
    assert 'foo' not in await fvcol.get(key)

    # Test replace vertex with sync set to True
    vertex = {'_key': key, 'bar': 400, 'foo': 200}
    result = await fvcol.replace(vertex, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await fvcol.get(key))['foo'] == 200
    assert (await fvcol.get(key))['bar'] == 400

    # Test delete vertex with bad revision
    if fvcol.context != 'transaction':
        old_rev = (await fvcol.get(key))['_rev']
        vertex['_rev'] = old_rev + '1'
        with assert_raises(DocumentRevisionError, DocumentDeleteError) as err:
            await fvcol.delete(vertex, check_rev=True)
        assert err.value.error_code in {1200, 1903}
        vertex['_rev'] = old_rev
        assert await fvcol.has(vertex)

    # Test delete missing vertex
    bad_key = generate_doc_key()
    with assert_raises(DocumentDeleteError) as err:
        await fvcol.delete(bad_key, ignore_missing=False)
    assert err.value.error_code == 1202
    if fvcol.context != 'transaction':
        assert await fvcol.delete(bad_key, ignore_missing=True) is False

    # Test delete existing vertex with sync set to True
    assert await fvcol.delete(vertex, sync=True, check_rev=False) is True
    if fvcol.context != 'transaction':
        assert await fvcol.get(vertex) is None
    assert not await fvcol.has(vertex)
    assert await fvcol.count() == 2

    # Test delete existing vertex with return_old set to True
    vertex = fvdocs[1]
    result = await fvcol.delete(vertex, return_old=True)
    assert 'old' in result
    assert await fvcol.count() == 1
    await empty_collection(fvcol)


async def test_vertex_management_via_graph(graph, fvcol):
    # Test insert vertex via graph object
    result = await graph.insert_vertex(fvcol.name, {})
    assert await fvcol.has(result['_key'])
    assert await fvcol.count() == 1
    vertex_id = result['_id']

    # Test get vertex via graph object
    assert (await graph.vertex(vertex_id))['_id'] == vertex_id

    # Test update vertex via graph object
    result = await graph.update_vertex({'_id': vertex_id, 'foo': 100})
    assert result['_id'] == vertex_id
    assert (await fvcol.get(vertex_id))['foo'] == 100

    # Test replace vertex via graph object
    result = await graph.replace_vertex({'_id': vertex_id, 'bar': 200})
    assert result['_id'] == vertex_id
    assert 'foo' not in await fvcol.get(vertex_id)
    assert (await fvcol.get(vertex_id))['bar'] == 200

    # Test delete vertex via graph object
    assert await graph.delete_vertex(vertex_id) is True
    assert not await fvcol.get(vertex_id)
    assert await fvcol.count() == 0


async def test_edge_management(ecol, bad_ecol, edocs, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        await fvcol.insert(vertex)
    for vertex in tvdocs:
        await tvcol.insert(vertex)

    edge = edocs[0]
    key = edge['_key']

    # Test insert edge with no key
    no_key_edge = {'_from': edge['_from'], '_to': edge['_to']}
    result = await ecol.insert(no_key_edge)
    assert await ecol.has(result['_key'])
    assert await ecol.count() == 1
    await empty_collection(ecol)

    # Test insert edge with return_new set to True
    result = await ecol.insert(no_key_edge, return_new=True)
    assert 'new' in result
    assert await ecol.has(result['edge']['_key'])
    assert await ecol.count() == 1
    await empty_collection(ecol)

    # Test insert vertex with ID
    edge_id = ecol.name + '/' + 'foo'
    await ecol.insert({
        '_id': edge_id,
        '_from': edge['_from'],
        '_to': edge['_to']
    })
    assert await ecol.has('foo')
    assert await ecol.has(edge_id)
    assert await ecol.count() == 1
    await empty_collection(ecol)

    with assert_raises(DocumentParseError) as err:
        await ecol.insert({
            '_id': generate_col_name() + '/' + 'foo',
            '_from': edge['_from'],
            '_to': edge['_to']
        })
    assert 'bad collection name' in err.value.message

    # Test insert first valid edge
    result = await ecol.insert(edge)
    assert result['_key'] == key
    assert '_rev' in result
    assert await ecol.has(edge) and await ecol.has(key)
    assert await ecol.count() == 1
    assert (await ecol.get(key))['_from'] == edge['_from']
    assert (await ecol.get(key))['_to'] == edge['_to']

    # Test insert duplicate edge
    with assert_raises(DocumentInsertError) as err:
        assert await ecol.insert(edge)
    assert err.value.error_code in {1202, 1210, 1906}
    assert await ecol.count() == 1

    edge = edocs[1]
    key = edge['_key']

    # Test insert second valid edge with silent set to True
    assert await ecol.insert(edge, sync=True, silent=True) is True
    assert await ecol.has(edge) and await ecol.has(key)
    assert await ecol.count() == 2
    assert (await ecol.get(key))['_from'] == edge['_from']
    assert (await ecol.get(key))['_to'] == edge['_to']

    # Test insert third valid edge using link method
    from_vertex = await fvcol.get(fvdocs[2])
    to_vertex = await tvcol.get(tvdocs[2])
    result = await ecol.link(from_vertex, to_vertex, sync=False)
    assert await ecol.has(result['_key'])
    assert await ecol.count() == 3

    # Test insert fourth valid edge using link method
    from_vertex = await fvcol.get(fvdocs[2])
    to_vertex = await tvcol.get(tvdocs[0])
    assert await ecol.link(
        from_vertex['_id'],
        to_vertex['_id'],
        {'_id': ecol.name + '/foo'},
        sync=True,
        silent=True
    ) is True
    assert await ecol.has('foo')
    assert await ecol.count() == 4

    with assert_raises(DocumentParseError) as err:
        assert await ecol.link({}, {})
    assert err.value.message == 'field "_id" required'

    # Test get missing vertex
    bad_document_key = generate_doc_key()
    if ecol.context != 'transaction':
        assert await ecol.get(bad_document_key) is None

    # Test get existing edge by body with "_key" field
    result = await ecol.get({'_key': key})
    assert await clean_doc(result) == edge

    # Test get existing edge by body with "_id" field
    result = await ecol.get({'_id': ecol.name + '/' + key})
    assert await clean_doc(result) == edge

    # Test get existing edge by key
    result = await ecol.get(key)
    assert await clean_doc(result) == edge

    # Test get existing edge by ID
    result = await ecol.get(ecol.name + '/' + key)
    assert await clean_doc(result) == edge

    # Test get existing edge with bad revision
    old_rev = result['_rev']
    with assert_raises(DocumentRevisionError) as err:
        await ecol.get(key, rev=old_rev + '1')
    assert err.value.error_code in {1903, 1200}

    # Test get existing edge with bad database
    with assert_raises(DocumentGetError) as err:
        await bad_ecol.get(key)
    assert err.value.error_code in {11, 1228}

    # Test update edge with a single field change
    assert 'foo' not in await ecol.get(key)
    result = await ecol.update({'_key': key, 'foo': 100})
    assert result['_key'] == key
    assert (await ecol.get(key))['foo'] == 100

    # Test update edge with return_old and return_new set to True
    result = await ecol.update(
        {'_key': key, 'foo': 100},
        return_old=True,
        return_new=True
    )
    assert 'old' in result
    assert 'new' in result
    assert 'edge' in result
    old_rev = (await ecol.get(key))['_rev']

    # Test update edge with multiple field changes
    result = await ecol.update({'_key': key, 'foo': 200, 'bar': 300})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 300
    old_rev = result['_rev']

    # Test update edge with correct revision
    result = await ecol.update({'_key': key, '_rev': old_rev, 'bar': 400})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 400
    old_rev = result['_rev']

    if ecol.context != 'transaction':
        # Test update edge with bad revision
        new_rev = old_rev + '1'
        with assert_raises(DocumentRevisionError, DocumentUpdateError):
            await ecol.update({'_key': key, '_rev': new_rev, 'bar': 500})
        assert (await ecol.get(key))['foo'] == 200
        assert (await ecol.get(key))['bar'] == 400

    # Test update edge in missing edge collection
    with assert_raises(DocumentUpdateError) as err:
        await bad_ecol.update({'_key': key, 'bar': 500})
    assert err.value.error_code in {11, 1228}
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 400

    # Test update edge with sync option
    result = await ecol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 500
    old_rev = result['_rev']

    # Test update edge with silent option
    assert await ecol.update({'_key': key, 'bar': 600}, silent=True) is True
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 600
    assert (await ecol.get(key))['_rev'] != old_rev
    old_rev = (await ecol.get(key))['_rev']

    # Test update edge without keep_none option
    result = await ecol.update({'_key': key, 'bar': None}, keep_none=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] is None
    old_rev = result['_rev']

    # Test update edge with keep_none option
    result = await ecol.update({'_key': key, 'foo': None}, keep_none=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert 'foo' not in await ecol.get(key)
    assert (await ecol.get(key))['bar'] is None

    # Test replace edge with a single field change
    edge['foo'] = 100
    result = await ecol.replace(edge)
    assert result['_key'] == key
    assert (await ecol.get(key))['foo'] == 100

    # Test replace edge with return_old and return_new set to True
    result = await ecol.replace(edge, return_old=True, return_new=True)
    assert 'old' in result
    assert 'new' in result
    assert 'edge' in result
    old_rev = (await ecol.get(key))['_rev']

    # Test replace edge with silent set to True
    edge['bar'] = 200
    assert await ecol.replace(edge, silent=True) is True
    assert (await ecol.get(key))['foo'] == 100
    assert (await ecol.get(key))['bar'] == 200
    assert (await ecol.get(key))['_rev'] != old_rev
    old_rev = (await ecol.get(key))['_rev']

    # Test replace edge with multiple field changes
    edge['foo'] = 200
    edge['bar'] = 300
    result = await ecol.replace(edge)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 200
    assert (await ecol.get(key))['bar'] == 300
    old_rev = result['_rev']

    # Test replace edge with correct revision
    edge['foo'] = 300
    edge['bar'] = 400
    edge['_rev'] = old_rev
    result = await ecol.replace(edge)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 300
    assert (await ecol.get(key))['bar'] == 400
    old_rev = result['_rev']

    edge['bar'] = 500
    if ecol.context != 'transaction':
        # Test replace edge with bad revision
        edge['_rev'] = old_rev + key
        with assert_raises(DocumentRevisionError, DocumentReplaceError) as err:
            await ecol.replace(edge)
        assert err.value.error_code in {1200, 1903}
        assert (await ecol.get(key))['foo'] == 300
        assert (await ecol.get(key))['bar'] == 400

    # Test replace edge with bad database
    with assert_raises(DocumentReplaceError) as err:
        await bad_ecol.replace(edge)
    assert err.value.error_code in {11, 1228}
    assert (await ecol.get(key))['foo'] == 300
    assert (await ecol.get(key))['bar'] == 400

    # Test replace edge with sync option
    result = await ecol.replace(edge, sync=True, check_rev=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert (await ecol.get(key))['foo'] == 300
    assert (await ecol.get(key))['bar'] == 500

    # Test delete edge with bad revision
    if ecol.context != 'transaction':
        old_rev = (await ecol.get(key))['_rev']
        edge['_rev'] = old_rev + '1'
        with assert_raises(DocumentRevisionError, DocumentDeleteError) as err:
            await ecol.delete(edge, check_rev=True)
        assert err.value.error_code in {1200, 1903}
        edge['_rev'] = old_rev
        assert await ecol.has(edge)

    # Test delete missing edge
    with assert_raises(DocumentDeleteError) as err:
        await ecol.delete(bad_document_key, ignore_missing=False)
    assert err.value.error_code == 1202
    if ecol.context != 'transaction':
        assert not await ecol.delete(bad_document_key, ignore_missing=True)

    # Test delete existing edge with sync set to True
    assert await ecol.delete(edge, sync=True, check_rev=False) is True
    if ecol.context != 'transaction':
        assert await ecol.get(edge) is None
    assert not await ecol.has(edge)

    # Test delete existing edge with return_old set to True
    await ecol.insert(edge)
    result = await ecol.delete(edge, return_old=True, check_rev=False)
    assert 'old' in result
    assert not await ecol.has(edge)
    await empty_collection(ecol)


async def test_vertex_edges(db, bad_db):
    graph_name = generate_graph_name()
    vcol_name = generate_col_name()
    ecol_name = generate_col_name()

    # Prepare test documents
    anna = {'_id': '{}/anna'.format(vcol_name)}
    dave = {'_id': '{}/dave'.format(vcol_name)}
    josh = {'_id': '{}/josh'.format(vcol_name)}
    mary = {'_id': '{}/mary'.format(vcol_name)}
    tony = {'_id': '{}/tony'.format(vcol_name)}

    # Create test graph, vertex and edge collections
    school = await db.create_graph(graph_name)

    vcol = await school.create_vertex_collection(vcol_name)
    ecol = await school.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[vcol_name],
        to_vertex_collections=[vcol_name]
    )
    # Insert test vertices into the graph
    await vcol.insert(anna)
    await vcol.insert(dave)
    await vcol.insert(josh)
    await vcol.insert(mary)
    await vcol.insert(tony)

    # Insert test edges into the graph
    await ecol.link(anna, dave)
    await ecol.link(josh, dave)
    await ecol.link(mary, dave)
    await ecol.link(tony, dave)
    await ecol.link(dave, anna)

    # Test edges with default direction (both)
    result = await ecol.edges(dave)
    assert 'stats' in result
    assert 'filtered' in result['stats']
    assert 'scanned_index' in result['stats']
    assert len(result['edges']) == 5

    result = await ecol.edges(anna)
    assert len(result['edges']) == 2

    # Test edges with direction set to "in"
    result = await ecol.edges(dave, direction='in')
    assert len(result['edges']) == 4

    result = await ecol.edges(anna, direction='in')
    assert len(result['edges']) == 1

    # Test edges with direction set to "out"
    result = await ecol.edges(dave, direction='out')
    assert len(result['edges']) == 1

    result = await ecol.edges(anna, direction='out')
    assert len(result['edges']) == 1

    bad_graph = bad_db.graph(graph_name)
    with assert_raises(EdgeListError) as err:
        await bad_graph.edge_collection(ecol_name).edges(dave)
    assert err.value.error_code in {11, 1228}


async def test_edge_management_via_graph(graph, ecol, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        await fvcol.insert(vertex)
    for vertex in tvdocs:
        await tvcol.insert(vertex)
    await empty_collection(ecol)

    # Get a random "from" vertex
    from_vertex = await fvcol.random()
    assert await graph.has_vertex(from_vertex)

    # Get a random "to" vertex
    to_vertex = await tvcol.random()
    assert await graph.has_vertex(to_vertex)

    # Test insert edge via graph object
    result = await graph.insert_edge(
        ecol.name,
        {'_from': from_vertex['_id'], '_to': to_vertex['_id']}
    )
    assert await ecol.has(result['_key'])
    assert await graph.has_edge(result['_id'])
    assert await ecol.count() == 1

    # Test link vertices via graph object
    result = await graph.link(ecol.name, from_vertex, to_vertex)
    assert await ecol.has(result['_key'])
    assert await ecol.count() == 2
    edge_id = result['_id']

    # Test get edge via graph object
    assert (await graph.edge(edge_id))['_id'] == edge_id

    # Test list edges via graph object
    result = await graph.edges(ecol.name, from_vertex, direction='out')
    assert 'stats' in result
    assert len(result['edges']) == 2

    result = await graph.edges(ecol.name, from_vertex, direction='in')
    assert 'stats' in result
    assert len(result['edges']) == 0

    # Test update edge via graph object
    result = await graph.update_edge({'_id': edge_id, 'foo': 100})
    assert result['_id'] == edge_id
    assert (await ecol.get(edge_id))['foo'] == 100

    # Test replace edge via graph object
    result = await graph.replace_edge({
        '_id': edge_id,
        '_from': from_vertex['_id'],
        '_to': to_vertex['_id'],
        'bar': 200
    })
    assert result['_id'] == edge_id
    assert 'foo' not in await ecol.get(edge_id)
    assert (await ecol.get(edge_id))['bar'] == 200

    # Test delete edge via graph object
    assert await graph.delete_edge(edge_id) is True
    assert not await ecol.has(edge_id)
    assert await ecol.count() == 1


async def test_traverse(db):
    # Create test graph, vertex and edge collections
    school = await db.create_graph(generate_graph_name())
    profs = await school.create_vertex_collection(generate_col_name())
    classes = await school.create_vertex_collection(generate_col_name())
    teaches = await school.create_edge_definition(
        edge_collection=generate_col_name(),
        from_vertex_collections=[profs.name],
        to_vertex_collections=[classes.name]
    )
    # Insert test vertices into the graph
    await profs.insert({'_key': 'anna', 'name': 'Professor Anna'})
    await profs.insert({'_key': 'andy', 'name': 'Professor Andy'})
    await classes.insert({'_key': 'CSC101', 'name': 'Introduction to CS'})
    await classes.insert({'_key': 'MAT223', 'name': 'Linear Algebra'})
    await classes.insert({'_key': 'STA201', 'name': 'Statistics'})
    await classes.insert({'_key': 'MAT101', 'name': 'Calculus I'})
    await classes.insert({'_key': 'MAT102', 'name': 'Calculus II'})

    # Insert test edges into the graph
    await teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/CSC101'.format(classes.name)
    })
    await teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/STA201'.format(classes.name)
    })
    await teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/MAT223'.format(classes.name)
    })
    await teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT101'.format(classes.name)
    })
    await teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT102'.format(classes.name)
    })
    await teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT223'.format(classes.name)
    })

    # Traverse the graph with default settings
    result = await school.traverse('{}/anna'.format(profs.name))
    visited = await extract('_key', result['vertices'])
    assert visited == ['CSC101', 'MAT223', 'STA201', 'anna']

    for path in result['paths']:
        for vertex in path['vertices']:
            assert set(vertex) == {'_id', '_key', '_rev', 'name'}
        for edge in path['edges']:
            assert set(edge) == {'_id', '_key', '_rev', '_to', '_from'}

    result = await school.traverse('{}/andy'.format(profs.name))
    visited = await extract('_key', result['vertices'])
    assert visited == ['MAT101', 'MAT102', 'MAT223', 'andy']

    # Traverse the graph with an invalid start vertex
    with assert_raises(GraphTraverseError):
        await school.traverse('invalid')

    with assert_raises(GraphTraverseError):
        bad_col_name = generate_col_name()
        await school.traverse('{}/hanna'.format(bad_col_name))

    with assert_raises(GraphTraverseError):
        await school.traverse('{}/anderson'.format(profs.name))

    # Travers the graph with max iteration of 0
    with assert_raises(GraphTraverseError):
        await school.traverse('{}/andy'.format(profs.name), max_iter=0)

    # Traverse the graph with max depth of 0
    result = await school.traverse('{}/andy'.format(profs.name), max_depth=0)
    assert await extract('_key', result['vertices']) == ['andy']

    result = await school.traverse('{}/anna'.format(profs.name), max_depth=0)
    assert await extract('_key', result['vertices']) == ['anna']

    # Traverse the graph with min depth of 2
    result = await school.traverse('{}/andy'.format(profs.name), min_depth=2)
    assert await extract('_key', result['vertices']) == []

    result = await school.traverse('{}/anna'.format(profs.name), min_depth=2)
    assert await extract('_key', result['vertices']) == []

    # Traverse the graph with DFS and BFS
    result = await school.traverse(
        {'_id': '{}/anna'.format(profs.name)},
        strategy='dfs',
        direction='any',
    )
    dfs_vertices = await extract('_key', result['vertices'])

    result = await school.traverse(
        {'_id': '{}/anna'.format(profs.name)},
        strategy='bfs',
        direction='any'
    )
    bfs_vertices = await extract('_key', result['vertices'])

    assert sorted(dfs_vertices) == sorted(bfs_vertices)

    # Traverse the graph with filter function
    result = await school.traverse(
        {'_id': '{}/andy'.format(profs.name)},
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert await extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']

    # Traverse the graph with global uniqueness (should be same as before)
    result = await school.traverse(
        {'_id': '{}/andy'.format(profs.name)},
        vertex_uniqueness='global',
        edge_uniqueness='global',
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert await extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']

    with assert_raises(DocumentParseError) as err:
        await school.traverse({})
    assert err.value.message == 'field "_id" required'
