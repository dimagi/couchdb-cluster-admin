from mock.mock import patch

from couchdb_cluster_admin.suggest_shard_allocation import suggest_shard_allocation, _NodeAllocation
from couchdb_cluster_admin.doc_models import ShardAllocationDoc
from couchdb_cluster_admin.file_plan import get_missing_files_by_node_and_source, Nodefile


@patch('couchdb_cluster_admin.file_plan.get_node_files', return_value=({'node1': [
    Nodefile('db1', 'node2', 'shard3', 'f3'),
    Nodefile('db1', 'node3', 'shard1', 'f1'),
    Nodefile('db1', 'node3', 'shard2', 'f2'),
    Nodefile('db1', 'node3', 'shard4', 'f4'),
]}, None))
@patch('couchdb_cluster_admin.file_plan.get_shard_allocation', return_value=ShardAllocationDoc.from_plan_json(
    'db1', {
        'shard_suffix': '123132',
        'by_range': {
            'shard1': ['node1'],
            'shard2': ['node1'],
            'shard3': ['node2'],
            'shard4': ['node2'],
        }
    }
))
def test_get_missing_files(m1, m2):
    """
    from:
    node1: shard1, shard2
    node2: shard3, shard4

    to:
    node2: shard3
    node3: shard1, shard2, shard4
    """
    missing_files = get_missing_files_by_node_and_source(None, None)
    assert missing_files == {
        'node3': {
            'node1': [Nodefile('db1', 'node3', 'shard1', 'f1'), Nodefile('db1', 'node3', 'shard2', 'f2')],
            'node2': [Nodefile('db1', 'node3', 'shard4', 'f4')],
        }
    }


def test_suggest_shard_allocation():
    real = suggest_shard_allocation(
        shard_sizes=[
            (916925224199.5, (u'00000000-1fffffff', u'commcarehq')),
            (916925224199.5, (u'20000000-3fffffff', u'commcarehq')),
            (916925224199.5, (u'40000000-5fffffff', u'commcarehq')),
            (916925224199.5, (u'60000000-7fffffff', u'commcarehq')),
            (916925224199.5, (u'80000000-9fffffff', u'commcarehq')),
            (916925224199.5, (u'a0000000-bfffffff', u'commcarehq')),
            (916925224199.5, (u'c0000000-dfffffff', u'commcarehq')),
            (916925224199.5, (u'e0000000-ffffffff', u'commcarehq')),
        ],
        n_nodes=8,
        n_copies=3,
    )
    expected = [
        _NodeAllocation(i=0, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'a0000000-bfffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq')
                                ]),
        _NodeAllocation(i=1, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq')]),
        _NodeAllocation(i=2, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=3, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=4, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=5, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')]),
        _NodeAllocation(i=6, size=2750775672598.5,
                        shards=[(u'a0000000-bfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')]),
        _NodeAllocation(i=7, size=2750775672598.5,
                        shards=[(u'a0000000-bfffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')])
    ]
    assert real == expected


def test_suggest_shard_allocation__no_chnage():
    existing_allocation = [
        _NodeAllocation(i=0, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'a0000000-bfffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq')
                                ]),
        _NodeAllocation(i=1, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq')]),
        _NodeAllocation(i=2, size=2750775672598.5,
                        shards=[(u'e0000000-ffffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=3, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'80000000-9fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=4, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'20000000-3fffffff', u'commcarehq')]),
        _NodeAllocation(i=5, size=2750775672598.5,
                        shards=[(u'c0000000-dfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')]),
        _NodeAllocation(i=6, size=2750775672598.5,
                        shards=[(u'a0000000-bfffffff', u'commcarehq'),
                                (u'60000000-7fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')]),
        _NodeAllocation(i=7, size=2750775672598.5,
                        shards=[(u'a0000000-bfffffff', u'commcarehq'),
                                (u'40000000-5fffffff', u'commcarehq'),
                                (u'00000000-1fffffff', u'commcarehq')])
    ]
    real = suggest_shard_allocation(
        shard_sizes=[
            (916925224199.5, (u'00000000-1fffffff', u'commcarehq')),
            (916925224199.5, (u'20000000-3fffffff', u'commcarehq')),
            (916925224199.5, (u'40000000-5fffffff', u'commcarehq')),
            (916925224199.5, (u'60000000-7fffffff', u'commcarehq')),
            (916925224199.5, (u'80000000-9fffffff', u'commcarehq')),
            (916925224199.5, (u'a0000000-bfffffff', u'commcarehq')),
            (916925224199.5, (u'c0000000-dfffffff', u'commcarehq')),
            (916925224199.5, (u'e0000000-ffffffff', u'commcarehq')),
        ],
        n_nodes=8,
        n_copies=3,
        existing_allocation=[node.shards for node in existing_allocation],
    )
    # assert no change
    assert real == existing_allocation


def test_suggest_shard_allocation__increase_copies():
    existing_allocation = [
        {(u'e0000000-ffffffff', u'commcarehq')},
        {(u'c0000000-dfffffff', u'commcarehq')},
        {(u'a0000000-bfffffff', u'commcarehq')},
        {(u'80000000-9fffffff', u'commcarehq')},
        {(u'60000000-7fffffff', u'commcarehq')},
        {(u'40000000-5fffffff', u'commcarehq')},
        {(u'20000000-3fffffff', u'commcarehq')},
        {(u'00000000-1fffffff', u'commcarehq')},
    ]
    new_allocation = suggest_shard_allocation(
        shard_sizes=[
            (916925224199.5, (u'00000000-1fffffff', u'commcarehq')),
            (916925224199.5, (u'20000000-3fffffff', u'commcarehq')),
            (916925224199.5, (u'40000000-5fffffff', u'commcarehq')),
            (916925224199.5, (u'60000000-7fffffff', u'commcarehq')),
            (916925224199.5, (u'80000000-9fffffff', u'commcarehq')),
            (916925224199.5, (u'a0000000-bfffffff', u'commcarehq')),
            (916925224199.5, (u'c0000000-dfffffff', u'commcarehq')),
            (916925224199.5, (u'e0000000-ffffffff', u'commcarehq')),
        ],
        n_nodes=8,
        n_copies=3,
        existing_allocation=existing_allocation,
    )

    for i, node in enumerate(new_allocation):
        # assert that the new allocation contains all the shards in the existing allocation
        # (not always true, but should be in this case)
        assert existing_allocation[i] & set(node.shards) == existing_allocation[i]
