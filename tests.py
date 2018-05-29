from mock.mock import patch

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
