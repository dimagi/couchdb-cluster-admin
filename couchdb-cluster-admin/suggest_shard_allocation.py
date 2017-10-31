from collections import namedtuple, defaultdict
import sys
from utils import humansize, get_arg_parser, get_config_from_args, check_connection, \
    get_db_list, get_db_metadata, get_shard_allocation

_NodeAllocation = namedtuple('_NodeAllocation', 'i size shards')


def suggest_shard_allocation(shard_sizes, n_nodes, n_copies):
    shard_sizes = reversed(sorted(shard_sizes))
    # size is a list here to simulate a mutable int
    nodes = [_NodeAllocation(i, [0], []) for i in range(n_nodes)]
    for size, shard in shard_sizes:
        selected_nodes = sorted(nodes, key=lambda node: node.size[0])[:n_copies]
        for node in selected_nodes:
            node.shards.append(shard)
            node.size[0] += size
    return nodes


def main(keep_shards_whole=True):
    """
    < shard-sizes.txt python couchdb-cluster-admin/suggest_shard_allocation.py 3 2
    """
    shards_sizes = []

    if not keep_shards_whole:
        for line in sys.stdin.readlines():
            size, shard = line.split()
            shards_sizes.append((int(size), shard))
    else:
        whole_shard_sizes = defaultdict(int)
        for line in sys.stdin.readlines():
            size, shard = line.split()
            whole_shard_sizes[shard.split('/')[0]] += int(size)
        for whole_shard, size in whole_shard_sizes.items():
            shards_sizes.append((size, whole_shard))

    for node in suggest_shard_allocation(shards_sizes, int(sys.argv[1]), int(sys.argv[2])):
        print "Node #{}".format(node.i + 1)
        print humansize(node.size[0])
        for shard in node.shards:
            print "  {}".format(shard)


def get_db_size(node_details, db_name):
    return get_db_metadata(node_details, db_name)['disk_size']


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    import gevent
    parser = get_arg_parser(u'Suggest shard allocation for a cluster')
    parser.add_argument('--allocate', dest='allocation', nargs="+", required=True,
                        help='List of nodes and how many copies you want on them, '
                             'like node1,node2,node3:<ncopies> [...]')
    args = parser.parse_args()
    allocation = [
        (nodes.split(','), int(copies))
        for nodes, copies in (group.split(':') for group in args.allocation)
    ]
    config = get_config_from_args(args)
    node_details = config.get_control_node()
    check_connection(node_details)
    db_names = get_db_list(node_details)

    def get_db_info():
        db_sizes = {}
        db_shards = {}
        shard_allocation_docs = {}

        def _gather_db_size(db_name):
            db_sizes[db_name] = get_db_size(node_details, db_name)

        def _gather_db_shard_names(db_name):
            doc = get_shard_allocation(node_details, db_name)
            shard_allocation_docs[db_name] = doc
            db_shards[db_name] = sorted(doc.by_range)

        gevent.joinall([gevent.spawn(_gather_db_size, db_name) for db_name in db_names] +
                       [gevent.spawn(_gather_db_shard_names, db_name) for db_name in db_names])

        return [(db_name, db_sizes[db_name], db_shards[db_name], shard_allocation_docs[db_name])
                for db_name in db_names]

    def print_db_info():
        for db_name, size, shards, _ in sorted(get_db_info()):
            print db_name, humansize(size), len(shards)

    def get_shard_sizes():
        return [
            (size/len(shards),
             "{shard}/{db}".format(shard=shard_name, db=db_name))
            for db_name, size, shards, _ in get_db_info()
            for shard_name in shards
        ]

    for nodes, copies in allocation:
        print u"Suggestion for {} with {} copies".format(','.join(nodes), copies)
        for node in suggest_shard_allocation(get_shard_sizes(), len(nodes), copies):
            print "Node #{}".format(node.i + 1)
            print humansize(node.size[0])
            for shard in node.shards:
                print "  {}".format(shard)
