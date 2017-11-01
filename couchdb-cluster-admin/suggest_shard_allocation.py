from collections import namedtuple, defaultdict
from utils import humansize, get_arg_parser, get_config_from_args, check_connection, \
    get_db_list, get_db_metadata, get_shard_allocation
from describe import print_shard_table

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


def get_db_size(node_details, db_name):
    return get_db_metadata(node_details, db_name)['disk_size']


def get_db_info(config):
    import gevent
    node_details = config.get_control_node()
    db_names = get_db_list(node_details)
    db_sizes = {}
    db_shards = {}
    shard_allocation_docs = {}

    def _gather_db_size(db_name):
        db_sizes[db_name] = get_db_size(node_details, db_name)

    def _gather_db_shard_names(db_name):
        doc = get_shard_allocation(config, db_name)
        shard_allocation_docs[db_name] = doc
        db_shards[db_name] = sorted(doc.by_range)

    gevent.joinall([gevent.spawn(_gather_db_size, db_name) for db_name in db_names] +
                   [gevent.spawn(_gather_db_shard_names, db_name) for db_name in db_names])

    return [(db_name, db_sizes[db_name], db_shards[db_name], shard_allocation_docs[db_name])
            for db_name in db_names]


def print_db_info(config):
    """
    Print table of <db name> <disk size (not including views)> <number of shards>
    :return:
    """
    for db_name, size, shards, _ in sorted(get_db_info(config)):
        print db_name, humansize(size), len(shards)


def get_shard_sizes(db_info):
    return [
        (size/len(shards), (shard_name, db_name))
        for db_name, size, shards, _ in db_info
        for shard_name in shards
    ]


def main():
    parser = get_arg_parser(u'Suggest shard allocation for a cluster')
    parser.add_argument('--allocate', dest='allocation', nargs="+", required=True,
                        help='List of nodes and how many copies you want on them, '
                             'like node1,node2,node3:<ncopies> [...]')
    args = parser.parse_args()
    config = get_config_from_args(args)
    formal_name_lookup = {nickname: formal_name
                          for formal_name, nickname in config.aliases.items()}
    allocation = [
        ([formal_name_lookup[node] for node in nodes.split(',')], int(copies))
        for nodes, copies in (group.split(':') for group in args.allocation)
    ]
    node_details = config.get_control_node()
    check_connection(node_details)

    db_info = get_db_info(config)

    suggested_allocation_by_db = defaultdict(list)
    for nodes, copies in allocation:
        for node_allocation in suggest_shard_allocation(get_shard_sizes(db_info), len(nodes), copies):
            print "{}\t{}".format(config.format_node_name(nodes[node_allocation.i]), humansize(node_allocation.size[0]))
            for shard_name, db_name in node_allocation.shards:
                suggested_allocation_by_db[db_name].append((nodes[node_allocation.i], shard_name))

    for db_name, _, _, shard_allocation_doc in db_info:
        suggested_allocation = set(suggested_allocation_by_db[db_name])
        current_allocation = {(node, shard)
                              for shard, nodes in shard_allocation_doc.by_range.items()
                              for node in nodes}
        by_range = defaultdict(list)
        by_node = defaultdict(list)
        for node, shard in suggested_allocation:
            by_range[shard].append(node)
            by_node[node].append(shard)
        shard_allocation_doc.by_range = dict(by_range)
        shard_allocation_doc.by_node = dict(by_node)
        shard_allocation_doc.changelog.extend([
            ["add", shard, node]
            for node, shard in suggested_allocation - current_allocation
        ])
        shard_allocation_doc.changelog.extend([
            ["delete", shard, node]
            for node, shard in current_allocation - suggested_allocation
        ])
        assert shard_allocation_doc.validate_allocation()

    print_shard_table([shard_allocation_doc for _, _, _, shard_allocation_doc in db_info])


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    main()
