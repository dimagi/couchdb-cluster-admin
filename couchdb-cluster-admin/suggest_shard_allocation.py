from collections import namedtuple, defaultdict
import json
from utils import humansize, get_arg_parser, get_config_from_args, check_connection, \
    get_db_list, get_db_metadata, get_shard_allocation, do_couch_request, put_shard_allocation
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


def get_view_signature_and_size(node_details, db_name, view_name):
    view_info = do_couch_request(
        node_details,
        '/{db_name}/_design/{view_name}/_info'.format(db_name=db_name, view_name=view_name)
    )
    return view_info['view_index']['signature'], view_info['view_index']['sizes']['file']


def get_views_list(node_details, db_name):
    view_response = do_couch_request(
        node_details,
        '/{db_name}/_all_docs?startkey="_design%2F"&endkey="_design0"'.format(db_name=db_name)
    )
    return [row['id'][len('_design/'):] for row in view_response['rows'] if row['id'].startswith('_design/')]


def get_db_info(config):
    import gevent
    processes = []
    node_details = config.get_control_node()
    db_names = get_db_list(node_details)
    db_sizes = {}
    db_shards = {}
    shard_allocation_docs = {}
    view_sizes = defaultdict(dict)

    def _gather_db_size(db_name):
        db_sizes[db_name] = get_db_size(node_details, db_name)

    def _gather_db_shard_names(db_name):
        doc = get_shard_allocation(config, db_name)
        shard_allocation_docs[db_name] = doc
        db_shards[db_name] = sorted(doc.by_range)

    def _gather_view_size(db_name, view_name):
        signature, size = get_view_signature_and_size(node_details, db_name, view_name)
        view_sizes[db_name][signature] = (view_name, size)

    def _gather_view_sizes(db_name):
        subprocesses = []
        for view_name in get_views_list(node_details, db_name):
            # _gather_view_size(db_name, view_name)
            subprocesses.append(gevent.spawn(_gather_view_size, db_name, view_name))
        gevent.joinall(subprocesses)

    processes.extend([gevent.spawn(_gather_view_sizes, db_name) for db_name in db_names])
    processes.extend([gevent.spawn(_gather_db_size, db_name) for db_name in db_names])
    processes.extend([gevent.spawn(_gather_db_shard_names, db_name) for db_name in db_names])

    gevent.joinall(processes)

    view_sizes = {db_name: {name: size for name, size in view_sizes[db_name].values()}
                  for db_name in db_names}
    return [(db_name, db_sizes[db_name], view_sizes[db_name], db_shards[db_name], shard_allocation_docs[db_name])
            for db_name in db_names]


def print_db_info(config):
    """
    Print table of <db name> <disk size (not including views)> <number of shards>
    :return:
    """
    info = sorted(get_db_info(config))
    row = u"{: <30}\t{: <20}\t{: <20}\t{: <20}"
    print row.format(u"Database", u"Data size on Disk", u"View size on Disk", u"Number of shards")
    for db_name, size, view_sizes, shards, _ in info:
        print row.format(
            db_name,
            humansize(size),
            humansize(sum([view_size for view_name, view_size in view_sizes.items()])),
            len(shards)
        )


def get_shard_sizes(db_info):
    return [
        (1.0 * sum([size] + views_size.values()) / len(shards), (shard_name, db_name))
        for db_name, size, views_size, shards, _ in db_info
        for shard_name in shards
    ]


def main():
    parser = get_arg_parser(u'Suggest shard allocation for a cluster')
    parser.add_argument('--allocate', dest='allocation', nargs="+", required=True,
                        help='List of nodes and how many copies you want on them, '
                             'like node1,node2,node3:<ncopies> [...]')

    parser.add_argument('--save-plan', dest='plan_file', required=False,
                        help='Save this plan to a file for use later.')

    parser.add_argument('--commit-to-couchdb', dest='commit', action='store_true', required=False,
                        help='Save the suggested allocation directly to couchdb, '
                             'changing the live shard allocation.')
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

    shard_allocations = [shard_allocation_doc for _, _, _, _, shard_allocation_doc in db_info]

    for shard_allocation_doc in shard_allocations:
        # have both a set for set operations and a list to preserve order
        # preserving order is useful for presenting things back to the user
        # based on the order they gave them
        db_name = shard_allocation_doc.db_name
        suggested_allocation = suggested_allocation_by_db[db_name]
        suggested_allocation_set = set(suggested_allocation_by_db[db_name])
        assert len(suggested_allocation) == len(suggested_allocation_set)
        current_allocation_set = {(node, shard)
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
            for node, shard in suggested_allocation_set - current_allocation_set
        ])
        shard_allocation_doc.changelog.extend([
            ["delete", shard, node]
            for node, shard in current_allocation_set - suggested_allocation_set
        ])
        assert shard_allocation_doc.validate_allocation()

    print_shard_table([shard_allocation_doc for shard_allocation_doc in shard_allocations])

    if args.plan_file:
        with open(args.plan_file, 'w') as f:
            json.dump({shard_allocation_doc.db_name: shard_allocation_doc.by_range
                       for shard_allocation_doc in shard_allocations}, f)

    if args.commit:
        for shard_allocation_doc in shard_allocations:
            print put_shard_allocation(config, shard_allocation_doc)



if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    main()
