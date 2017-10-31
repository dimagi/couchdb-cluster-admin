from utils import (
    check_connection,
    get_arg_parser,
    get_db_list,
    get_membership,
    get_shard_allocation,
    indent,
    node_details_from_args,
)


def strip_couchdb(node):
    if node.startswith('couchdb@'):
        return node[len('couchdb@'):]


if __name__ == '__main__':
    parser = get_arg_parser(u'Describe a couchdb cluster')
    args = parser.parse_args()

    node_details = node_details_from_args(args)
    check_connection(node_details)

    print u'Membership'
    print indent(get_membership(node_details).get_printable())

    print u'Shards'
    last_header = None
    for db_name in get_db_list(node_details):
        allocation = get_shard_allocation(node_details, db_name)
        if not allocation.validate_allocation():
            print db_name
            print u"In this allocation by_node and by_range are inconsistent:", repr(allocation)
        else:
            this_header = sorted(allocation.by_range)
            if this_header != last_header:
                print '\t',
                for shard in this_header:
                    print u'{}\t'.format(shard),
                last_header = this_header
                print
            print '{}\t'.format(db_name),
            for shard, nodes in sorted(allocation.by_range.items()):
                print u'{}\t'.format(u','.join(map(strip_couchdb, nodes))),
            print
