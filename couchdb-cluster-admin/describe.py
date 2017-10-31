from utils import (
    check_connection,
    get_arg_parser,
    get_db_list,
    get_membership,
    get_shard_allocation,
    indent,
    node_details_from_args,
)


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
        this_header = sorted(allocation.by_range)
        print indent(allocation.get_printable(include_shard_names=(last_header != this_header)))
        last_header = this_header
