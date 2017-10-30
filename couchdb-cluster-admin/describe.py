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
    parser = get_arg_parser('Describe a couchdb cluster')
    args = parser.parse_args()

    node_details = node_details_from_args(args)
    check_connection(node_details)

    print 'Membership'
    print indent(get_membership(node_details).get_printable())

    print 'Shards'
    for db_name in get_db_list(node_details):
        print indent(db_name)
        print indent(repr(get_shard_allocation(node_details, db_name)), n=2)
