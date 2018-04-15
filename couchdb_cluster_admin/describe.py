from utils import (
    check_connection,
    get_arg_parser,
    get_config_from_args,
    get_db_list,
    get_membership,
    get_shard_allocation,
    indent,
)


def print_shard_table(shard_allocation_docs):
    last_header = None
    db_names = [shard_allocation_doc.db_name for shard_allocation_doc in shard_allocation_docs]
    max_db_name_len = max(map(len, db_names))
    for shard_allocation_doc in shard_allocation_docs:
        this_header = sorted(shard_allocation_doc.by_range)
        print shard_allocation_doc.get_printable(include_shard_names=(last_header != this_header), db_name_len=max_db_name_len)
        last_header = this_header


if __name__ == '__main__':
    parser = get_arg_parser(u'Describe a couchdb cluster')
    args = parser.parse_args()

    config = get_config_from_args(args)
    node_details = config.get_control_node()
    check_connection(node_details)

    print u'Membership'
    print indent(get_membership(config).get_printable())

    print u'Shards'
    print_shard_table([
        get_shard_allocation(config, db_name)
        for db_name in sorted(get_db_list(node_details))
    ])
