import argparse
from collections import defaultdict
import json
from utils import get_arg_parser, get_config_from_args, get_shard_allocation, set_up_parser
from doc_models import ShardAllocationDoc
from describe import print_shard_table


def read_plan_file(filename):
    with open(filename) as f:
        return json.load(f)


def update_shard_allocation_docs_from_plan(shard_allocation_docs, plan):
    shard_allocation_docs = {shard_allocation_doc.db_name: shard_allocation_doc
                             for shard_allocation_doc in shard_allocation_docs}
    for db_name, by_range in sorted(plan.items()):
        shard_allocation_doc = shard_allocation_docs[db_name]
        by_node = defaultdict(list)
        for shard, nodes in by_range.items():
            for node in nodes:
                by_node[node].append(shard)
        shard_allocation_doc.by_range = by_range
        shard_allocation_doc.by_node = by_node
        assert shard_allocation_doc.validate_allocation()
    return shard_allocation_docs


def figure_out_what_you_can_delete(plan):
    all_files = set()
    important_files_by_node = defaultdict(set)
    for db_name, by_range in plan.items():
        for shard, nodes in by_range.items():
            for node in nodes:
                couch_file = 'shards/{shard}/{db_name}.*.couch'.format(
                    shard=shard, db_name=db_name)
                view_file = '.shards/{shard}/{db_name}.*_design'.format(
                    shard=shard, db_name=db_name)
                important_files_by_node[node].update([couch_file, view_file])
                all_files.update([couch_file, view_file])

    deletable_files_by_node = {}
    for node, important_files in important_files_by_node.items():
        deletable_files_by_node[node] = all_files - important_files

    return deletable_files_by_node


def show_plan(config, plan):
    shard_allocation_docs = [get_shard_allocation(config, db_name) for db_name in plan]
    update_shard_allocation_docs_from_plan(shard_allocation_docs, plan)
    print_shard_table(shard_allocation_docs)


def run_plan_prune(plan, node):
    deletable_files_by_node = figure_out_what_you_can_delete(plan)

    for filename in sorted(deletable_files_by_node[node]):
        print filename


def main():
    parser = argparse.ArgumentParser(description=u'Helper for various manual database file operations')
    subparsers = parser.add_subparsers(dest='command')
    subparser_list = [subparsers.add_parser(
        'prune',
        help=u"List files that can be safely removed. "
             u"(May list files that do not exist on the machine.)"
    ), subparsers.add_parser(
        'show-plan',
        help=u"Just print the shard allocation table"
    )]
    for subparser in subparser_list:
        set_up_parser(subparser)
        subparser.add_argument(
            '--node', dest='node', required=True,
            help=u'Which node to make suggestions for.')
        subparser.add_argument(
            '--from-plan', dest='plan_file', required=True,
            help=u'Get target shard allocation from plan file.')

    args = parser.parse_args()
    config = get_config_from_args(args)
    plan = read_plan_file(args.plan_file)

    if args.command == 'show-plan':
        show_plan(config, plan)

    if args.command == 'prune':
        run_plan_prune(plan, config.get_formal_node_name(args.node))


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    main()
