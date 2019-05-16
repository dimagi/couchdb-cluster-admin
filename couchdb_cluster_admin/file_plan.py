import argparse
import itertools
from collections import defaultdict, namedtuple
import json

from utils import get_config_from_args, get_shard_allocation, set_up_parser
from describe import print_shard_table
from doc_models import ShardAllocationDoc


Nodefile = namedtuple('Nodefile', 'db_name, node, shard, filename')


def read_plan_file(filename):
    with open(filename) as f:
        plan = json.load(f)

    return {
        db_name: ShardAllocationDoc.from_plan_json(db_name, plan_json)
        for db_name, plan_json in plan.items()
    }

def update_shard_allocation_docs_from_plan(cluster_allocation_doc, plan):
    cluster_allocation_doc = {shard_allocation_doc.db_name: shard_allocation_doc
                              for shard_allocation_doc in cluster_allocation_doc}
    for db_name, plan_allocation_doc in sorted(plan.items()):
        shard_allocation_doc = cluster_allocation_doc[db_name]
        shard_allocation_doc.by_range = plan_allocation_doc.by_range
        shard_allocation_doc.by_node = plan_allocation_doc.by_node
        assert shard_allocation_doc.validate_allocation()
    return cluster_allocation_doc


def figure_out_what_you_can_and_cannot_delete(plan, shard_suffix_by_db_name=None):
    """
    :param plan:
    :param shard_suffix_by_db_name:
    :return: tuple(important_files_by_node, deletable_files_by_node)
                important_files_by_node is a dict of node->[Nodefile, ...]
                deletable_files_by_node is a dict of node->[filename, ...]
    """
    if not shard_suffix_by_db_name:
        shard_suffix_by_db_name = defaultdict(lambda: '.*')

    all_filenames = set()
    important_files_by_node = defaultdict(set)
    for db_name, plan_allocation_doc in plan.items():
        shard_suffix = shard_suffix_by_db_name.get(db_name, None)
        for shard, nodes in plan_allocation_doc.by_range.items():
            for node in nodes:
                couch_file_name = 'shards/{shard}/{db_name}{shard_suffix}.couch'.format(
                    shard=shard, db_name=db_name, shard_suffix=shard_suffix)
                view_file_name = '.shards/{shard}/{db_name}{shard_suffix}_design'.format(
                    shard=shard, db_name=db_name, shard_suffix=shard_suffix)

                couch_file = Nodefile(db_name, node, shard, couch_file_name)
                important_files_by_node[node].add(couch_file)
                all_filenames.add(couch_file_name)

                view_file = Nodefile(db_name, node, shard, view_file_name)
                # _global_changes doesn't have any views, so there's no view file
                # The same is true of _any_ db with no views, but it's rare enough
                # that I'm just cutting corners for simplicity
                if db_name != '_global_changes':
                    important_files_by_node[node].add(view_file)
                    all_filenames.add(view_file_name)

    deletable_files_by_node = {}
    for node, important_files in important_files_by_node.items():
        important_filenames = {file.filename for file in important_files}
        deletable_files_by_node[node] = all_filenames - important_filenames

    return important_files_by_node, deletable_files_by_node


def assemble_shard_allocations_from_plan(config, plan):
    shard_allocation_docs = [get_shard_allocation(config, db_name) for db_name in plan]
    update_shard_allocation_docs_from_plan(shard_allocation_docs, plan)
    return shard_allocation_docs


def show_plan(config, plan):
    plan_allocation_docs = plan.values()
    for doc in plan_allocation_docs:
        doc.set_config(config)
    print_shard_table(plan_allocation_docs)


def _get_shard_suffixes(config, plan):
    shard_suffix_by_db_name = {}
    for db_name, plan_allocation_doc in plan.items():
        cluster_allocation_doc = get_shard_allocation(config, db_name)

        if plan_allocation_doc.shard_suffix:
            assert cluster_allocation_doc.shard_suffix == plan_allocation_doc.shard_suffix

        shard_suffix_by_db_name[db_name] = cluster_allocation_doc.usable_shard_suffix

    return shard_suffix_by_db_name


def get_missing_files_by_node_and_source(config, plan):
    """
    :return: Lists of ``Nodefile`` tuples representing files that are missing from the node
             grouped by target node and source node:
             {
                 'target1': {
                     'source1': [Nodefile(...), Nodefile(...)]
                 }
             }
    """
    missing_files = defaultdict(lambda: defaultdict(list))
    important_files_by_node, _ = get_node_files(config, plan)
    important_files = itertools.chain(*important_files_by_node.values())
    for db_name, db_files in itertools.groupby(important_files, key=lambda f: f.db_name):
        cluster_allocation_doc = get_shard_allocation(config, db_name)
        for file in db_files:
            if file.shard not in cluster_allocation_doc.by_node.get(file.node, {}):
                source = cluster_allocation_doc.by_range[file.shard][0]
                missing_files[file.node][source].append(file)

    return missing_files


def run_plan_prune(config, plan, node):
    _, deletable_files_by_node = get_node_files(config, plan)
    for file in sorted(deletable_files_by_node[node], key=lambda f: f.filename):
        print file.filename


def run_important_plan(config, plan, node):
    important_files_by_node, _ = get_node_files(config, plan)
    for file in sorted(important_files_by_node[node], key=lambda f: f.filename):
        print file.filename


def get_node_files(config, plan):
    return figure_out_what_you_can_and_cannot_delete(plan, _get_shard_suffixes(config, plan))


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
    ), subparsers.add_parser(
        'important',
        help=u"List files that must be present and up to date on a node "
             u"before it is safe to commit the plan. "
             u"(May list files that already exist on the node.)"
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
        run_plan_prune(config, plan, config.get_formal_node_name(args.node))

    if args.command == 'important':
        run_important_plan(config, plan, config.get_formal_node_name(args.node))


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    main()
