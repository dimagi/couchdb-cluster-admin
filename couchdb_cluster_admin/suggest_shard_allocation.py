import argparse
from collections import defaultdict
import json

import requests
from memoized import memoized_property

from utils import humansize, get_arg_parser, get_config_from_args, check_connection, \
    get_db_list, get_db_metadata, get_shard_allocation, do_couch_request, put_shard_allocation
from describe import print_shard_table
from file_plan import read_plan_file
from doc_models import ShardAllocationDoc, AllocationSpec


class _NodeAllocation(object):
    def __init__(self, i, size, shards):
        self.i = i
        self.size = size
        self.shards = shards

    def as_tuple(self):
        return self.i, self.size, self.shards

    def __eq__(self, other):
        return self.as_tuple() == other.as_tuple()

    def __repr__(self):
        return '_NodeAllocation({self.i!r}, {self.size!r}, {self.shards!r})'.format(self=self)


def suggest_shard_allocation(shard_sizes, n_nodes, n_copies, existing_allocation=None):
    return Allocator(shard_sizes, n_nodes, n_copies, existing_allocation).suggest_shard_allocation()


class Allocator(object):
    def __init__(self, shard_sizes, n_nodes, n_copies, existing_allocation=None):
        self.shard_sizes = shard_sizes
        self.n_nodes = n_nodes
        self.n_copies = n_copies
        self.existing_allocation = existing_allocation or ([set()] * self.n_nodes)
        self.nodes = [_NodeAllocation(i, 0, []) for i in range(self.n_nodes)]
        self._average_size = sum([size for size, _ in shard_sizes]) * n_copies * 1.0 / n_nodes
        self._copies_still_in_original_location_by_shard = defaultdict(int)
        for shards in self.existing_allocation:
            for shard in shards:
                self._copies_still_in_original_location_by_shard[shard] += 1

    def suggest_shard_allocation(self):
        # First distribute, preferring shards' current locations
        for shard in self._get_shard_sizes_largest_to_smallest():
            for node in self._select_shard_locations(shard):
                self._add_shard_to_node(node, shard)

        # Then rebalance
        self._rebalance_nodes()
        return self.nodes

    def _get_shard_sizes_largest_to_smallest(self):
        return [shard for _, shard in reversed(sorted(self.shard_sizes))]

    def _select_shard_locations(self, shard):
        """
        Selects best location for n_copies of a given shard, based the allocation so far
        preferring a shard's existing locations

        returns a list of nodes (_NodeAllocation) that has length n_copies
        """
        return sorted(
            self.nodes,
            key=lambda node: (shard not in self.existing_allocation[node.i], node.size)
        )[:self.n_copies]

    @memoized_property
    def _sizes_by_shard(self):
        return {shard: size for size, shard in self.shard_sizes}

    def _add_shard_to_node(self, node, shard):
        node.shards.append(shard)
        node.size += self._sizes_by_shard[shard]

    def _rebalance_nodes(self):
        larger_nodes, smaller_nodes = self._split_nodes_by_under_allocated()
        if not smaller_nodes:
            return

        while True:
            # Move copies from larger_nodes to smaller_nodes
            # until doing so would make a larger node smaller than average_size
            # Never move more than half - 1 copies of a shard from their original location
            # (as given by existing_allocation)---these are the shard's "pivot locations"
            larger_nodes.sort(key=lambda node: node.size, reverse=True)
            smallest_node = min(smaller_nodes, key=lambda node: node.size)
            if smallest_node.size >= self._average_size:
                break
            try:
                large_node, shard = self._find_shard_to_move(larger_nodes, smallest_node)
            except self.NoEligibleMove:
                break
            else:
                self._move_shard(shard, large_node, smallest_node)

    def _move_shard(self, shard, node1, node2):
        if self._is_original_location(node1, shard):
            self._copies_still_in_original_location_by_shard[shard] -= 1
        node1.shards.remove(shard)
        node1.size -= self._sizes_by_shard[shard]
        self._add_shard_to_node(node2, shard)

    def _split_nodes_by_under_allocated(self):
        """
        Split nodes into okay nodes and under-allocated nodes

        Any node whose size is less than half the size of the largest node
        is deemed under-allocated

        :return: (okay_nodes, under_allocated_nodes)
        """
        threshold = max(self.nodes, key=lambda node: node.size).size / 2
        return (
            [node for node in self.nodes if node.size >= threshold],
            [node for node in self.nodes if node.size < threshold]
        )

    class NoEligibleMove(Exception):
        pass

    def _find_shard_to_move(self, larger_nodes, smallest_node):
        for large_node in larger_nodes:
            for shard in large_node.shards:
                if shard in smallest_node.shards:
                    # don't move a shard if a copy of it is already on the target node
                    continue
                if large_node.size - self._sizes_by_shard[shard] < self._average_size:
                    # don't move a shard if it would make the source node smaller than average
                    continue
                if self._is_original_location(large_node, shard) \
                        and not self._can_still_move_original_copies(shard):
                    # don't move a shard if that shard has already had
                    # the max number of its copies moved
                    # this is to make sure we have n/2+1 pivot locations for a shard
                    continue
                return large_node, shard
        raise self.NoEligibleMove()

    def _is_original_location(self, node, shard):
        return shard in self.existing_allocation[node.i]

    def _can_still_move_original_copies(self, shard):
        # unmoved original shards is larger than half of n_copies
        return self._copies_still_in_original_location_by_shard[shard] > (self.n_copies / 2 + 1)


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
        gevent.joinall(subprocesses, raise_error=True)

    processes.extend([gevent.spawn(_gather_view_sizes, db_name) for db_name in db_names])
    processes.extend([gevent.spawn(_gather_db_size, db_name) for db_name in db_names])
    processes.extend([gevent.spawn(_gather_db_shard_names, db_name) for db_name in db_names])

    gevent.joinall(processes, raise_error=True)

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


def get_shard_sizes(db_info, databases):
    return [
        (1.0 * sum([size] + views_size.values()) / len(shards), (shard_name, db_name))
        for db_name, size, views_size, shards, _ in db_info
        for shard_name in shards if db_name in databases
    ]


def normalize_allocation_specs(db_info, allocation_specs):
    """
    Modify allocation_specs in place to explicitly fill in database

    An allocation spec without explicit databases is assigned all databases
    not mentioned elsewhere.
    """
    db_names = {db_name for db_name, _, _, _, _ in db_info}
    mentioned_dbs = {db_name for allocation in allocation_specs
                     for db_name in (allocation.databases if allocation.databases else [])}
    unmentioned_dbs = list(db_names - mentioned_dbs)

    for allocation in allocation_specs:
        if allocation.databases is None:
            allocation.databases = list(unmentioned_dbs)


def get_existing_shard_allocation(db_info, databases, nodes):
    return [
        {
            (shard_name, db_name)
            for db_name, _, _, _, shard_allocation_doc in db_info if db_name in databases
            for shard_name in shard_allocation_doc.by_node.get(node, [])
        }
        for node in nodes
    ]


def make_suggested_allocation_by_db(config, db_info, allocation_specs):
    suggested_allocation_by_db = defaultdict(list)
    normalize_allocation_specs(db_info, allocation_specs)

    for allocation in allocation_specs:
        existing_allocation = get_existing_shard_allocation(db_info, allocation.databases, allocation.nodes)
        suggested_shard_allocation = suggest_shard_allocation(
            get_shard_sizes(db_info, allocation.databases), len(allocation.nodes), allocation.copies,
            existing_allocation=existing_allocation
        )
        for node_allocation in suggested_shard_allocation:
            print "{}\t{}".format(config.format_node_name(allocation.nodes[node_allocation.i]), humansize(node_allocation.size))
            for shard_name, db_name in node_allocation.shards:
                suggested_allocation_by_db[db_name].append((allocation.nodes[node_allocation.i], shard_name))

    shard_allocations_docs = {
        db_name: shard_allocation_doc
        for db_name, _, _, _, shard_allocation_doc in db_info
    }

    suggested_allocation_docs_by_db = {}
    for db_name, allocation in suggested_allocation_by_db.items():
        by_range = defaultdict(list)
        for node, shard in allocation:
            by_range[shard].append(node)

        doc = ShardAllocationDoc(_id=db_name, shard_suffix=shard_allocations_docs[db_name].shard_suffix)
        doc.populate_from_range(by_range)
        suggested_allocation_docs_by_db[db_name] = doc
    return suggested_allocation_docs_by_db


def apply_suggested_allocation(shard_allocations, plan):

    for shard_allocation_doc in shard_allocations:
        # have both a set for set operations and a list to preserve order
        # preserving order is useful for presenting things back to the user
        # based on the order they gave them
        db_name = shard_allocation_doc.db_name
        suggested_allocation = plan[db_name]
        assert suggested_allocation.validate_allocation()
        suggested_allocation_set = {(node, shard)
                                    for shard, nodes in suggested_allocation.by_range.items()
                                    for node in nodes}
        current_allocation_set = {(node, shard)
                                  for shard, nodes in shard_allocation_doc.by_range.items()
                                  for node in nodes}
        shard_allocation_doc.by_range = suggested_allocation.by_range
        shard_allocation_doc.by_node = suggested_allocation.by_node
        shard_allocation_doc.changelog.extend([
            ["add", shard, node]
            for node, shard in suggested_allocation_set - current_allocation_set
        ])
        shard_allocation_doc.changelog.extend([
            ["delete", shard, node]
            for node, shard in current_allocation_set - suggested_allocation_set
        ])
        if shard_allocation_doc.shard_suffix:
            assert shard_allocation_doc.shard_suffix == suggested_allocation.shard_suffix
        else:
            shard_allocation_doc.shard_suffix = suggested_allocation.shard_suffix

        assert shard_allocation_doc.validate_allocation()
    return shard_allocations


def main():
    parser = get_arg_parser(u'Suggest shard allocation for a cluster')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--allocate', dest='allocation', nargs="+",
                       help='List of nodes and how many copies you want on them, '
                            'like node1,node2,node3:<ncopies>[:db1,db2] [...]')

    group.add_argument('--from-plan', dest='plan_file',
                       help=u'Get target shard allocation from plan file.')

    parser.add_argument('--save-plan', dest='save_to_plan_file', required=False,
                        help='Save this plan to a file for use later.')

    parser.add_argument('--commit-to-couchdb', dest='commit', action='store_true', required=False,
                        help='Save the suggested allocation directly to couchdb, '
                             'changing the live shard allocation.')

    parser.add_argument('--create-missing-databases', dest='create', action='store_true', required=False,
                        help="Create databases in the cluster if they don't exist.")

    args = parser.parse_args()
    config = get_config_from_args(args)

    node_details = config.get_control_node()
    check_connection(node_details)

    if args.save_to_plan_file and args.plan_file:
        # this probably isn't the intended use of this exception
        # but makes it clear enough to the caller at this point.
        raise argparse.ArgumentError(None, "You cannot use --save-plan with --from-plan.")

    if args.allocation:
        shard_allocations = generate_shard_allocation(config, args.allocation)
    else:
        plan = read_plan_file(args.plan_file)
        create = args.create
        shard_allocations = get_shard_allocation_from_plan(config, plan, create)

    print_shard_table([shard_allocation_doc for shard_allocation_doc in shard_allocations])

    if args.save_to_plan_file:
        with open(args.save_to_plan_file, 'w') as f:
            json.dump({shard_allocation_doc.db_name: shard_allocation_doc.to_plan_json()
                       for shard_allocation_doc in shard_allocations}, f)

    if args.commit:
        for shard_allocation_doc in shard_allocations:
            db_name = shard_allocation_doc.db_name
            try:
                print put_shard_allocation(config, shard_allocation_doc)
            except requests.exceptions.HTTPError as e:
                if db_name.startswith('_') and e.response.json().get('error') == 'illegal_docid':
                    print("Skipping {} (error response was {})".format(db_name, e.response.json()))
                else:
                    raise


def get_shard_allocation_from_plan(config, plan, create=False):
    shard_allocations_docs = [get_shard_allocation(config, db_name, create) for db_name in plan]
    shard_allocations = apply_suggested_allocation(
        shard_allocations_docs, plan
    )
    return shard_allocations


def parse_allocation_line(config, allocation_line):
    try:
        nodes, copies, databases = allocation_line.split(':')
    except ValueError:
        nodes, copies = allocation_line.split(':')
        databases = None

    nodes = [config.get_formal_node_name(node) for node in nodes.split(',')]
    copies = int(copies)
    if databases:
        databases = databases.split(',')
    return AllocationSpec(
        nodes=nodes,
        copies=copies,
        databases=databases,
    )


def generate_shard_allocation(config, allocation):
    allocation = [
        parse_allocation_line(config, allocation_line) for allocation_line in allocation
    ]
    db_info = get_db_info(config)
    shard_allocations_docs = [shard_allocation_doc
                              for _, _, _, _, shard_allocation_doc in db_info]
    shard_allocations = apply_suggested_allocation(
        shard_allocations_docs,
        make_suggested_allocation_by_db(config, db_info, allocation)
    )
    return shard_allocations


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    main()
