from collections import namedtuple, defaultdict
import sys

_NodeAllocation = namedtuple('_NodeAllocation', 'i size shards')


def suggest_shard_allocation(shard_sizes, n_nodes, n_copies):
    shard_sizes = sorted(shard_sizes)
    # size is a list here to simulate a mutable int
    nodes = [_NodeAllocation(i, [0], []) for i in range(n_nodes)]
    for size, shard in shard_sizes:
        selected_nodes = sorted(nodes, key=lambda node: node.size[0])[:n_copies]
        for node in selected_nodes:
            node.shards.append(shard)
            node.size[0] += size
    return nodes


def humansize(nbytes):
    """
    Copied from https://stackoverflow.com/questions/14996453/python-libraries-to-calculate-human-readable-filesize-from-bytes#14996816
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


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


if __name__ == '__main__':
    main()
