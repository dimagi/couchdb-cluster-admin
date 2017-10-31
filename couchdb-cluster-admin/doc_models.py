from jsonobject import JsonObject, ListProperty, DictProperty, StringProperty


class MembershipDoc(JsonObject):
    _allow_dynamic_properties = False

    cluster_nodes = ListProperty(unicode, required=True)
    all_nodes = ListProperty(unicode, required=True)

    def get_printable(self):
        return (
            u"cluster_nodes:\t{all_nodes}\n"
            u"all_nodes:\t{cluster_nodes}"
        ).format(
            cluster_nodes=u'\t'.join(self.cluster_nodes),
            all_nodes=u'\t'.join(self.all_nodes),
        )


class ShardAllocationDoc(JsonObject):
    _allow_dynamic_properties = False

    _id = StringProperty()
    _rev = StringProperty()

    by_node = DictProperty(ListProperty(unicode), required=True)
    changelog = ListProperty(ListProperty(unicode), required=True)
    shard_suffix = ListProperty(int, required=True)
    by_range = DictProperty(ListProperty(unicode), required=True)

    @property
    def usable_shard_suffix(self):
        return ''.join(map(chr, self.shard_suffix))

    def validate_allocation(self):
        pairs_from_by_node = {(node, shard)
                              for node, shards in self.by_node.items()
                              for shard in shards}
        pairs_from_by_range = {(node, shard)
                               for shard, nodes in self.by_range.items()
                               for node in nodes}

        return pairs_from_by_node == pairs_from_by_range
