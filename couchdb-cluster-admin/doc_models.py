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

    @property
    def db_name(self):
        return self._id

    def validate_allocation(self):
        pairs_from_by_node = {(node, shard)
                              for node, shards in self.by_node.items()
                              for shard in shards}
        pairs_from_by_range = {(node, shard)
                               for shard, nodes in self.by_range.items()
                               for node in nodes}

        return pairs_from_by_node == pairs_from_by_range

    def get_printable(self, include_shard_names=True):
        """
        Prints one row in a shard table

        :param include_shard_names: include header row consisting of the names of shards
        :return: a string to be printed out
        """
        parts = []
        from utils import strip_couchdb
        if not self.validate_allocation():
            parts.append(self.db_name)
            parts.append(u"In this allocation by_node and by_range are inconsistent:", repr(self))
        else:
            if include_shard_names:
                parts.append(u'\t')
                for shard in sorted(self.by_range):
                    parts.append(u'{}\t'.format(shard))
                parts.append(u'\n')
            parts.append(u'{}\t'.format(self.db_name))
            for shard, nodes in sorted(self.by_range.items()):
                parts.append(u'{}\t'.format(u','.join(map(strip_couchdb, nodes))))
        return ''.join(parts)
