from collections import defaultdict
from jsonobject import JsonObject, ListProperty, DictProperty, StringProperty, IntegerProperty


class ConfigInjectionMixin(object):
    @property
    def config(self):
        from utils import Config
        try:
            return self._config
        except AttributeError:
            return Config()

    def set_config(self, config):
        if config:
            self._config = config


class MembershipDoc(ConfigInjectionMixin, JsonObject):
    _allow_dynamic_properties = False

    cluster_nodes = ListProperty(unicode, required=True)
    all_nodes = ListProperty(unicode, required=True)

    def get_printable(self):
        return (
            u"cluster_nodes:\t{all_nodes}\n"
            u"all_nodes:\t{cluster_nodes}"
        ).format(
            cluster_nodes=u'\t'.join(map(self.config.format_node_name, self.cluster_nodes)),
            all_nodes=u'\t'.join(map(self.config.format_node_name, self.all_nodes)),
        )


class ShardAllocationDoc(ConfigInjectionMixin, JsonObject):
    _allow_dynamic_properties = False

    _id = StringProperty()
    _rev = StringProperty(exclude_if_none=True)

    by_node = DictProperty(ListProperty(unicode))
    changelog = ListProperty(ListProperty(unicode))
    shard_suffix = ListProperty(int)
    by_range = DictProperty(ListProperty(unicode))

    @property
    def usable_shard_suffix(self):
        return ''.join(map(chr, self.shard_suffix))

    @property
    def db_name(self):
        return self._id

    def to_plan_json(self):
        return {
            'by_range': self.by_range,
            'shard_suffix': self.usable_shard_suffix
        }

    @classmethod
    def from_plan_json(self, db_name, plan_json):
        shard_suffix = [ord(c) for c in plan_json['shard_suffix']]
        doc = ShardAllocationDoc(_id=db_name, shard_suffix=shard_suffix)
        doc.populate_from_range(plan_json['by_range'])
        return doc

    def populate_from_range(self, by_range):
        self.by_range = by_range
        self.by_node = self._populate_other(by_range)
        assert self.validate_allocation()

    def populate_from_nodes(self, by_node):
        self.by_node = by_node
        self.by_range = self._populate_other(by_node)
        self.validate_allocation()

    def _populate_other(self, by_item):
        by_other = defaultdict(list)
        for key, values in by_item.items():
            for value in values:
                by_other[value].append(key)
        return dict(by_other)

    def get_node_list(self):
        return list({node for node, shards in self.by_node.items()})

    def get_host_list(self):
        return [node.split('@')[1] for node in self.get_node_list()]

    def validate_allocation(self):
        pairs_from_by_node = {(node, shard)
                              for node, shards in self.by_node.items()
                              for shard in shards}
        pairs_from_by_range = {(node, shard)
                               for shard, nodes in self.by_range.items()
                               for node in nodes}

        return pairs_from_by_node == pairs_from_by_range

    def get_printable(self, include_shard_names=True, db_name_len=20):
        """
        Prints one row in a shard table

        :param include_shard_names: include header row consisting of the names of shards
        :return: a string to be printed out
        """
        parts = []
        if not self.validate_allocation():
            parts.append(self.db_name)
            parts.append(u"In this allocation by_node and by_range are inconsistent:", repr(self))
        else:
            first_column = u'{{: <{}}}  '.format(db_name_len)
            other_columns = u'{: ^20s}  '
            if include_shard_names:
                parts.append(first_column.format(u''))
                for shard in sorted(self.by_range):
                    parts.append(other_columns.format(shard))
                parts.append(u'\n')
            parts.append(first_column.format(self.db_name))
            for shard, nodes in sorted(self.by_range.items()):
                parts.append(other_columns.format(u','.join(map(self.config.format_node_name, nodes))))
        return ''.join(parts)


class AllocationSpec(JsonObject):
    databases = ListProperty(unicode)
    nodes = ListProperty(unicode)
    copies = IntegerProperty()
