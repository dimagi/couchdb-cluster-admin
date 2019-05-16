import argparse
import getpass
from collections import namedtuple
import os
from jsonobject import JsonObject, StringProperty, IntegerProperty, DictProperty

import requests
import time
import yaml
from requests import HTTPError

from doc_models import MembershipDoc, ShardAllocationDoc

NodeDetails = namedtuple('NodeDetails', 'ip port node_local_port username password socks_port')


def do_couch_request(node_details, path, method='get', params=None, json=None):
    return _do_request(node_details, path, node_details.port, method=method, params=params, json=json)


def do_node_local_request(node_details, path, method='get', params=None, json=None):
    return _do_request(node_details, path, node_details.node_local_port, method=method, params=params, json=json)


def _do_request(node_details, path, port, method='get', params=None, json=None):
    proxies = {}
    if node_details.socks_port:
        proxy = 'socks5://localhost:%s' % port
        proxies['http'] = proxy
        proxies['https'] = proxy

    response = requests.request(
        method=method,
        url="http://{}:{}/{}".format(node_details.ip, port, path),
        auth=(node_details.username, node_details.password) if node_details.username else None,
        params=params,
        json=json,
        proxies=proxies
    )
    response.raise_for_status()
    return response.json()


def get_db_list(node_details):
    db_names = do_couch_request(node_details, '_all_dbs')
    return db_names


def get_db_metadata(node_details, db_name):
    return do_couch_request(node_details, db_name)


def get_membership(config):
    if isinstance(config, NodeDetails):
        node_details = config
        config = None
    else:
        node_details = config.get_control_node()
    membership_doc = MembershipDoc.wrap(do_couch_request(node_details, '_membership'))
    membership_doc.set_config(config)
    return membership_doc


def get_shard_allocation(config, db_name, create=False):
    if isinstance(config, NodeDetails):
        node_details = config
        config = None
    else:
        node_details = config.get_control_node()
    try:
        shard_allocation_doc = ShardAllocationDoc.wrap(do_node_local_request(node_details, '_dbs/{}'.format(db_name)))
    except HTTPError as e:
        if e.response.status_code == 404:
            if create:
                shard_allocation_doc = ShardAllocationDoc(_id=db_name)
            else:
                raise Exception('Database "{}" does not exist. Use "--create-missing-databases" flag if you want'
                                ' to have the database created when the plan is committed.'.format(db_name))
        else:
            raise

    shard_allocation_doc.set_config(config)
    return shard_allocation_doc


def put_shard_allocation(config, shard_allocation_doc):
    node_details = config.get_control_node()
    return do_node_local_request(
        node_details,
        '_dbs/{}'.format(shard_allocation_doc.db_name),
        method='PUT',
        json=shard_allocation_doc.to_json(),
    )


def confirm(msg):
    return raw_input(msg + "\n(y/n)") == 'y'


def get_arg_parser(command_description):
    parser = argparse.ArgumentParser(description=command_description)
    set_up_parser(parser)
    return parser


def set_up_parser(parser):
    parser.add_argument('--conf', dest='conf')
    parser.add_argument('--control-node-ip', dest='control_node_ip',
                        help='IP of an existing node in the cluster')
    parser.add_argument('--username', dest='username',
                        help='Admin username')
    parser.add_argument('--control-node-port', dest='control_node_port', default=15984, type=int,
                        help='Port of control node. Default: 15984')
    parser.add_argument('--control-node-local-port', dest='control_node_local_port', default=15986, type=int,
                        help='Port of control node for local operations. Default: 15986')


class Config(JsonObject):
    control_node_ip = StringProperty()
    control_node_port = IntegerProperty()
    control_node_local_port = IntegerProperty()
    username = StringProperty()
    aliases = DictProperty(unicode)

    def set_password(self, password):
        self._password = password

    def get_control_node(self):
        return NodeDetails(
            self.control_node_ip, self.control_node_port, self.control_node_local_port,
            self.username, self._password, None
        )

    def format_node_name(self, node):
        if node in self.aliases:
            return self.aliases[node]
        elif node.startswith('couchdb@'):
            return node[len('couchdb@'):]
        else:
            return node

    def get_formal_node_name(self, node_nickname):
        if not hasattr(self, '_formal_name_lookup'):
            self._formal_name_lookup = {
                nickname: formal_name
                for formal_name, nickname in self.aliases.items()
            }
        return self._formal_name_lookup[node_nickname]


def get_config_from_args(args):
    if args.conf:
        with open(args.conf) as f:
            config = Config.wrap(yaml.load(f))
    else:
        config = Config(
            control_node_ip=args.control_node_ip,
            control_node_port=args.control_node_port,
            control_node_local_port=args.control_node_local_port,
            username=args.username,
            aliases=None,
        )

    if 'COUCHDB_CLUSTER_ADMIN_PASSWORD' in os.environ:
        password = os.environ['COUCHDB_CLUSTER_ADMIN_PASSWORD']
    elif config.username:
        password = getpass.getpass('Password for "{}@{}":'.format(config.username, config.control_node_ip))
    else:
        password = None
    config.set_password(password)
    return config


def node_details_from_args(args):
    return get_config_from_args(args).get_control_node()


def add_node_to_cluster(node_details, new_node):
    do_node_local_request(node_details, '_nodes/{}'.format(new_node), json={}, method='put')
    success = False
    for attempt in range(0, 3):
        success = is_node_in_cluster(node_details, new_node)
        if success:
            break
        time.sleep(1)  # wait for state to be propagated

    if not success:
        raise Exception('Node could not be added to cluster')


def remove_node_from_cluster(node_details, node_to_remove):
    node_url_path = '_nodes/{}'.format(node_to_remove)
    node_doc = do_node_local_request(node_details, node_url_path)
    do_node_local_request(node_details, node_url_path, method='delete', params={
        'rev': node_doc['_rev']
    })


def check_connection(node_details):
    do_couch_request(node_details, '')


def is_node_in_cluster(node_details, node_to_check):
    membership = get_membership(node_details)
    return node_to_check in membership.cluster_nodes


def indent(text, n=1):
    padding = n * u'\t'
    return u''.join(padding + line for line in text.splitlines(True))


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
