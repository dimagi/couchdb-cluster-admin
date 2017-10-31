# couchdb-cluster-admin
utility for managing multi-node couchdb 2.x clusters

# First, put together a config file for your setup

This will make the rest of the commcands simpler to run. Copy the example

```
cp config/conf.example.yml config/mycluster.yml
```

and then edit it with the details of your cluster.

# Get a quick overview of your cluster

Now you can run

```
python couchdb-cluster-admin/describe.py --conf config/mycluster.yml
```

to see an overview of your cluster nodes and shard allocation.
For example, in the following output:

```
Membership
	cluster_nodes:	couch3	couch1	couch4	couch2
	all_nodes:	couch3	couch1	couch4	couch2
Shards
	                   00000000-1fffffff  20000000-3fffffff  40000000-5fffffff  60000000-7fffffff  80000000-9fffffff  a0000000-bfffffff  c0000000-dfffffff  e0000000-ffffffff
	mydb                    couch1             couch1             couch1             couch1             couch1             couch1             couch1             couch1
	my_second_database      couch1             couch1             couch1             couch1             couch1             couch1             couch1             couch1
```

you can see that while there are four nodes,
all shards are currently assigned only to the first node.

# Help estimating shard allocation

Right now this only works if you have a single large instance that you are trying to shard
into multiple. In order for this command to work, you must have ssh access
to the current large machine, and read access to the couch data-dir.


```bash
bash couchdb-cluster-admin/size-shards.sh <remote-ip> <remote-data-dir> | python couchdb-cluster-admin/suggest_shard_allocation.py <n-nodes> <n-copies>
```
