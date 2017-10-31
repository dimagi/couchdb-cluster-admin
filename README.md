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

In order to plan out a shard reallocation, you can run the following command:

```bash
python couchdb-cluster-admin/suggest_shard_allocation.py --conf config/mycluster.yml --allocate couch1:1 couch2,couch3,couch4:2
```

The values for the `--allocate` arg in the example above should be interpreted as
"Put 1 copy on couch1, and put 2 copies spread across couch2, couch3, and couch4".

The output looks like this:

```
couch1	57.57 GB
couch2	42.15 GB
couch3	36.5 GB
couch4	36.5 GB
                     00000000-1fffffff     20000000-3fffffff     40000000-5fffffff     60000000-7fffffff     80000000-9fffffff     a0000000-bfffffff     c0000000-dfffffff     e0000000-ffffffff
mydb                couch1,couch2,couch4  couch1,couch2,couch3  couch1,couch3,couch4  couch1,couch2,couch4  couch1,couch2,couch3  couch1,couch3,couch4  couch1,couch2,couch4  couch1,couch2,couch3
my_second_database  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4  couch1,couch3,couch4
```

Note, the reallocation does not take into account the current location of shards,
so it is much more useful in the situation that you're moving from a single-node cluster
to a multi-node cluster than it is in the situation where you're adding one more node to a multi-node cluster.
In the example above, couch1 would be the single-node cluster and couch2, couch3, and couch4
form are the multi-node cluster–to-be. You can imagine that after implementing
the shard allocation suggested here, we might remove all shards from couch1 and remove it from the cluster.

Note also that there is no guarantee that the "same" shard of different databases will go to the same node;
each (db, shard)-pair is treated as an independent unit when making computing an even shard allocation.
In this example there are only a few dbs and shards; when shards * dbs is high,
this process can be quite good at evenly balancing your data across nodes.
