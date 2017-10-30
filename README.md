# couchdb-cluster-admin
utility for managing multi-node couchdb 2.x clusters


# Help estimating shard allocation

Right now this only works if you have a single large instance that you are trying to shard
into multiple. In order for this command to work, you must have ssh access
to the current large machine, and read access to the couch data-dir.


```bash
bash couchdb-cluster-admin/size-shards.sh <remote-ip> <remote-data-dir> | python couchdb-cluster-admin/suggest_shard_allocation.py <n-nodes> <n-copies>
```
