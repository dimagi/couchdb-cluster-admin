#!/usr/bin/env bats

function setup {
    db_name=test$(head -n20 /dev/random | python -c 'import hashlib, sys; print(hashlib.md5(sys.stdin.read()).hexdigest())')
    echo "PUT" $db_name
    curl -sX PUT http://localhost:15984/${db_name}
    curl -sX PUT http://localhost:15984/_users
    curl -sX PUT http://localhost:15984/_users/org.couchdb.user:jan \
     -H "Accept: application/json" \
     -H "Content-Type: application/json" \
     -d '{"name": "jan", "password": "apple", "roles": [], "type": "user"}'

}

function doccount {
    curl -sX GET http://localhost:15984/${db_name} | sed 's/.*"doc_count":\([^,]*\),.*/\1/g'
}

function longform_doccount {
    curl -sX GET http://localhost:15984/${db_name}/_all_docs | grep '^{"id"' | wc -l | sed 's/ //g'
}

function make_rsync_files {
    FROM_NODE=$1
    TO_NODE=$2
    python -m couchdb_cluster_admin.file_plan important --conf test/local.yml --from-plan=test/local.plan.json --node $TO_NODE > test/$TO_NODE.files.txt
}

function rsync_files {
    FROM_NODE=$1
    TO_NODE=$2
    < test/$TO_NODE.files.txt rsync -vaH data/$FROM_NODE/data/ data/$TO_NODE/data/ --files-from - -r
}

function wait_for_couch_ping {
    while :
    do
      curl http://localhost:15984/${db_name} -sv 2>&1 | grep '^< HTTP/.* 200 OK' && break || continue
      sleep 1
    done
}

@test "add shards from one node to a cluster" {
    python -m couchdb_cluster_admin.suggest_shard_allocation --conf=test/local.yml --allocate node1:1 --commit-to-couchdb

    python -m couchdb_cluster_admin.suggest_shard_allocation --conf=test/local.yml --allocate node1:1 node2,node3,node4:2 --save-plan=test/local.plan.json

    for i in {1..10}
    do
        curl -sX POST http://localhost:15984/${db_name} -d '{}' -H 'Content-Type: application/json' &
    done

    wait

    [ "$(doccount)" = '10' ]
    [ "$(longform_doccount)" = '10' ]

    { make_rsync_files node1 node2; } &
    { make_rsync_files node1 node3; } &
    { make_rsync_files node1 node4; } &

    wait

    docker restart couchdb-cluster

    wait_for_couch_ping

    run rsync_files node1 node2 && [ "$status" = '23' ]
    run rsync_files node1 node3 && [ "$status" = '23' ]
    run rsync_files node1 node4 && [ "$status" = '23' ]

    python -m couchdb_cluster_admin.suggest_shard_allocation --conf=test/local.yml --from-plan=test/local.plan.json --commit-to-couchdb

    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    sleep 5
    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    sleep 5
    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    sleep 5
    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    sleep 5
    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    sleep 5
    echo $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount) $(doccount)
    [ "$(doccount)" = '10' ]
    echo $(longform_doccount) $(longform_doccount) $(longform_doccount)
    [ "$(longform_doccount)" = '10' ]
}

function teardown {
    echo "DELETE" $db_name
    curl -sX DELETE http://localhost:15984/${db_name}
    rm test/local.plan.json
    rm test/node2.files.txt
    rm test/node3.files.txt
    rm test/node4.files.txt
}
