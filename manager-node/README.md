# Kertish DFS Manager Node

Manager node is responsible to orchestrate data nodes.
Default bind endpoint port is `:9400`

Manager node keep the index of files in Redis dss and cluster information in mongo db. Redis dss will also use to keep 
index and cluster stability

Should be started with parameters that are set as environment variables

### Environment Variables
- `BIND_ADDRESS` (optional) : Service binding address. Ex: `127.0.0.1:9400` Default: `:9400`

Head node will access the service using `http://127.0.0.1:9400/client/manager` and
Data nodes will access the service using `http://127.0.0.1:9400/client/node`

- `MONGO_CONN` (mandatory) : Mongo DB endpoint. Ex: `mongodb://admin:password@127.0.0.1:27017`

Cluster and Data node setup and information will be kept in Mongo DB.

- `MONGO_DATABASE` (optional) : Mongo DB name. Default: `kertish-dfs`

- `REDIS_CONN` (mandatory) : Redis dss. Ex: `127.0.0.1:6379`

Will be used to index file information.

- `REDIS_CLUSTER_MODE` : Redis cluster mode activation Ex: `true`

- `LOCKING_CENTER` (mandatory) : Locking-Center Server. Ex: `127.0.0.1:22119`

Will be used to synchronize accesses between services

### Manager Cluster and Node Manipulation Requests

- `GET` is used to sync cluster/clusters, list cluster/clusters and nodes and find the cluster information for file.

##### Required Headers:
- `X-Action` defines the behaviour of get request. Values: `sync` or `repair` or `move` or `clusters` or `find`

##### Possible Status Codes
- `422`: Required Request Headers are not valid or absent

##### Sync Action
Sync action is to trigger the sync operation on cluster/clusters.

- `X-Options` header is used to point the cluster or omit it to run the sync operation in all clusters.

##### Possible Status Codes
- `404`: Not found
- `500`: Operational failures
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 100,
  "message": "clusters are not available for sync"
}
```

##### Repair Action
Repair action is to trigger the consistency and integrity repair operation on cluster/clusters.

##### Possible Status Codes
- `404`: Not found
- `500`: Operational failures
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 105,
  "message": "clusters are not available for repair"
}
```

##### Move Action
Move action is to move one cluster content to other one. Target cluster should have enough space for move operation.

- `X-Options` header is used to point the source and target clusters for move operation. `sourceClusterId,targetClusterId`

##### Possible Status Codes
- `404`: Not found
- `500`: Operational failures
- `503`: Service Unavailable
- `507`: Insufficient Space
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 130,
  "message": "cluster is not available for cluster wide actions"
}
```

##### Clusters Action
Clusters action is to get the information about the cluster/clusters and depended on data nodes.

- `X-Options` header is used to point the cluster or omit it to get al clusters information.

##### Possible Status Codes
- `404`: Not found
- `500`: Operational failures
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 110,
  "message": "cluster is already exists"
}
```
 
##### Find Action
Find action is to get the clusterId and data node of a file. 

- `X-Options` header is used to point the fileId. FileId is a sha512 encoded hex string. Ex: `e5c0adae0f05cf60f7e34b45bd44249f42627b1f3b1b453ae45e106adbfdfbdb`

- Successful response contains `X-Cluster-Id` and `X-Address` for search result.

##### Possible Status Codes
- `404`: Not found
- `500`: Operational failures
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 120,
  "message": "cluster is already exists"
}
```
---
- `POST` is used to create cluster, register node, make reservation, create read and delete maps.

##### Required Headers:
- `X-Action` defines the behaviour of post request. Values: `register` or `reserve` or `readMap` or `deleteMap`

##### Possible Status Codes
- `422`: Required Request Headers are not valid or absent

##### Register Action
Register action will create cluster and/or register node.

- `X-Options` header contains the registration or addition details. Format is 
`clusterId=[DataNodeBındingAddress]:[DataNodeBındingPort],[DataNodeBındingAddress]:[DataNodeBındingPort],...`

if you omit the clusterId, request will be accepted as to create new cluster with the given data nodes.

to create new cluster:
Ex: `127.0.0.1:9430,127.0.0.1:9431`

to add new data nodes to the existence cluster:
Ex: `8f0e2bc02811f346d6cbb542c92d118d=127.0.0.1:9430,127.0.0.1:9431`

##### Possible Status Codes
- `400`: Operational failure
- `409`: Cluster is already created/Data Node is already registered
- `422`: Required Request Headers are not valid or absent
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 200,
  "message": "cluster is already exists"
}
```

Successful request response sample
```json
{
  "clusterId": "8f0e2bc02811f346d6cbb542c92d118d",
  "size": 1073741824,
  "used": 0,
  "nodes": [
    {
      "nodeId": "7a758a149e4453b20a40b35f83f3a0e4",
      "address": "127.0.0.1:9430",
      "master": true
    }
  ],
  "reservations": []
}
```

##### Reservation Action
Reservation action is to reserve data space on data nodes to guaranteed that files can be stored.

- `X-Size` header uint64 value for the required space size.

##### Possible Status Codes
- `400`: Operational failures
- `422`: Required Request Headers are not valid or absent
- `507`: Insufficient space
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 210,
  "message": "cluster is already exists"
}
```

Successful request response sample
```json
{
  "reservationId": "e813bfe9-50fd-4a13-992d-10995326210f",
  "clusters": [
    {
      "clusterId": "8f0e2bc02811f346d6cbb542c92d118d",
      "address": "127.0.0.1:9430",
      "chunk": {
        "sequence": 1,
        "index": 0,
        "size": 10247680
      } 
    }
  ]
}
```

##### Read and Delete Map Action
Creates the cluster access map for the specified files.

- `X-Options` header holds the file hex id list with `,` separated. Ex: `e5c0adae0f05cf60f7e34b45bd44249f42627b1f3b1b453ae45e106adbfdfbdb,45bd44249f42627b1f3b1b453ae45e106adbfdfbdba5c0adae0f05cf60f7e34b`

##### Possible Status Codes
- `400`: Operational failures
- `422`: Required Request Headers are not valid or absent
- `507`: No available node
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 220,
  "message": "cluster is already exists"
}
```

Successful request response sample
```json
{
  "e5c0adae0f05cf60f7e34b45bd44249f42627b1f3b1b453ae45e106adbfdfbdb": "127.0.0.1:9430",
  "45bd44249f42627b1f3b1b453ae45e106adbfdfbdba5c0adae0f05cf60f7e34b": "127.0.0.1:9431"
}
```
---
- `DELETE` is used to delete cluster, unregister node, discard or commit reservation.

##### Required Headers:
- `X-Action` defines the behaviour of delete request. Values: `unregister` or `commit` or `discard`

##### Possible Status Codes
- `422`: Required Request Headers are not valid or absent

##### Unregister Action
Unregister action will delete cluster and/or unregister node.

- `X-Options` header contains the unregistering or deletion details. Format is 
`[type],[clusterId/nodeId]`

type values are `c` for cluster and `n` for node. You should push the related id for the operation 
Ex: `c,8f0e2bc02811f346d6cbb542c92d118d`

##### Possible Status Codes
- `404`: Cluster/Node not found
- `422`: Required Request Headers are not valid or absent
- `423`: (only node deletion) Last node cannot be deleted
- `500`: Operational failure
- `200`: Successful

All failed responses comes with error json. code can be `300` for cluster or `350` for node operation Ex:

```json
{
  "code": 300,
  "message": "cluster is already exists"
}
```

##### Commit Reservation Action
Commit Reservation action is to commit that reservation can move to the further process.

- `X-Reservation-Id` header for registration id.
- `X-Options` for committing details. It has a special format `clusterId=size,clusterId=size,...`

clusterId can repeat and the size will grow base on clusterId matching.
size must be uint64.

when reservation is committed once with the clusterId-size map, unused space will be added to the cluster total space. 

##### Possible Status Codes
- `400`: Operational failures
- `422`: Required Request Headers are not valid or absent
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 360,
  "message": "cluster is already exists"
}
```

##### Discard Reservation Action
Discard Reservation action is to discard the reservation in all related clusters.

- `X-Reservation-Id` header for registration id.

when reservation is discarded once, unused space will be added to the cluster total space. 

##### Possible Status Codes
- `400`: Operational failures
- `422`: Required Request Headers are not valid or absent
- `200`: Successful

All failed responses comes with error json. Ex:

```json
{
  "code": 370,
  "message": "cluster is already exists"
}
```