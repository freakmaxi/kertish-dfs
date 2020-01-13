# 2020 DFS

2020 is a distributed file system with basic functionality. It is developed to cover the expectation for
mass file storage requirement in isolated networks. **It does not have security implementation.**

#### Terminology
- Farm: Whole 2020 DFS with multi Clusters. A farm can have one or more clusters with different sizes.
- Cluster: A group of servers to hold data. A cluster can have one or more data node. 
- Data Node: A service running in a cluster to handle data manipulation requests. 
- Master: The server that has the latest version of the data blocks
- Slave: The server that has the carbon copy of the master in the cluster.
- Data Block: Data particle of the big data. Max size is 32mb. 

#### Summary
2020 dfs allows you to create data farm in different locations. Every data farm consist of clusters. Clusters
are data banks. Every cluster has one or more data node. Always one data node works as master. Every data node
additions to that cluster will be used as backup of the master.

Data is always written to and deleted from the master data node in the cluster. However, reading request
will be distributed on the slave data nodes. So in read sensitive environments, as much as new data node added
to the cluster will help you to increase the response time. 

#### Features
- Scalable horizontally. You can add as much as cluster. No size limit.
- Data shadowing. Copy operation won't increase the usage.
- Data particle stacking. Same data block won't be duplicated. 
- Fast move/copy operations.
- Multi tasking. Different request can work on the same folder.
- Automated sync. Data nodes are smart enough to sync the data in the cluster.
- REST architecture for file/folder manipulation.
- Commandline `Admin` and `FileSystem` manipulation tools

#### Architecture

2020 dfs farm consist of minimum
- 1 Manager Node
- 1 Head Node
- 1 Data Node
- Mongo DB
- Redis DSS

`Head Node` is for filesystem interaction. When the data is wanted to access, the application should
make the request to this node. It works as REST service. Filesystem command-line tool communicate directly to 
head node. Check `head-node` folder for details.

`Manager Node` is for orchestrating the cluster(s). When the system should be setup first time or 
manage farm for adding, removing cluster/node, this node will be used. Admin command-line tool 
communicate directly with manager node. Check `manager-node` folder for details.

`Data Node` is to keep the data blocks. All the file data particles will be distributed on data nodes in
different clusters. **CAUTION: Deletion of a data node from cluster may cause the data lost and inconsistency.**

#### Setup

I'll setup a farm using
- 1 Manager Node
- 1 Head Node
- 4 Data Nodes in 2 Clusters working as Master/Slave
- Mongo DB
- Redis DSS

Whole setup will be done on the same machine. This is just for testing purpose. In real world, you 
will need 6 server for this setup.

You will not find the Mongo DB and Redis DSS setup. Please follow the instruction on their web sites.
- [https://www.mongodb.com][Mongo DB]
- [https://redis.io][Redis DSS]

##### Setting Up Manager Node

- Download `2020-dfs-manager` or compile it.
- Copy `2020-dfs-manager` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod 777 /usr/local/bin/2020-dfs-manager`
- Create an empty file on your user path and copy paste the following and save the file
```shell script
#!/bin/sh

export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export REDIS_CONN:="127.0.0.1:6379"                     # Modify the values according to your setup
/usr/local/bin/2020-dfs-manager
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.

##### Setting Up Head Node

- Download `2020-dfs-head` or compile it.
- Copy `2020-dfs-head` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod 777 /usr/local/bin/2020-dfs-head`
- Create an empty file on your user path  and copy paste the following and save the file
```shell script
#!/bin/sh

export MANAGER_ADDRESS="http://127.0.0.1:9400" 
export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export REDIS_CONN:="127.0.0.1:6379"                     # Modify the values according to your setup
/usr/local/bin/2020-dfs-head
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.

##### Setting Up Data Node(s)

- Download `2020-dfs-data` or compile it.
- Copy `2020-dfs-data` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod 777 /usr/local/bin/2020-dfs-data`
- Create following folders - /opt/c1n1 - /opt/c1n2 - /opt/c2n1 - /opt/c2n2
- Create an empty file on your user path named `dn-c1n1.sh` and copy paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9430"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824"
export ROOT_PATH=""/opt/c1n1"
/usr/local/bin/2020-dfs-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.
- Create an empty file on your user path named `dn-c1n2.sh` and copy paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9431"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824"
export ROOT_PATH=""/opt/c1n2"
/usr/local/bin/2020-dfs-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.
- Create an empty file on your user path named `dn-c2n1.sh` and copy paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9432"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824"
export ROOT_PATH=""/opt/c2n1"
/usr/local/bin/2020-dfs-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.
- Create an empty file on your user path named `dn-c2n2.sh` and copy paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9433"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824"
export ROOT_PATH=""/opt/c2n2"
/usr/local/bin/2020-dfs-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.

##### Creating Clusters

- Download `2020-dfs-admin` or compile it.
- Copy `2020-dfs-admin` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod 777 /usr/local/bin/2020-dfs-admin`
- Enter the following command
`2020-dfs-admin -create-cluster 127.0.0.1:9430,127.0.0.1:9431`
- Enter the following command
`2020-dfs-admin -create-cluster 127.0.0.1:9432,127.0.0.1:9433`

if everything went right, you should see something like this
```
Cluster Details: eddd204e4cd23a14cb2f20c84299ee81
      Data Node: 127.0.0.1:9430 (MASTER) -> 526f15a45bf813838accd7fff5040ad7
      Data Node: 127.0.0.1:9431 (SLAVE) -> 205a634981bbad7d8a0651046ed1c87b

ok.
```
and
```
Cluster Details: 8f0e2bc02811f346d6cbb542c92d118d
      Data Node: 127.0.0.1:9432 (MASTER) -> 7a758a149e4453b20a40b35f83f3a0e4
      Data Node: 127.0.0.1:9433 (SLAVE) -> 6776201a0bb7daafb46c9e3931f0807e

ok.
```

##### Manipulating FileSystem

- Download `2020-dfs` or compile it.
- Copy `2020-dfs` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod 777 /usr/local/bin/2020-dfs`
- Enter the following command
`2020-dfs ls`
output: 
```
processing... ok.
total 0
```
- Put a file
`2020-dfs cp local:/usr/local/bin/2020-dfs /2020-dfs`
output: 
```
processing... ok.
```
- Enter the following command
`2020-dfs ls`
output: 
```
processing... ok.
total 1
-  7291kb 2020 Jan 13 05:30 2020-dfs
```

If you get the same or similar outputs like here, congratulations! you successfully set up your 2020-dfs. 

### One Important Note

When you setup the cluster and start taking data blocks, consider that cluster is as absolute. You can not delete it,
move it or change it. Due to this reason, when you are creating the structure of your dfs, pay attention to your
cluster setups the most. Transferring cluster will be introduced in future.

[Mongo DB]: https://www.mongodb.com

[Redis DSS]: https://redis.io