# Kertish-DFS

Kertish-dfs is a distributed file system with basic functionality. It is developed to cover the expectation for
mass file storage requirement in isolated networks. **It does not have security implementation.**

#### What is it for?
Kertish-dfs is developed to cover the traditional file system requirements in a scalable way. Software will use the same
path-tree structure virtually to keep the files but will not stuck in hard-drive limits of the machine. It is possible to create
a fail-safe, highly available fast and scalable file storage.

#### Where can it be used?
Systems where they need huge storage space requirements to keep all the files logically together. For example; 
common cloud storage for micro services, video storage for a streaming services, and the like...

#### How shouldn't be used?
Kertish-dfs does not have any security implementation. For this reason, it is best to use it in a publicly isolated
network. Also it does not have file/folder permission integration, so user base limitations are also not available.

#### How is the best usage?
Kertish-dfs is suitable to use as a back service of front services. It means, it is better not to allow users directly 
to the file manipulation.

#### Terminology
- Farm: Whole Kertish-dfs with multi clusters. A farm can have one or more clusters with different sizes.
- Cluster: A group of servers to hold data. A cluster can have one or more data node. 
- Data Node: A service running in a cluster to handle data manipulation requests. 
- Master: The server that has the latest version of the data blocks
- Slave: The server that has the carbon copy of the master in the cluster.
- Data Block: Data particle of the big data. Max size is 32mb. 

#### Summary
Kertish-dfs allows you to create data farm in distributed locations. Every data farm consist of clusters. Clusters
are data banks. Every cluster has one or more data node. Always one data node works as master. Every data node
additions to that cluster will be used as backup of the master.

Data is always written to and deleted from the master data node in the cluster. However, reading request
will be balanced between slave data nodes. So in read sensitive environments, as much as new data node added
to the cluster will help you to increase the response time. 

#### Features
- Scalable horizontally. You can add as much as cluster to grow the size of the storage. No total size limit.
- Data shadowing. Copy operation won't increase the usage but let you the file/folder logically placed
- Data particle stacking. Same data block won't be duplicated so theoretically total physical size may smaller than 
the real size   
- Fast traditional move/copy operations. It can take up to 5 seconds to move/copy a 1tb sized file in the dfs. 
- Multi tasking. Different request can work on the same folder.
- Automated sync. Data nodes are smart enough to sync the data in the cluster.
- REST architecture for file/folder manipulation.
- Command-line `Admin` and `File System` tools

#### Architecture

Kertish-dfs farm consist of minimum
- 1 Manager Node
- 1 Head Node
- 1 Data Node
- Mongo DB
- Redis DSS
- Locking-Center Server

`Head Node` is for filesystem interaction. When the data is wanted to access, the application should
make the request to this node. It works as REST service. Filesystem command-line tool communicate directly to 
head node. Head node is scalable. Check `head-node` folder for details.

`Manager Node` is for orchestrating the cluster(s). When the system should be setup first time or 
manage farm for adding, removing cluster/node, this node will be used. Admin command-line tool 
communicate directly with manager node. Manager node is NOT scalable for now. Check `manager-node` folder for details.

`Data Node` is to keep the data blocks. All the file data particles will be distributed on data nodes in
different clusters.

**CAUTION: Deletion of a data node from cluster may cause the data lost and inconsistency.**
---
#### Setup

I'll setup a farm using
- 1 Manager Node
- 1 Head Node
- 4 Data Nodes in 2 Clusters working as Master/Slave
- Mongo DB
- Redis DSS
- Locking-Center Server

Whole setup will be done on the same machine. This is just for testing purpose. In real world, you 
will need 6 servers for this setup. It is okay to keep the Mongo DB, Redis DSS, Locking-Center Server and Manager Node
in the same machine if it covers the DB and DSS expectations. 

You will not find the Mongo DB, Redis DSS, Locking-Center Server setup. Please follow the instruction on their web
sites.
- [Mongo DB](https://www.mongodb.com)
- [Redis DSS](https://redis.io)
- [Locking-Center Server](https://github.com/freakmaxi/locking-center)

#### Preperation

- Download the latest release of Kertish-dfs or compile it using the `create_release.sh` shell script file located under
the `-build-` folder.

##### Setting Up Manager Node

- Copy `kertish-dfs-manager` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-dfs-manager`
- Create an empty file in your user path, copy-paste the following and save the file
```shell script
#!/bin/sh

export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export REDIS_CONN="127.0.0.1:6379"                      # Modify the values according to your setup
/usr/local/bin/kertish-dfs-manager
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.
---
##### Setting Up Head Node

- Copy `kertish-dfs-head` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-dfs-head`
- Create an empty file in your user path, copy-paste the following and save the file
```shell script
#!/bin/sh

export MANAGER_ADDRESS="http://127.0.0.1:9400" 
export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export LOCKING_CENTER="127.0.0.1:6379"                  # Modify the values according to your setup
/usr/local/bin/kertish-dfs-head
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.
---
##### Setting Up Data Node(s)

- Copy `kertish-dfs-data` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-dfs-data`
- Create following folders - /opt/c1n1 - /opt/c1n2 - /opt/c2n1 - /opt/c2n2
- Create an empty file on your user path named `dn-c1n1.sh`, copy-paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9430"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824" # 1gb
export ROOT_PATH=""/opt/c1n1"
/usr/local/bin/kertish-dfs-data
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.

---

- Create an empty file on your user path named `dn-c1n2.sh`, copy-paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9431"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824" # 1gb
export ROOT_PATH=""/opt/c1n2"
/usr/local/bin/kertish-dfs-data
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.

---

- Create an empty file on your user path named `dn-c2n1.sh`, copy-paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9432"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824" # 1gb
export ROOT_PATH=""/opt/c2n1"
/usr/local/bin/kertish-dfs-data
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.

---

- Create an empty file on your user path named `dn-c2n2.sh`, copy-paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9433"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824" # 1gb
export ROOT_PATH=""/opt/c2n2"
/usr/local/bin/kertish-dfs-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.
---
##### Creating Clusters

**IMPORTANT:** Data nodes sizes in the SAME CLUSTER have to be the same. You may have different servers with
different sized hard-drives. You should use the `SIZE` environment variable to align the storage spaces according to the
the server that has the smallest hard-drive size

 
- Copy `kertish-dfs-admin` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-dfs-admin`
- Enter the following command
`kertish-dfs-admin -create-cluster 127.0.0.1:9430,127.0.0.1:9431`
- If everything went right, you should see an output like
```
Cluster Details: eddd204e4cd23a14cb2f20c84299ee81
      Data Node: 127.0.0.1:9430 (MASTER) -> 526f15a45bf813838accd7fff5040ad7
      Data Node: 127.0.0.1:9431 (SLAVE) -> 205a634981bbad7d8a0651046ed1c87b

ok.
```
- Enter the following command to create the one another cluster
`kertish-dfs-admin -create-cluster 127.0.0.1:9432,127.0.0.1:9433`
- If everything went right, you should see something like this
```
Cluster Details: 8f0e2bc02811f346d6cbb542c92d118d
      Data Node: 127.0.0.1:9432 (MASTER) -> 7a758a149e4453b20a40b35f83f3a0e4
      Data Node: 127.0.0.1:9433 (SLAVE) -> 6776201a0bb7daafb46c9e3931f0807e

ok.
```
---
##### Manipulating FileSystem

- Copy `kertish-dfs` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-dfs`
- Enter the following command
`kertish-dfs ls -l`
output: 
```
processing... ok.
total 0
```
- Put a file from your local drive to dfs
`kertish-dfs cp local:/usr/local/bin/kertish-dfs /kertish-dfs`
output: 
```
processing... ok.
```
- Enter the following command
`kertish-dfs ls -l`
output: 
```
processing... ok.
total 1
-  7291kb 2020 Jan 13 05:30 kertish-dfs
```

If you get the same or similar outputs like here, congratulations! you successfully set up your Kertish-dfs. 

### One Last Important Note

When you setup the cluster and the cluster starts taking data blocks, consider that cluster is as absolute. Deleting 
the cluster will cause you data inconsistency and lost. Due to this reason, when you are creating the structure of
your farm, pay attention to your cluster setups the most. Moving, Splitting and Joining the clusters will be introduced
in future releases.
