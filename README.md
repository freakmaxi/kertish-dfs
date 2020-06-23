<img src="http://imfrk.com/p/kertish-dfs/logo-point.png" width="100px">

# Kertish-DFS

Kertish-dfs is a simple and highly scalable distributed file system to store and serve billions of files. It is
developed to cover the expectation for mass file storage requirements in isolated networks.
**It does not have security implementation.**

#### What is it for?
Kertish-dfs is developed to cover the traditional file system requirements in a scalable way. Software will use the same
path-tree structure virtually to keep the files but will not stuck in hard-drive limits of the machine. It is possible
to create a fail-safe, highly available fast and scalable file storage.

#### Where can it be used?
Systems where they need huge storage space requirements to keep all the files logically together. For example; 
common cloud storage for micro services, video storage for a streaming services, and the like...

#### How shouldn't be used?
Kertish-dfs does not have any security implementation. For this reason, it is best to use it in a publicly isolated
network. Also it does not have file/folder permission integration, so user base limitations are also not available.

#### How is the best usage?
Kertish-dfs is suitable to use as a back service of front services. It means, it is better not to allow users directly 
to the file manipulation.

#### Architecture
Kertish-dfs has 3 vital parts for a working farm. Manager-Node, Head-Node, Data-Node.
- Manager-Node is responsible to handle data-node synchronisation and harmony for each cluster. It is handling the
space reservation, indexing data-node contents for fast search and find operations, health tracking of each data-node
and optimize for the best performance every second, cluster balancing, check/repair/fix operations and synchronisation.
- Head-Node(s) are responsible to handle read, write, delete, copy, move and merge operations. So, when you want to put
a file to Kertish-dfs, you will push the file to this node and it will handle the rest.
- Data-Node(s) are responsible to hold the file chunks and serve it when it is requested as fast as possible.

Manager-Node and Data-Node must not accept direct request(s) coming from the outside of the Kertish-dfs farm. For 
file/folder manipulation, only the access point should be Head-Node.

**Here is a sample scenario;**
1. User wants to upload a file (demo.mov - 125Mb) to /Foo/Bar as demo.mov
2. Uses the Head-Node to upload file with /Foo/Bar/demo.mov header and the file content as binary
3. Head-Node accepts the whole file and asks to Manager-Node where and how to put this file in data-node cluster(s)
4. Manager-Node creates the best possible chunk map for Head-Node and reserve the spaces for those chunks in data-node(s)
5. Head-Node divides the file into chunks and push the chunks to related data-node(s) and at the end commit the provided
map reservation if it has successful placement or discard the reservation to save the space.
6. User gets success or error response with http status header.
7. User can hold the file location in a database (/Foo/Bar/demo.mov)

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
- Possible to take "snapshot" for marking the state of data-node and revert that moment if it requires.
- REST architecture for file/folder manipulation.
- Command-line `Admin` and `File System` tools

#### System Requirements

Kertish-dfs nodes has different hardware requirements to work flawless.

- **Manager-Node** has redis, mongodb and locking-center TCP connections. In addition to that, it serves REST end-points
for head-node and data-node for management feedback requests. For these purposes, a powerful network connection is a
must. If you provide minimum 4 or more CPU Cores, it will significantly drop the response time between node communication.
According to your Kertish farm setup, you may need 2GB or more memory. If you are serving many small files between
1kb to 16mb, it is better to keep memory not less than 8 GB for 4 clusters with 8 data-nodes working master-slave logic
and disk space size is between 350GB to 600GB. It is required to handle synchronization and repair operation handling
otherwise, it can fall to swap space which case slow operation problem and if there is not any swap space configuration,
it will lead the service to crash. **NOTE Always remember that Manager-Node is not scalable right now. It will work only
one instance. I'm working to make scalable.**

- **Head-Node** has mongodb and locking-center TCP connections. Also, it serves REST end-points for file system
manipulations. It means, it is a good idea to have 200mbit or powerful network connection. Head-Node will cache the
uploaded file to process. So, if you are uploading raw 32GB file to the Kertish-dfs, you should have a powerful
memory and swap disk to hold the whole file in the memory. For this reason, you should have a powerful SSD Disk with a 
huge swap space configured. **NOTE this logic will be extended and will able to cover real-time uploading without
caching.** If you are configuring the Kertish-dfs farm to serve just small files between 1kb to 2GB, 4GB ram with
8GB swap space will be more than enough to cover expectations. On read wise, Head-Node does not cache anything, it
transfers the data from the data-node to client. So memory is essential just for file uploads. Remember that, Head-Node
is scalable and you can put as much as Head-Node for file manipulation behind the load balancer. CPU is not a big 
consideration. Minimum 2 or more CPU Cores will be sufficient. Head-Node does not do any serious calculation.

- **Data-Node** has backend custom TCP ports to serve content. It makes REST requests to Manager-Node. For this reason,
each node can have 100mbit or powerful network connection to serve files. Data-Node has optional caching feature to
cache the most requested files to serve quickly. For a data-node with 500GB disk space without caching, 4GB memory is
enough to operate. More memory will not change the response time or performance. If you consider to use caching, just
put as much as memory top of 4GB to improve response time. It means, if you have a machine with 16GB memory,
16GB-4GB = 12GB memory can be use for caching. Hard disk is a key point here. Better to use SSD for fast access and
serve. HDD will be also okay if you are storing huge files because it will not have many small file chunks stored on
the disk. However, small files will create many chunks which will affect seek time of the disk head and that will lead
you a slow data-node. On CPU wise, it is not a critical topic. Minimum 2 or more CPU cores will be sufficient to serve
files. On the other hand, slave nodes are periodically synchronize content with master and on that operation, CPU usage
can raise. So if you provide fast and more CPU core(s), synchronisation will finish quicker. 

#### Setup Description

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
#### Sample Setup

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

### Setup Using Docker

The docker hub page is [https://hub.docker.com/r/freakmaxi/kertish-dfs]

You can use the sample docker-compose file to kickstart the Kertish-dfs farm in docker container with 6 Data-Nodes 
working in 3 Clusters as Master/Slave

[https://github.com/freakmaxi/kertish-dfs/blob/master/docker-compose.yml]

`docker-compose up` will make everything ready for you.

Download setup script from [https://github.com/freakmaxi/kertish-dfs/blob/master/kertish-docker-setup.sh]

- Download Client-Tools for Kertish-dfs from [https://github.com/freakmaxi/kertish-dfs/releases] according to your OS
- Give execution permission to the file `sudo chmod +x kertish-docker-setup`
- Execute setup script.
- type `y` and press `enter`

Your Kertish-dfs farm is ready to go.

Put any file using `kertish-dfs` file system tool. Ex:

`./kertish-dfs cp local:~/Downloads/demo.mov /demo.mov`

Just change the path and file after `local:` according to the file in your system. Try to choose a file more than 70 Mb
to see file chunk distribution between clusters. If file size is smaller than 32 Mb, it will be placed only in a cluster.

`./kertish-dfs ls -l` will give you an output similar like below

```
processing... ok.
total 1
- 87701kb 2020 Jun 22 22:07 demo.mov
```

---

### Setup Using Release/Source

#### Preparation

- Download the latest release of Kertish-dfs or compile it using the `create_release.sh` shell script file located under
the `-build-` folder.

##### Setting Up Manager Node

- Copy `kertish-manager` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-manager`
- Create an empty file in your user path, copy-paste the following and save the file
```shell script
#!/bin/sh

export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export REDIS_CONN="127.0.0.1:6379"                      # Modify the values according to your setup
export LOCKING_CENTER="127.0.0.1:22119"                 # Modify the values according to your setup
/usr/local/bin/kertish-manager
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.
---
##### Setting Up Head Node

- Copy `kertish-head` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-head`
- Create an empty file in your user path, copy-paste the following and save the file
```shell script
#!/bin/sh

export MANAGER_ADDRESS="http://127.0.0.1:9400" 
export MONGO_CONN="mongodb://root:pass@127.0.0.1:27017" # Modify the values according to your setup
export LOCKING_CENTER="127.0.0.1:22119"                 # Modify the values according to your setup
/usr/local/bin/kertish-head
```
- Give execution permission to the file `sudo chmod +x [Saved File Location]`
- Execute the saved file.
---
##### Setting Up Data Node(s)

- Copy `kertish-data` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-data`
- Create following folders - /opt/c1n1 - /opt/c1n2 - /opt/c2n1 - /opt/c2n2
- Create an empty file on your user path named `dn-c1n1.sh`, copy-paste the following and save the file
```shell script
#!/bin/sh

export BIND_ADDRESS="127.0.0.1:9430"
export MANAGER_ADDRESS="http://127.0.0.1:9400"
export SIZE="1073741824" # 1gb
export ROOT_PATH="/opt/c1n1"
/usr/local/bin/kertish-data
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
export ROOT_PATH="/opt/c1n2"
/usr/local/bin/kertish-data
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
export ROOT_PATH="/opt/c2n1"
/usr/local/bin/kertish-data
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
export ROOT_PATH="/opt/c2n2"
/usr/local/bin/kertish-data
```
- Give execution permission to the file `sudo chmod 777 [Saved File Location]`
- Execute the saved file.
---
##### Creating Clusters

**IMPORTANT:** Data nodes sizes in the SAME CLUSTER have to be the same. You may have different servers with
different sized hard-drives. You should use the `SIZE` environment variable to align the storage spaces according to the
the server that has the smallest hard-drive size

 
- Copy `kertish-admin` executable to `/usr/local/bin` folder on the system.
- Give execution permission to the file `sudo chmod +x /usr/local/bin/kertish-admin`
- Enter the following command
`kertish-admin -create-cluster 127.0.0.1:9430,127.0.0.1:9431`
- If everything went right, you should see an output like
```
Cluster Details: eddd204e4cd23a14cb2f20c84299ee81
      Data Node: 127.0.0.1:9430 (MASTER) -> 526f15a45bf813838accd7fff5040ad7
      Data Node: 127.0.0.1:9431 (SLAVE) -> 205a634981bbad7d8a0651046ed1c87b

ok.
```
- Enter the following command to create the one another cluster
`kertish-admin -create-cluster 127.0.0.1:9432,127.0.0.1:9433`
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
your farm, pay attention to your cluster setups the most. If you want to remove the cluster from the farm, consider to
move the cluster first from one point to another using `kertish-admin` client tool.
