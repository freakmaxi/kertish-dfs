# Kertish DFS Data Node

Data node is responsible to store file blocks and serve then they requested.
Default bind endpoint port is `:9430`

Should be started with parameters that are set as environment variables

### Environment Variables
- `BIND_ADDRESS` (optional) : Service binding address. Ex: `127.0.0.1:9430` Default: `:9430`

- `MANAGER_ADDRESS` (mandatory) : Manager Node accessing endpoint. Ex: `http://127.0.0.1:9400`

Manager address will be used to create the data node mapping, reservation, discard and commit 
operations of file/folder placement.

- `SIZE` (mandatory) : The size limit of the node. All the data nodes should be the same size if 
they'll be used in the same cluster. Size value should be uint64. Ex: `1073741824`

- `ROOT_PATH` (optional) : The path to store file blocks. Default: `/opt`

- `CACHE_LIMIT` (optional): Small sized files can be cached for fast access. Value should be uint64 in byte format
Default: `0` (disabled)

- `CACHE_LIFETIME` (optional): Cache lifetime. When it reaches to the limit, garbage collector will free up the memory.
Value should be uint64 in minutes. Default: `360` (6 hours)

### Data Node
Data nodes are smart enough to sync each other. Every create and delete request will be distributed between nodes
using the manager as a gateway. On the first run, if manager node is not accessible, it will start as stand-alone. When 
manager node becomes available, they will automatically join the related cluster.
