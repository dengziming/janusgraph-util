

## the idea

load data from csv/db/hive into janusgraph can be divided into 2 seps, first is to generate sstable/hfile,
and the second is load sstable/hfile data into cassandra/hbase.

### generate sstable/hfile

to generate sstable file, we should read file from csv and then create `JanusGraphVertex` or `JanusGraphEdge`,
then serialize them using the `EdgeSerializer`, then use SSTableLoader provided by cassandra. so how to
create `JanusGraphVertex` or `JanusGraphEdge` is the biggest problem.

### allocated id

as we know, every time we new `JanusGraphVertex` or `JanusGraphEdge`, we need to get a Global uniqueness id from janusgraph,
and janusgraphId is allocated one by one, so we can rewrite the `IDPool` to allocate id fastly.

### idMapper

but when new a `JanusGraphEdge`, we also need to send the startNode and endNode, how can we get the startNode or endNode?
in fact we just need to know the nodeId. 

if the input is csv data export from relation database, there must be a primary key in the node, 
and the startNode or endNode of edge is the primary key's value. 
so when we get a vertexId when new `JanusGraphVertex`, we should persist the value and the id in 
somewhere(e.g. HashMap), then we can get the startId from the map according to value of startNode,
so we need a idMapper to keep track of the vertex primary key value and its janusgraph id.

When the number of vertex data is relatively small, a HashMap is a good choice, but whe we have a lot of
vertices, we should implements our own map, that's what idmapper done.

### load sstable/hfile data into cassandra/hbase

`org.apache.cassandra.tools.BulkLoader` can load sstable into cassandra, we just need to call it,
but there are 2 risks, first is the BulkLoader will call System.exit() every time , the second is that 
bulk will use some core api instead of thrift api,make sure the cassandra embed with janusgraph are the same version with your cassandra cluster



## 



