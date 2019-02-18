# janusgraph-util
util for janusgraph to import data and so on

```bash
--into /path/to/data \ 
--janus-config-file janusgraph.properties \ 
--skip-duplicate-nodes true \ 
--skip-bad-edges true \ 
--ignore-extra-columns true \ 
--ignore-empty-strings true \ 
--bulk-loading true \
--bad-tolerance 10000000 \ 
--processors 1 \ 
--id-type string \ 
--max-memory 2G \ 
--drop-keyspace-if-exists true \ 
--nodes:titan src/main/resources/v_titan.csv \ 
--nodes:location src/main/resources/v_location.csv \ 
--nodes:god src/main/resources/v_god.csv \ 
--nodes:demigod src/main/resources/v_demigod.csv \ 
--nodes:human src/main/resources/v_human.csv \ 
--nodes:monster src/main/resources/v_monster.csv \ 
--edges:father src/main/resources/e_god_titan_father.csv \ 
--edges:father src/main/resources/e_demigod_god_father.csv \ 
--edges:mother src/main/resources/e_demigod_human_mother.csv \ 
--edges:lives src/main/resources/e_god_location_lives.csv \ 
--edges:lives src/main/resources/e_monster_location_lives.csv \ 
--edges:brother src/main/resources/e_god_god_brother.csv \ 
--edges:battled src/main/resources/e_demigod_monster_battled.csv \ 
--edges:pet src/main/resources/e_god_monster_pet.csv 
> /path/to/log.log 2>&1 &
```


into : to specify a directory to store SSTable files, this directory should not exists

janus-config-file : to specify your janusgraph config file, you can use janus-config:key value to specify every key

janus-config:key :to specify your janusgraph config if --janus-config-file is missing

ignore-empty-strings : empty strings in csv file will be ignored

bulk-loading: if true, will generator SSTable(cassandra) or HFile(Hbase) instead of insert one by one;
if false, vertices and edges will be inserted into DB one by one through transaction.

max-memory: the max offheap memory, this should be lager than 64*NumberOfNodes 

processors: Number of Thread

drop-keyspace-if-exists: this should only be used when test

bad-tolerance : if the edge data can't be found in vertex data, it is an bad data bad-tolerance means how many we can tolerance

nodes:key value: the key is the vertex Label, and the value is the csv file, more than one file should separate by ",", 
for expmple:--nodes:food "food_header.csv,food_fruit.csv,food_meat.csv,food_other.csv"

edges:key value: similar to nodes.

id-type string :this is necessary, and now we just support string as id.

csv file header of node files, for example `name:ID(god),age:Int`, name is the property, and it is a primary key of node so 
there is ID behind name, and the (god) represent Label, it's unnecessary sometimes. 
csv file header of edge files, for example `god:START_ID(god),monster:END_ID(monster)`, START_ID means the start node
of the edge, END_ID means the end node of the edge, (god) and (monster) represent the label of nodes, 
god and monster are property name, which are meaningless, so you can use `:START_ID(god),:END_ID(monster)` instead.

# error handling

if you set bulk-loading=true, you can just use Cassandra as backend db.

if you set bulk-loading=false, you can use any bigtable that janusgraph support as backend. 
if you got error like this:`Input error: Could not find implementation class: org.janusgraph.diskstorage.cql.CQLStoreManager`
you can add some dependency to pom.xml

```xml
<groupId>org.janusgraph</groupId>
<artifactId>janusgraph-cql</artifactId>
```

# 中文介绍

直接使用命令运行就可以，注意配置项 bulk-loading 默认为 false，如果改成 true，可以实现快速导数据到 Cassandra，
原理是使用Cassandra提供的 SSTableloader。目前只支持 Cassandra 的快速导入，Hbase 的 HFile 无法完成。
如果 bulk-loading 设置为 false，则会一条一条插入 数据库，使用任何一种 JanusGraph 支持的后端存储都可以。

- 如果配置为 false 会使用 `TxImportStoreImpl` ，会将数据写入 janusgraph，支持所有的存储。
- 如果配置为 true 会使用 `BulkImportStoreImpl` 就会将数据写为 SSTable，然后需要使用命令行导入cassandra，目前只支持 Cassandra。
- 注意 cassandra 的 SSTableWriter 有 bug，如果数据量大会出错。

如果有类似的错误： Input error: Could not find implementation class: org.janusgraph.diskstorage.cql.CQLStoreManager。
很明显是因为你 bulk-loading 设置为 false 然后使用了 CQLStoreManager 写数据，但是没有加入 CQLStoreManager 的依赖。只需要加上即可：

```xml
<groupId>org.janusgraph</groupId>
<artifactId>janusgraph-cql</artifactId>
```