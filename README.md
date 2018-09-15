# janusgraph-util
util for janusgraph to import data and so on

```bash
--into /path/to/data \ 
--janus-config-file janusgraph.properties \ 
--skip-duplicate-nodes true \ 
--skip-bad-edges true \ 
--ignore-extra-columns true \ 
--ignore-empty-strings true \ 
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
