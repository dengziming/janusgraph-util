# janusgraph-util
util for janusgraph to import data and so on

```bash
--into /path/to/data \ 
--janus-config-file janusgraph.properties \ 
--skip-duplicate-nodes true \ 
--skip-bad-relationships true \ 
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
--relationships:father src/main/resources/e_god_titan_father.csv \ 
--relationships:father src/main/resources/e_demigod_god_father.csv \ 
--relationships:mother src/main/resources/e_demigod_human_mother.csv \ 
--relationships:lives src/main/resources/e_god_location_lives.csv \ 
--relationships:lives src/main/resources/e_monster_location_lives.csv \ 
--relationships:brother src/main/resources/e_god_god_brother.csv \ 
--relationships:battled src/main/resources/e_demigod_monster_battled.csv \ 
--relationships:pet src/main/resources/e_god_monster_pet.csv 
> /path/to/log.log 2>&1 &
```