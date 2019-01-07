// LOAD CSV AND CREATE PLAYER NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv($train) yield map as train
','
CREATE (:Player {name:train.name, age:train.age, role:train.role, nationality:train.nationality, from_club:train.from_club, to_club:train.to_club, from_league:train.from_league, to_league:train.to_league, market_value:train.market_value, transfer_value:train.transfer_values})
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE COUNTRY NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Country {name:train.nationality});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE TRANSFER_VALUE NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Transfer_Value {value:train.transfer_values});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE MARKET_VALUE NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Market_Value {value:train.market_value});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE ROLE NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Role {role:train.role});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE LEAGUE NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:League {name:train.from_league});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE LEAGUE NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:League {name:train.to_league});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE CLUB NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Club {name:train.from_club});
', {batchsize:100, iterateList:true, parallel:true});
commit

// LOAD CSV AND CREATE CLUB NODE
begin
CALL apoc.periodic.iterate('
CALL apoc.load.csv("/home/sambeth/Documents/projects_2018/pydatagh/top_football_transfers/data/top_transfers_17-18.csv") yield map as train
','
MERGE (:Club {name:train.to_club});
', {batchsize:100, iterateList:true, parallel:true});
commit

// create indexes
MATCH (p:Player)
CALL apoc.index.addNode(p, ["name"])
RETURN count(*)

MATCH (c:Country)
CALL apoc.index.addNode(c, ["name"])
RETURN count(*)

MATCH (c:Club)
CALL apoc.index.addNode(c, ["name"])
RETURN count(*)

MATCH (l:League)
CALL apoc.index.addNode(l, ["name"])
RETURN count(*)

MATCH (r:Role)
CALL apoc.index.addNode(r, ["role"])
RETURN count(*)

MATCH (m:Market_Value)
CALL apoc.index.addNode(m, ["value"])
RETURN count(*)

MATCH (t:Transfer_Value)
CALL apoc.index.addNode(t, ["value"])
RETURN count(*)

// create RELATIONSHIPS

// PLAYER = COUNTRY
MATCH (p:Player)
MATCH (c:Country)
CALL apoc.create.relationship(p, 'FROM', {}, c) YIELD rel
REMOVE p.nationality
RETURN rel

begin
MATCH (p:Player)
MATCH (c:Country)
WHERE p.nationality = c.name
MERGE (p)-[:FROM]->(c)
REMOVE p.nationality
commit

// PLAYER = ROLE
begin
MATCH (p:Player)
MATCH (r:Role)
WHERE p.role = r.role
MERGE (p)-[:PLAYS_AS_A]->(r)
REMOVE p.role
commit

// PLAYER = MARKET VALUE
begin
MATCH (p:Player)
MATCH (m:Market_Value)
WHERE p.market_value = m.value
MERGE (p)-[:VALUE_AT]->(m)
REMOVE p.market_value
commit

// PLAYER = TRANSFER VALUE
begin
MATCH (p:Player)
MATCH (t:Transfer_Value)
WHERE p.transfer_value = t.value
MERGE (p)-[:SOLD_AT]->(t)
REMOVE p.transfer_value
commit

// PLAYER = FROM CLUB
begin
MATCH (p:Player)
MATCH (c:Club)
WHERE p.from_club = c.name
MERGE (p)-[:FROM_CLUB]->(c)
REMOVE p.from_club
commit

// PLAYER = TO CLUB
begin
MATCH (p:Player)
MATCH (c:Club)
WHERE p.to_club = c.name
MERGE (p)-[:TO_CLUB]->(c)
REMOVE p.to_club
commit

// PLAYER = FROM LEAGUE
begin
MATCH (p:Player)
MATCH (l:League)
WHERE p.from_league = l.name
MERGE (p)-[:FROM_LEAGUE]->(l)
REMOVE p.from_league
commit

// PLAYER = TO LEAGUE
begin
MATCH (p:Player)
MATCH (l:League)
WHERE p.to_league = l.name
MERGE (p)-[:TO_LEAGUE]->(l)
REMOVE p.to_league
commit

// DELETE DUPLICATE NODES SAMPLE
begin
MATCH (g:geo) 
WITH g.id as id, collect(g) AS nodes 
WHERE size(nodes) >  1
FOREACH (g in tail(nodes) | DELETE g)
commit

// DELETE DUPLICATE NODES WITH RELATIONSHIPS SAMPLE 
begin
MATCH (g:geo) 
WITH g.id as id, collect(g) AS nodes 
WHERE size(nodes) >  1
UNWIND tail(nodes) as tails
MATCH (tails)-[r]-()
DELETE r
commit