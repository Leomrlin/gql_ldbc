--标签的中心人物
--描述
--对于给定Tag,计算用户的中心度
--对该Tag感兴趣的中心度初始加100
--中心度还需加总最近该用户创建的具有该Tag消息数
--同时求和用户所有朋友的中心度
--参数：$tag String = 'Huang Bo' $startDate Date = 1672502400000  $endDate Date = 1696160400000  (2023-01-01 ~ 2023-10-01)
--输出：personId bigint, score bigint, friendsScore bigint
--排序要求：自身中心度+朋友中心度和降序，人id升序，取前100
--线上参数： $tag String = 'Mickey_Mantle' $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $tag String = 'Caramelldansen' $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $tag String = 'Lena_Horne'  $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $tag String = 'Ty_Cobb'  $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $tag String = 'Triple_H'  $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $tag String = 'Hamid_Karzai'  $startDate Date = 1340984232000  $endDate Date = 1353679766000
CREATE GRAPH bi (
  --static
  --Place
  Vertex Country (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex City (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex Continent (
    id bigint ID,
    name varchar,
    url varchar
  ),
  --Organisation
  Vertex Company (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex University (
    id bigint ID,
    name varchar,
    url varchar
  ),
  --Tag
	Vertex TagClass (
	  id bigint ID,
	  name varchar,
	  url varchar
	),
	Vertex Tag (
	  id bigint ID,
	  name varchar,
	  url varchar
	),

  --dynamic
  Vertex Person (
    id bigint ID,
    creationDate bigint,
    firstName varchar,
    lastName varchar,
    gender varchar,
    --birthday Date,
    --email {varchar},
    --speaks {varchar},
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Forum (
    id bigint ID,
    creationDate bigint,
    title varchar
  ),
  --Message
  Vertex Post (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint,
    lang varchar,
    imageFile varchar
  ),
  Vertex Comment (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint
  ),

  --relations
  --static
	Edge isLocatedIn (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge isPartOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
  Edge isSubclassOf (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge hasType (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),

  --dynamic
	Edge hasModerator (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge containerOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge replyOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge hasTag (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
  Edge hasInterest (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge hasCreator (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge workAt (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    workForm bigint
  ),
  Edge studyAt (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    classYear bigint
  ),

  --temporary
  Edge hasMember (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  ),
  Edge likes (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  )
) WITH (
	storeType='memory',
	geaflow.dsl.using.vertex.path = 'resource:///data/bi_vertex.txt',
	geaflow.dsl.using.edge.path = 'resource:///data/bi_edge.txt'
);

USE GRAPH bi;


CREATE TABLE tbl_result (
  personId bigint,
  score bigint,
  friendsScore bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--MATCH (person:Person)
----计算person的中心度
--LET person.messageScore = COUNT((person:Person)
--                                <-[:hasCreator]-(message:Post|Comment
--                                where creationDate > 1672502400000 and creationDate < 1696160400000)
--                                -[:hasTag]->(tag:Tag where name = 'Huang Bo')
--                                => message)
--LET person.hasInterest = COUNT((person:Person)-[:hasInterest]->(tag:Tag where name = 'Huang Bo') => tag)
--LET person.hasInterestScore = case when person.hasInterest > 0 then 100 else 0 end
----将person的分数打标
--LET GLOBAL person.score = person.hasInterestScore + person.messageScore
--MATCH (person:Person)-[:knows]-{0,1}(friend:Person)
--RETURN person.id as personId, person.score as personCentralityScore,
--SUM(IF(friend.id = person.id, CAST(0 as BIGINT), CAST(friend.score as BIGINT))) as friendScore
--GROUP BY personId, personCentralityScore
--ORDER BY personCentralityScore + friendScore DESC, personId LIMIT 100
--;


----GQL
--INSERT INTO tbl_result
--MATCH (person:Person)
----计算person的中心度
--LET person.messageScore = COUNT((person:Person)
--                                <-[:hasCreator]-(message:Post|Comment
--                                where creationDate > 1672502400000 and creationDate < 1696160400000)
--                                -[:hasTag]->(tag:Tag where name = 'Huang Bo')
--                                => message)
--LET person.hasInterest = COUNT((person:Person)-[:hasInterest]->(tag:Tag where name = 'Huang Bo') => tag)
--LET person.hasInterestScore = case when person.hasInterest > 0 then 100 else 0 end
----将person的分数打标
--LET GLOBAL person.score = person.hasInterestScore + person.messageScore
--MATCH (person:Person)-[:knows]-{0,1}(friend:Person)
--RETURN person.id as personId, person.score as personCentralityScore,
--SUM(IF(friend.id = person.id, CAST(0 as BIGINT), CAST(friend.score as BIGINT))) as friendScore
--GROUP BY personId, personCentralityScore
--ORDER BY personCentralityScore + friendScore DESC, personId LIMIT 100
--;


--优化阐释
--AST层
--将tag做类型消除，方便做走图规约
--RBO层
--将return的本地聚合部分推入走图执行
--优化后GQL
INSERT INTO tbl_result
MATCH (person:Person)
--计算person的中心度
--tag.name = 'Huang Bo' tag.id = 1020002
LET person.hasInterest = COUNT((person:Person)-[:hasInterest]->(tag where id = 1020002) => tag.id)
LET person.messageScore = COUNT((person:Person)
                                <-[:hasCreator]-(message:Post|Comment
                                where creationDate > 1672502400000 and creationDate < 1696160400000)
                                -[:hasTag]->(tag where id = 1020002)
                                => tag.id)
--将person的分数打标
LET GLOBAL person.score = person.messageScore + IF(person.hasInterest > 0, 100, 0)
MATCH (person:Person)-[:knows]-{0,1}(friend:Person)
RETURN person.id as personId, person.score as personCentralityScore,
SUM(IF(friend.id = person.id, CAST(0 as BIGINT), CAST(friend.score as BIGINT))) as friendScore
GROUP BY personId, personCentralityScore
ORDER BY personCentralityScore + friendScore DESC, personId LIMIT 100
;