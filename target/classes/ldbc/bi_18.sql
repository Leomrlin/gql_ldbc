--朋友推荐
--描述
--对于给定标签，推荐还不认识的两个都对该标签感兴趣的人认识
--同时计算这两个人都认识的朋友数
--排序要求：都认识的朋友数降序，人id升序，取前20
--参数：$tag String = 'Huang Bo'
--输出：person1Id bigint, person2Id bigint, mutualFriendCount bigint
--线上参数： $tag String = 'Mickey_Mantle'
--线上参数： $tag String = 'Caramelldansen'
--线上参数： $tag String = 'Lena_Horne'
--线上参数： $tag String = 'Ty_Cobb'
--线上参数： $tag String = 'Triple_H'
--线上参数： $tag String = 'Hamid_Karzai'
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
  person1Id bigint,
  person2Id bigint,
  mutualFriendCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--MATCH (:Tag where name = 'Huang Bo')<-[:hasInterest]-(person1:Person)
--                                    -[:knows]-(personM:Person)
--                                    -[:knows]-(person2:Person)
--WHERE person1.id <> person2.id
--  AND COUNT((person2:Person)-[:hasInterest]->(tag:Tag where name = 'Huang Bo') => tag) > 0
--  AND COUNT((person2:Person)-[:knows]-(person1_cyc:Person where id = person1.id) => person1_cyc) = 0
--RETURN person1.id as person1Id, person2.id as person2Id, COUNT(personM.id) as mutualFriendCount
--GROUP BY person1Id, person2Id
--ORDER BY mutualFriendCount DESC, person1Id, person2Id
--;

----GQL
--INSERT INTO tbl_result
--MATCH (tag:Tag where name = 'Huang Bo')<-[:hasInterest]-(person1:Person)
--    , (tag:Tag where name = 'Huang Bo')<-[:hasInterest]-(person2:Person)
--WHERE person1.id <> person2.id
--MATCH (person1:Person)-[:knows]-(personM:Person)-[:knows]-(person2:Person)
--WHERE COUNT((person2:Person)-[:knows]-(person1_cyc:Person where id = person1.id) => person1_cyc) = 0
--RETURN person1.id as person1Id, person2.id as person2Id, COUNT(DISTINCT personM.id) as mutualFriendCount
--GROUP BY person1Id, person2Id
--ORDER BY mutualFriendCount DESC, person1Id, person2Id
--;

--优化阐释
--AST层
--将tag的Join优化为global标记，权衡join笛卡尔积和走personM的复杂度
--将person1_cyc做类型消除，方便做走图规约
--优化后GQL
INSERT INTO tbl_result
MATCH (person1:Person)
LET person1.likeTag = COUNT((person1:Person)-[:hasInterest]->(tag where name = 'Huang Bo') => tag)
LET GLOBAL person1.likeTagGlobal = person1.likeTag
MATCH (person1:Person)
WHERE person1.likeTag > 0
MATCH (person1:Person)-[:knows]-(personM:Person)
                               -[:knows]-(person2:Person where likeTagGlobal > 0)
WHERE person1.id <> person2.id
  AND COUNT((person2:Person)-[:knows]-(person1_cyc where id = person1.id) => person1_cyc.id) = 0
RETURN person1.id as person1Id, person2.id as person2Id, COUNT(DISTINCT personM.id) as mutualFriendCount
GROUP BY person1Id, person2Id
ORDER BY mutualFriendCount DESC, person1Id, person2Id
;