--信息传播分析
--描述
--对于给定的Tag,当一个人在论坛1发送相关消息之后一段时间
--论坛2出现了两个来自论坛1不同的人的有关消息与评论
--排序要求：符合要求的论坛2消息计数降序，论坛1发帖人id升序，取前10
--参数：$tag String = 'Huang Bo', $delta bigint = 1728000000
--输出：person1Id bigint, messageCount bigint
--线上参数： $tag String = 'Mickey_Mantle'  $delta bigint = 86400000
--线上参数： $tag String = 'Caramelldansen'  $delta bigint = 1728000000
--线上参数： $tag String = 'Lena_Horne'    $delta bigint = 86400000
--线上参数： $tag String = 'Ty_Cobb'    $delta bigint = 1728000000
--线上参数： $tag String = 'Triple_H'     $delta bigint = 86400000
--线上参数： $tag String = 'Hamid_Karzai'   $delta bigint = 1728000000
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
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--GQL
INSERT INTO tbl_result
WITH p AS (
    MATCH (tag:Tag where name = 'Huang Bo') RETURN tag.id AS startId
)
Match (:Tag where id = p.startId)<-[:hasTag]-(message2:Post|Comment)
                                         -[:replyOf]->{0,}(post2:Post)
                                         <-[:containerOf]-(forum2:Forum)
    , (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
                                         -[:hasCreator]->(person1:Person)
WHERE message2.creationDate > message1.creationDate + 1728000000
  AND COUNT((forum2:Forum)-[:hasMember]->(person1_cyc:Person where id = person1.id)
                                => person1_cyc) = 0
Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
                                         -[:hasCreator]->(person2:Person)
                                         <-[:hasMember]-(forum1:Forum)
WHERE forum1.id <> forum2.id
Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
                                         -[:replyOf]->(message2:Post|Comment)
                                         -[:hasCreator]->(person3:Person)
                                         <-[:hasMember]-(forum1:Forum)
WHERE person2.id <> person3.id
Match (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
                                         -[:replyOf]->{0,}(post1:Post)
                                         <-[:containerOf]-(forum1:Forum)
RETURN person1.id as person1Id, COUNT(DISTINCT message2.id) as messageCount
GROUP BY person1Id
ORDER BY messageCount DESC, person1Id LIMIT 10
;

----GQL
--INSERT INTO tbl_result
--WITH p AS (
--    MATCH (tag:Tag where name = 'Huang Bo') RETURN tag.id AS startId
--)
--Match (message2:Post|Comment)
--LET message2.isHasTag = COUNT((message2)-[:hasTag]->(tag where id = p.startId) => tag.id)
--LET GLOBAL message2.isHasTagGlobal = message2.isHasTag
--Match (message2:Post|Comment)
--WHERE message2.isHasTag > 0
--Match (message2:Post|Comment)
--LET message2.forum2Id = MAX((message2)-[:replyOf]->{0,}(post2:Post)
--                                      <-[:containerOf]-(forum2) => forum2.id)
--Match (message2:Post|Comment)<-[:replyOf]-(comment:Comment WHERE isHasTagGlobal > 0)
--                             -[:hasCreator]->(person2:Person)
--    , (message2:Post|Comment)-[:hasCreator]->(person3:Person)
--WHERE person2.id <> person3.id
--
--Match (message1:Post|Comment)
--WHERE message1.isHasTag > 0
--  AND message1.creationDate > message1.creationDate + 1728000000
--
--Match (message1:Post|Comment)-[:hasCreator]->(person1:Person)
--    , (message1:Post|Comment)-[:replyOf]->{0,}(post1:Post)
--
--Match (:Tag where id = p.startId)<-[:hasTag]-(message2:Post|Comment)
--                                         -[:replyOf]->{0,}(post2:Post)
--                                         <-[:containerOf]-(forum2:Forum)
--    , (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
--                                         -[:hasCreator]->(person1:Person)
--WHERE message2.creationDate > message1.creationDate + 1728000000
--  AND COUNT((forum2:Forum)-[:hasMember]->(person1_cyc:Person where id = person1.id)
--                                => person1_cyc) = 0
--Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
--                                         -[:hasCreator]->(person2:Person)
--                                         <-[:hasMember]-(forum1:Forum)
--WHERE forum1.id <> forum2.id
--Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
--                                         -[:replyOf]->(message2:Post|Comment)
--                                         -[:hasCreator]->(person3:Person)
--                                         <-[:hasMember]-(forum1:Forum)
--WHERE person2.id <> person3.id
--Match (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
--                                         -[:replyOf]->{0,}(post1:Post)
--                                         <-[:containerOf]-(forum1:Forum)
--RETURN person1.id as person1Id, COUNT(DISTINCT message2.id) as messageCount
--GROUP BY person1Id
--ORDER BY messageCount DESC, person1Id LIMIT 10
--;