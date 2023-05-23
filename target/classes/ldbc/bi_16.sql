--假新闻检测
--描述
--对于两个标签/日期对，在对应日期创建了带有对应标签消息的人作为传播者
--将传播者朋友数量小于阈值的人筛选出来，计算对于每个标签/日期对他们创建的消息数量
--排序要求：两个标签的创建消息数量和降序，人id升序，取前20
--参数：$tagA String = 'Cai Ming' $dateA Date = 1696160400000
--     $tagB String = 'Huang Bo' $dateB Date = 1696160400000
--     maxKnowsLimit bigint = 3
--输出：person(id bigint), messageCountA bigint, messageCountB bigint
--线上参数：
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
  messageCountA bigint,
  messageCountB bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
----找到满足要求的论坛
--INSERT INTO tbl_result
--SELECT personId,
--       case when tagAFriends <= 3 then messageCountA else 0 end as messageCountA,
--       case when tagBFriends <= 3 then messageCountB else 0 end as messageCountB
--FROM (
--    MATCH (person:Person)
--    LET person.countTagAMessage =
--      COUNT((person:Person)<-[:hasCreator]-(msg:Post|Comment)
--                           -[:hasTag]->(tag:Tag where name = 'Cai Ming')
--                           => msg)
--    LET GLOBAL person.globalCountTagAMessage = person.countTagAMessage
--    LET person.countTagBMessage =
--      COUNT((person:Person)<-[:hasCreator]-(msg:Post|Comment)
--                           -[:hasTag]->(tag:Tag where name = 'Huang Bo')
--                           => msg)
--    LET GLOBAL person.globalCountTagBMessage = person.countTagBMessage
--    MATCH (person:Person)
--    WHERE person.countTagAMessage + person.countTagBMessage > 0
--    MATCH (person:Person)-[:knows]-(friend:Person)
--    RETURN person.id as personId,
--           person.countTagAMessage as messageCountA,
--           SUM(IF(friend.globalCountTagAMessage > 0, 1, 0)) as tagAFriends,
--           person.countTagBMessage as messageCountB,
--           SUM(IF(friend.globalCountTagBMessage > 0, 1, 0)) as tagBFriends
--    GROUP BY personId, messageCountA, messageCountB
--)
--ORDER BY messageCountA + messageCountB DESC, personId LIMIT 20
--;

--GQL
--找到满足要求的论坛
INSERT INTO tbl_result
SELECT personId,
       case when tagAFriends <= 3 then messageCountA else 0 end as messageCountA,
       case when tagBFriends <= 3 then messageCountB else 0 end as messageCountB
FROM (
    MATCH (person:Person)
    LET person.countTagAMessage =
      COUNT((person:Person)<-[:hasCreator]-(msg:Post|Comment)
                           -[:hasTag]->(tag:Tag where name = 'Cai Ming')
                           => msg)
    LET GLOBAL person.globalCountTagAMessage = person.countTagAMessage
    LET person.countTagBMessage =
      COUNT((person:Person)<-[:hasCreator]-(msg:Post|Comment)
                           -[:hasTag]->(tag:Tag where name = 'Huang Bo')
                           => msg)
    LET GLOBAL person.globalCountTagBMessage = person.countTagBMessage
    MATCH (person:Person)
    WHERE person.countTagAMessage + person.countTagBMessage > 0
    MATCH (person:Person)-[:knows]-(friend:Person)
    RETURN person.id as personId,
           person.countTagAMessage as messageCountA,
           SUM(IF(friend.globalCountTagAMessage > 0, 1, 0)) as tagAFriends,
           person.countTagBMessage as messageCountB,
           SUM(IF(friend.globalCountTagBMessage > 0, 1, 0)) as tagBFriends
    GROUP BY personId, messageCountA, messageCountB
)
ORDER BY messageCountA + messageCountB DESC, personId LIMIT 20
;