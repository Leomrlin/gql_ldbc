--有多少人有给定数量的消息
--描述
--计算每个用户满足条件的消息数目
--以不同消息数目对用户分组，列出每个组用户的数量
--排序要求：组内用户数量降序，消息数目降序
--参数：$startDate Date = 1673798400000  $lengthThreshold bigint = 16  $languages {string}  = en
--输出：messageCount bigint, personCount bigint
--线上参数：$startDate Date = 1297289038000  $lengthThreshold bigint = 40  $languages {string}  = en
--线上参数：$startDate Date = 1340984232000  $lengthThreshold bigint = 10  $languages {string}  = tk
--线上参数：$startDate Date = 1292405215000  $lengthThreshold bigint = 160  $languages {string}  = uz
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
  messageCount bigint,
  personCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--SELECT personMsgCount as messageCount, COUNT(personId) as personCount
--FROM (
--  MATCH (post:Post)<-[:replyOf]-{0,}(msg:Post|Comment)
--                   -[:hasCreator]->(person:Person)
--  WHERE msg.creationDate > 1673798400000
--    and (msg.content is not null and length(msg.content) > 0 or msg.imageFile is not null)
--    and msg.length < 30
--    and post.lang = 'en'
--  RETURN person.id as personId, COUNT(msg) as personMsgCount
--  GROUP BY personId
--)
--GROUP BY personMsgCount
--ORDER BY personCount DESC, personMsgCount DESC
--;

--优化阐释
--AST层
--将person做类型消除，方便做走图规约
--CBO层
--将过滤条件提前执行
--RBO层
--将return的本地聚合部分推入msg节点执行
--优化后GQL
INSERT INTO tbl_result
SELECT personMsgCount as messageCount, COUNT(personId) as personCount
FROM (
  MATCH (post:Post)<-[:replyOf]-{0,}(msg:Post|Comment)
  WHERE msg.creationDate > 1673798400000
    and (msg.content is not null and length(msg.content) > 0 or msg.imageFile is not null)
    and msg.length < 30
    and post.lang = 'en'
  MATCH (msg:Post|Comment)-[:hasCreator]->(person)
  RETURN person.id as personId, COUNT(msg.id) as personMsgCount
  GROUP BY personId
)
GROUP BY personMsgCount
ORDER BY personCount DESC, personMsgCount DESC
;