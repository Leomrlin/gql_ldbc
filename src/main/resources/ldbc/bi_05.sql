--给定主题的最活跃博主
--描述
--对于给定Tag,统计每个用户与Tag相关的热度
--聚合出具有Tag的消息总数，获赞总数，评论总数，并计算热度值
--排序要求：热度值降序，人id升序，取前100
--参数：$tag String = 'Cai Ming'
--输出：person(id bigint), replyCount bigint, likeCount bigint, messageCount bigint, score bigint
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
  personId bigint,
  replyCount bigint,
  likeCount bigint,
  messageCount bigint,
  score bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--SELECT personId, replyCount, likeCount, messageCount,
--       messageCount + 2*replyCount + 10*likeCount as score
--FROM (
--  MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(m:Post|Comment)
--  LET m.replyCount = COUNT((m:Post|Comment)<-[:replyOf]-(comment:Comment) => comment)
--  LET m.likeCount = COUNT((m:Post|Comment)<-[:likes]-(liker:Person) => liker)
--  MATCH (m:Post|Comment)-[:hasCreator]->(person:Person)
--  RETURN person.id as personId, SUM(m.replyCount) as replyCount, SUM(m.likeCount) as likeCount,
--         COUNT(m) as messageCount
--  GROUP BY personId
--)
--ORDER BY score DESC, personId LIMIT 100
--;

--优化阐释
--AST层
--将comment, liker, person的类型消除，表达式返回值改为仅ID，方便做走图规约
--将与用户交互，把子查询中的comment和liker的类型消除，把person的类型消除
--CBO层
--将Tag的大小表Join转为broadcast join实现
--RBO层
--将return的本地聚合部分推入m节点执行
--优化后GQL
INSERT INTO tbl_result
SELECT personId, replyCount, likeCount, messageCount,
       messageCount + 2*replyCount + 10*likeCount as score
FROM (
  MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(m:Post|Comment)
  LET m.replyCount = COUNT((m:Post|Comment)<-[:replyOf]-(comment) => comment.id)
  LET m.likeCount = COUNT((m:Post|Comment)<-[:likes]-(liker) => liker.id)
  MATCH (m:Post|Comment)-[:hasCreator]->(person)
  RETURN person.id as personId, SUM(m.replyCount) as replyCount, SUM(m.likeCount) as likeCount,
         COUNT(m) as messageCount
  GROUP BY personId
)
ORDER BY score DESC, personId LIMIT 100
;
