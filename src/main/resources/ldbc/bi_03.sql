--一个国家的热门话题
--描述
--对于某个国家和某个TagClass
--找出归属该国的具有TagClass消息的论坛
--聚合出论坛带有该TagClass消息数目
--排序要求：消息数目降序，论坛id升序，取前20
--参数：$tagClass String = 'Comedian' $country String = 'Belarus'
--输出：forum(id bigint, title string, creationDate bigint), person(id bigint), messageCount bigint
--线上参数： $tagClass String = 'CollegeCoach' $country String = 'Belarus'
--线上参数： $tagClass String = 'MusicalArtist' $country String = 'India'
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
  forumId bigint,
  title varchar,
  creationDate bigint,
  personId bigint,
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
----找到满足要求的论坛
--INSERT INTO tbl_result
--MATCH (:Country where name = 'Belarus')<-[:isPartOf]-(:City)
--                                       <-[:isLocatedIn]-(person:Person)
--                                       <-[:hasModerator]-(forum:Forum)
--                                       -[:containerOf]->(:Post)
--                                       <-[:replyOf]-{0,}(msg:Post|Comment)
--                                       -[:hasTag]->(:Tag)
--                                       -[:hasType]->(:TagClass where name = 'Comedian')
----计算聚合值
--RETURN forum.id as forumId, forum.title, forum.creationDate,
--       person.id as personId, Count(DISTINCT msg.id) as messageCount
--GROUP BY forumId, title, creationDate, personId
----结果行最终排序
--ORDER BY messageCount DESC, forumId LIMIT 20
--;

----GQL
----找到满足要求的论坛
--INSERT INTO tbl_result
--MATCH (:Country where name = 'Belarus')<-[:isPartOf]-(:City)
--                                       <-[:isLocatedIn]-(person:Person)
--    , (forum:Forum)-[:hasModerator]->(person:Person)
--    , (forum:Forum)-[:containerOf]->(:Post)
--                   <-[:replyOf]-{0,}(msg:Post|Comment)
--    , (:TagClass where name = 'Comedian')<-[:hasType]-(:Tag)
--                                         <-[:hasTag]-(msg:Post|Comment)
----计算聚合值
--RETURN forum.id as forumId, forum.title, forum.creationDate,
--       person.id as personId, Count(DISTINCT msg.id) as messageCount
--GROUP BY forumId, title, creationDate, personId
----结果行最终排序
--ORDER BY messageCount DESC, forumId LIMIT 20
--;

--优化阐释
--AST层
--将forum连续走图优化为虚拟边(第二个forum在path首个)，依据是contry方向的person远多于forum方向，且需用国家条件
--将tag连续走图优化为Join，依据是tag可能存在负载倾斜，抵消了tag对message过滤的效果(定量权衡)
--将tag做类型消除，方便做走图规约
--CBO层
--可插入nop调整迭代间负载
--将Tag的大小表Join转为broadcast join实现
--RBO层
--将return的本地聚合部分推入走图执行
--优化后GQL
INSERT INTO tbl_result
MATCH (forum:Forum)-[:hasModerator]->(person:Person)
                   -[:isLocatedIn]-(:City)
                   -[:isPartOf]->(:Country where name = 'Belarus')
    , (forum:Forum)-[:containerOf]->(:Post)
                   <-[:replyOf]-{0,}(msg:Post|Comment)
                   -[:hasTag]->(tag)
    , (:TagClass where name = 'Comedian')<-[:hasType]-(tag)
RETURN forum.id as forumId, forum.title, forum.creationDate,
       person.id as personId, Count(DISTINCT msg.id) as messageCount
GROUP BY forumId, title, creationDate, personId
ORDER BY messageCount DESC, forumId LIMIT 20
;