--按国家/地区划分的主要消息创建者
--描述
--对于某个国家,找出人数前Top 100的论坛
--Top论坛的成员作为重点人群
--聚合出重点人群在Top论坛创建的消息数目
--排序要求：消息数目降序，人id升序，取前100
--参数：$date Date = 1672502400000 (2023-01-01)
--输出：person(id bigint, firstName string, lastName string, creationDate bigint), messageCount bigint
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
  firstName string,
  lastName string,
  creationDate bigint,
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--GQL
--找到满足要求的论坛
WITH top100 AS (
  MATCH (forum:Forum where ts > 1334246400000)
  LET forum.memberCount = COUNT((forum:Forum)-[:hasMember]->(member:Person)
                                -[:isLocatedIn]->(:City)
                                -[:isPartOf]->(:Country)
                                => member)
  RETURN forum.id as forumId
  ORDER BY forum.memberCount DESC LIMIT 100
)
--递归地查找满足要求的消息
MATCH (topForum1:Forum where id in top100)-[:containerOf]->(:Post)<-[:replyOf]-{1,}(msg:Message),
(msg:Message)-[:hasCreator]->(person:Person)
WHERE EXISTS (person:Person)<-[:hasMember]-(topForum2:Forum where id in top100)
--产生结果行
RETURN person.id as personId, person.firstName, person.lastName, person.creationDate, Count(*) as messageCount
GROUP BY personId, firstName, lastName, creationDate
--结果行最终排序
ORDER BY messageCount DESC, personId LIMIT 100
;