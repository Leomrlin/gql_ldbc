--顶级话题线发起者
--描述
--计算每个用户的发帖数量，和这些帖子中回帖数量的和
--排序要求：回帖数量和降序，人id升序，取前100
--参数：$startDate Date = 1672502400000  $endDate Date = 1696160400000  (2023-01-01 ~ 2023-10-01)
--输出：person(id bigint, firstName string, lastName string), threadCount bigint, messageCount bigint
--线上参数：$startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数：$startDate Date = 1292405215000  $endDate Date = 1336499888000
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
  threadCount bigint,
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--GQL
INSERT INTO tbl_result
SELECT personId, firstName, lastName, threadCount, threadCount + messageCount as messageCount
FROM (
  --2023-01-01 ~ 2023-10-01
  MATCH (person:Person)<-[:hasCreator]-(post:Post where creationDate between 1672502400000 and 1696160400000)
    |+| (person:Person)<-[:hasCreator]-(post:Post where creationDate between 1672502400000 and 1696160400000)
                       <-[:replyOf]-{1,}(comment:Comment)
  WHERE comment is null or comment.creationDate between 1672502400000 and 1696160400000
  RETURN person.id as personId, person.firstName, person.lastName,
         COUNT(DISTINCT post.id) as threadCount, COUNT(1) as messageCount
  GROUP BY personId, firstName, lastName
)
ORDER BY messageCount DESC, personId LIMIT 100
;
