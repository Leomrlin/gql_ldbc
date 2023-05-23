--朋友三角
--描述
--计算给定时间范围内，同一个国家的不同三角好友关系数量
--参数：$country String = 'China'  $startDate Date = 1672502400000  $endDate Date = 1696160400000  (2023-01-01 ~ 2023-10-01)
--输出：triangleCount bigint
--线上参数： $country String = 'Belarus' $startDate Date = 1340984232000  $endDate Date = 1353679766000
--线上参数： $country String = 'India' $startDate Date = 1292405215000  $endDate Date = 1336499888000
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
  triangleCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--MATCH (:Country where name = 'China')<-[:isPartOf]-(:City)<-[:isLocatedIn]-(personA:Person)
--      -[:knows where creationDate between 1672502400000 and 1696160400000]-(personB:Person)
--      -[:knows where creationDate between 1672502400000 and 1696160400000]-(personC:Person)
--      -[:isLocatedIn]->(:City)-[:isPartOf]->(:Country where name = 'China')
--    , (personB:Person)-[:isLocatedIn]->(:City)-[:isPartOf]->(:Country where name = 'China')
--    , (personC:Person)-[:knows where creationDate between 1672502400000 and 1696160400000]-(_personA:Person)
--WHERE personA.id <> personB.id AND personB.id <> personC.id AND personA.id <> personC.id
--  AND personA.id = _personA.id
--RETURN COUNT(personA.id) / 6 as triangleCount
--;

--优化阐释
--AST层
--将personB, personC连续走图优化为虚拟边
--CBO层
--可插入nop调整迭代间负载
--将Tag的大小表Join转为broadcast join实现
--RBO层
--将return的本地聚合部分推入personA节点执行
--优化后GQL
INSERT INTO tbl_result
MATCH (:Country where name = 'China')<-[:isPartOf]-(:City)<-[:isLocatedIn]-(personA:Person)
      -[:knows where creationDate between 1672502400000 and 1696160400000]-(personB:Person)
    , (:Country where name = 'China')<-[:isPartOf]-(:City)<-[:isLocatedIn]-(personB:Person)
    , (personB:Person)
      -[:knows where creationDate between 1672502400000 and 1696160400000]-(personC:Person)
    , (:Country where name = 'China')<-[:isPartOf]-(:City)<-[:isLocatedIn]-(personC:Person)
WHERE personA.id <> personB.id AND personB.id <> personC.id AND personA.id <> personC.id
MATCH (personC:Person)
      -[:knows where creationDate between 1672502400000 and 1696160400000]-(personA:Person)
WHERE personA.id <> personB.id AND personB.id <> personC.id AND personA.id <> personC.id
RETURN COUNT(personA.id + personB.id + personC.id) / 6 as triangleCount
;