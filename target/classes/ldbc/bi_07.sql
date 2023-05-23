--相关话题
--描述
--对于给定Tag,在具有该Tag的消息直接评论中查找
--聚合与原Tag不同的所有Tag相关的评论数
--排序要求：评论数降序，Tag升序，取前100
--参数：$tag String = 'Cai Ming'
--输出：relatedTagName string, count bigint
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
  name string,
  tagCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);


----GQL
--INSERT INTO tbl_result
--MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(:Post|Comment)
--                                       <-[:replyOf]-(comment:Comment)
--WHERE COUNT((comment:Comment)-[:hasTag]->(t:Tag where name = tag.name) => t) = 0
--MATCH (comment:Comment)-[:hasTag]->(relatedTag:Tag)
--RETURN relatedTag.name, COUNT(comment) as tagCount
--GROUP BY name
--ORDER BY tagCount DESC, name LIMIT 100
--;

--优化阐释
--AST层
--将t的类型消除，表达式返回值改为仅ID，方便做走图规约
--将tag连续走图优化为Join，依据是tag可能存在负载倾斜，抵消了tag对message过滤的效果(定量权衡)
--CBO层
--将Tag的大小表Join转为broadcast join实现
--RBO层
--将return的本地聚合部分推入m节点执行
--优化后GQL
INSERT INTO tbl_result
MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(:Post|Comment)
                                       <-[:replyOf]-(comment:Comment)
WHERE COUNT((comment:Comment)-[:hasTag]->(t where id = tag.id) => t.id) = 0
MATCH (comment:Comment)-[:hasTag]->(relatedTag:Tag)
RETURN relatedTag.name, COUNT(comment) as tagCount
GROUP BY name
ORDER BY tagCount DESC, name LIMIT 100
;