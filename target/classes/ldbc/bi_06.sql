--给定主题的最权威用户
--描述
--对于给定Tag,统计每个用户与Tag相关的权威度
--权威度是消息喜欢者受欢迎程度的加和
--受欢迎程度是用户收到的所有赞的总数
--排序要求：权威度降序，人id升序，取前100
--参数：$tag String = 'Cai Ming'
--输出：person(id bigint), authorityScore bigint
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
  score bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--SELECT personId, SUM(cnt) as authorityScore FROM (
--    MATCH (person1:Person)<-[:hasCreator]-(message1:Post|Comment)
--    WHERE COUNT((message1:Post|Comment)-[:hasTag]->(tag:Tag where name = 'Cai Ming') => tag) > 0
--    MATCH (message1:Post|Comment)<-[:likes]-(person2:Person)
--    MATCH (person2:Person)<-[:hasCreator]-(message2:Post|Comment)<-[:likes]-(person3:Person)
--    RETURN person1.id as personId, message1.id as msgId, person2.id as liker,
--           COUNT(message2.id + person3.id) as cnt
--    GROUP BY personId, msgId, liker
--)
--GROUP BY personId
--ORDER BY authorityScore DESC, personId LIMIT 100
--;

----GQL
--INSERT INTO tbl_result
--SELECT personId, SUM(cnt) as authorityScore FROM (
--    MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(message1:Post|Comment)
--                                           -[:hasCreator]->(person1:Person)
--        , (message1:Post|Comment)<-[:likes]-(person2:Person)
--        , (person3:Person)-[:likes]->(message2:Post|Comment)-[:hasCreator]->(person2:Person)
--    RETURN person1.id as personId, message1.id as msgId, person2.id as liker,
--           COUNT(message2.id + person3.id) as cnt
--    GROUP BY personId, msgId, liker
--)
--GROUP BY personId
--ORDER BY authorityScore DESC, personId LIMIT 100
--;

--优化阐释
--AST层
--将person3的类型消除，表达式返回值改为仅ID，方便做走图规约
--将person1走图改写为LET语句，消除类型，使得返回值仅为ID
--CBO层
--将person2路径暂存，减少子查询调用次数
--RBO层
--将return的本地聚合部分推入m节点执行
--优化后GQL
INSERT INTO tbl_result
SELECT personId, authorityScore FROM (
    MATCH (tag:Tag where name = 'Cai Ming')<-[:hasTag]-(message1:Post|Comment)
    LET message1.creator = MAX((message1)-[:hasCreator]->(person1) => CAST(person1.id as BIGINT))
    MATCH (message1:Post|Comment)<-[:likes]-(person2:Person)
    LET person2.popularityScore = COUNT((person2)<-[:hasCreator]-(message2:Post|Comment)
                                                 <-[:likes]-(person3)
                                                 => message2.id + person3.id)
    RETURN message1.creator as personId, SUM(person2.popularityScore) as authorityScore
    GROUP BY personId
)
WHERE authorityScore > 0
ORDER BY authorityScore DESC, personId LIMIT 100
;