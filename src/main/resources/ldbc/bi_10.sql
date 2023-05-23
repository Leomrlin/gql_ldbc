--社交圈专家
--描述
--在用户的一定社交距离内，查找某个特定国家的社交圈专家
--找出社交圈专家发送的具有某个Tag的所有消息，以这些消息所具有的所有Tag为专家感兴趣Tag
--计算每个社交圈专家感兴趣的Tag，以及该Tag相关的消息数
--排序要求：消息数降序，tag，专家id升序，取前100
--参数：$personId bigint = 1100012  $country String = 'China'  $tagClass String = 'Pet'
--输出：expertCandidatePersonId bigint, tagName string, messageCount bigint

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
  tagName string,
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
--INSERT INTO tbl_result
--WITH p AS (
--  CALL SSSP(1100012, 'knows', 'Person') YIELD (vid, distance)
--  RETURN vid, distance
--)
--MATCH (country:Country where name = 'China')<-[:isPartOf]-(:City)
--                                            <-[:isLocatedIn]-(expertCandidatePerson:Person where id = p.vid)
--WHERE p.distance >= 3 AND p.distance <= 4
--MATCH (expertCandidatePerson:Person)<-[:hasCreator]-(msg:Post|Comment)
--WHERE COUNT((msg:Post|Comment)-[:hasTag]->(:Tag)
--                              -[:hasType]->(tagC:TagClass where name = 'Pet')
--                              => tagC) > 0
--MATCH (msg:Post|Comment)-[:hasTag]->(tag:Tag)
--RETURN expertCandidatePerson.id as personId, tag.name as tagName, COUNT(msg) as messageCount
--GROUP BY personId, tagName
--ORDER BY messageCount DESC, tagName, personId LIMIT 100
--;

--优化阐释
--AST层
--将expertCandidatePerson连续走图优化为虚拟边(第二个expertCandidatePerson在path首个)，依据是给了ID
--RBO层
--将return的本地聚合部分推入m节点执行
--优化后GQL
INSERT INTO tbl_result
WITH p AS (
  CALL SSSP(1100012, 'knows', 'Person') YIELD (vid, distance)
  RETURN vid, distance
  THEN FILTER distance >= 3 AND distance <= 4
)
MATCH (country:Country where name = 'China')<-[:isPartOf]-(:City)
                                            <-[:isLocatedIn]-(expertCandidatePerson:Person where id = p.vid)
                                            <-[:hasCreator]-(msg:Post|Comment)
WHERE COUNT((msg:Post|Comment)-[:hasTag]->(:Tag)
                              -[:hasType]->(tagC:TagClass where name = 'Pet')
                              => tagC) > 0
MATCH (msg:Post|Comment)-[:hasTag]->(tag:Tag)
RETURN expertCandidatePerson.id as personId, tag.name as tagName, COUNT(msg) as messageCount
GROUP BY personId, tagName
ORDER BY messageCount DESC, tagName, personId LIMIT 100
;