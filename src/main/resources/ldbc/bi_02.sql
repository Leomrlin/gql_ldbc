--标签演变
--描述
--对于某个Tag类型的消息,在D日开始的第一个100天和第二个100天两个窗口中
--分别聚合出消息数，并求得差值
--排序要求：差值降序，Tag内容升序，取前100
--参数：$date Date = 16725024000000  $tagClass String = 'Comedian'
--输出：tag.name String, countWindow1 Integer, countWindow2 Integer, diff Integer
--线上参数： $date Date = 1326269831000  $tagClass String = 'MusicalArtist'
--线上参数： $date Date = 1293172663000  $tagClass String = 'CollegeCoach'
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
  countWindow1 bigint,
  countWindow2 bigint,
  diff bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

----GQL
----获取数据
--INSERT INTO tbl_result
--SELECT name, countWindow1, countWindow2, abs(countWindow2 - countWindow1) as diff
--FROM (
--MATCH (tagClass:TagClass where name = 'Comedian')<-[:hasType]-(tag:Tag)<-[:hasTag]-
--      (msg:Post|Comment where creationDate between 1672502400000
--      and 1672502400000 + 17280000000)
--RETURN  tag.name as name,
--        COUNT(if (msg.creationDate between 1672502400000
--        and 1672502400000 + 8640000000, 1, cast(null as int))) as countWindow1,
--        COUNT(if (msg.creationDate between 1672502400000 + 8640000001
--        and 1672502400000 + 17280000000, 1, cast(null as int))) as countWindow2
--GROUP BY tag.name
--)
--ORDER BY diff desc, name LIMIT 100
--;

--优化阐释
--AST层
--将tag连续走图优化为Join，依据是tag可能存在负载倾斜，抵消了tag对message过滤的效果(定量权衡)
--将tag做类型消除，方便做走图规约
--CBO层
--将小表大表Join转化为broadcast实现，依据是tag可能存在负载倾斜，避免在tag上join造成热点
--RBO层
--将return的本地聚合部分推入走图执行
--优化后GQL
INSERT INTO tbl_result
SELECT name, countWindow1, countWindow2, abs(countWindow2 - countWindow1) as diff
FROM (
MATCH (tagClass:TagClass where name = 'Comedian')<-[:hasType]-(tag)
    , (msg:Post|Comment where creationDate between 1672502400000 and 1672502400000 + 17280000000)
      -[:hasTag]->(tag)
RETURN  tag.name as name,
        COUNT(if (msg.creationDate between 1672502400000
        and 1672502400000 + 8640000000, 1, cast(null as int))) as countWindow1,
        COUNT(if (msg.creationDate between 1672502400000 + 8640000001
        and 1672502400000 + 17280000000, 1, cast(null as int))) as countWindow2
GROUP BY tag.name
)
ORDER BY diff desc, name LIMIT 100
;