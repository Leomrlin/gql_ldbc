--一个国家的僵尸数
--描述
--找出一个国家的所有僵尸,计算其收到的来自僵尸的喜欢和所有喜欢数量,得出僵尸分数
--排序要求：僵尸分数降序，僵尸id升序，取前100
--参数：$endDate Date = 1696160400000  $country string = 'Belarus'
--输出：zombieId bigint, zombieLike bigint, totalLike bigint, zombieScore bigint
--线上参数： $endDate Date = 1353679766000  $country String = 'Belarus'
--线上参数： $endDate Date = 1336499888000  $country String = 'India'
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
  zombieId bigint,
  zombieLike bigint,
  totalLike bigint,
  zombieScore bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--GQL
INSERT INTO tbl_result
SELECT zombieId, zombieLike, totalLike,
       case when totalLike = 0 then 0.0 else CAST(zombieLike as Double)/totalLike end as zombieScore
FROM (
    SELECT zombieId, SUM(case when likerId is null then 0 else 1 end) as totalLike,
           SUM(likerIsZombie) as zombieLike
    FROM (
        MATCH (:Country where name = 'Belarus')
              <-[:isPartOf]-(:City)
              <-[:isLocatedIn]-(zombie:Person where creationDate < 1696160400000)
              WHERE
        (COUNT((zombie:Person)<-[:hasCreator]-(message:Post|Comment where creationDate < 1696160400000)
                              =>message) / 4) < 1  --此处月数简化为 4 个月，暂未按照 BI_13 的标准计算月数
        LET GLOBAL zombie.isZombie = true
        MATCH (zombie:Person)<-[:hasCreator]-(message:Post|Comment)
        MATCH (message:Post|Comment)
          |+| (message:Post|Comment)<-[:likes]-(liker:Person where creationDate < 1696160400000)
        RETURN zombie.id as zombieId, message.id as msgId,
               liker.id as likerId, case when liker.isZombie then 1 else 0 end as likerIsZombie
    )
    GROUP BY zombieId
)
ORDER BY zombieScore DESC, zombieId
;