CREATE TABLE t_position (
  positionId VARCHAR PRIMARY KEY,
  positionType VARCHAR,
  updatedDate BIGINT,
  accountId VARCHAR,
  securityCode VARCHAR,
  amount BIGINT,
  price DECIMAL(10, 4),
  active BOOLEAN
) WITH (
  KAFKA_TOPIC = 'topic_position',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'updatedDate'
);

CREATE STREAM s_position (
  positionId VARCHAR KEY,
  positionType VARCHAR,
  updatedDate BIGINT,
  accountId VARCHAR,
  securityCode VARCHAR,
  amount BIGINT,
  price DECIMAL(10, 4),
  active BOOLEAN
) WITH (
  KAFKA_TOPIC = 'topic_position',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'updatedDate'
);

CREATE TABLE t_account (
  accountId VARCHAR PRIMARY KEY,
  accountName VARCHAR,
  updatedDate BIGINT
) WITH (
  KAFKA_TOPIC = 'topic_account',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'updatedDate'
);

CREATE STREAM s_account (
  accountId VARCHAR KEY,
  accountName VARCHAR,
  updatedDate BIGINT
) WITH (
  KAFKA_TOPIC = 'topic_account',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'updatedDate'
);

create table t_position_aggr_9501_rule01 with(kafka_topic = 'topic_rule_execution_result') as
select
  cast(rowtime as STRING) as checkId,
  'rule01' as ruleId,
  ARRAY_EXCEPT(collect_list(positionId), ARRAY ['p00']) as positionId,
  ARRAY_LENGTH(
    ARRAY_EXCEPT(collect_list(positionType), ARRAY ['COMMIT'])
  ) as violationCount
from
  t_position
where
  accountId = '9501'
  and (
    (securityCode = '600000SS') -- 禁投600000SS
    or positionType = 'COMMIT'
  )
  and active = true
group by
  cast(rowtime as STRING)
having
  ARRAY_CONTAINS(collect_list(positionType), 'COMMIT') = true emit changes;

create table t_position_aggr_9501_rule02 as
select
  securityCode,
  AS_VALUE(securityCode) as securityCode2,
  'rule02' as ruleId,
  ARRAY_MAX(collect_list(rowtime)) as checkId,
  collect_list(positionType) as positionType,
  sum(price * amount) as upperValue,
  1000000 as buttomValue
from
  t_position
where
  accountId = '9501'
  and (
    (
      securityCode = '600100SS'
      or securityCode = '600200SS'
    )
    or positionType = 'COMMIT'
  )
  and active = true
group by
  securityCode
having
  ARRAY_LENGTH(collect_list(positionType)) > 0 emit changes;

CREATE or REPLACE STREAM s_position_aggr_9501_rule02 (
  securityCode VARCHAR KEY,
  checkId BIGINT,
  positionType ARRAY<VARCHAR>,
  ruleId VARCHAR,
  upperValue DECIMAL(10, 4),
  bottomValue DECIMAL(10, 4)
) WITH (
  KAFKA_TOPIC = 'T_POSITION_AGGR_9501_RULE02',
  VALUE_FORMAT = 'JSON'
);

CREATE STREAM s_rule_execution_result (
  checkId BIGINT KEY,
  positionType ARRAY<VARCHAR>,
  ruleId VARCHAR,
  securityCode VARCHAR,
  upperValue DECIMAL(10, 4),
  bottomValue DECIMAL(10, 4)
) WITH (
  KAFKA_TOPIC = 'topic_rule_execution_result',
  VALUE_FORMAT = 'JSON'
);

insert into s_rule_execution_result select checkId, positionType, ruleId, securityCode, upperValue, bottomValue from s_position_aggr_9501_rule02 partition by checkId emit changes;