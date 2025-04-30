#enter ksql
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

ksql> set 'auto.offset.reset' = 'earliest';

ksql> show topics;

ksql> print 'ksql-test-topic' from beginning;

ksql> create stream ksql_test_stream (emp_id integer, name varchar, vacation_days integer) with (kafka_topic='ksql-test-topic', value_format='DELIMITED');

ksql> create table ksql_test_table (emp_id integer primary key, name varchar, vacation_days integer) with (kafka_topic='ksql-test-topic', value_format='DELIMITED');

ksql> select * from ksql_test_stream;

ksql> select sum(vacation_days) from ksql_test_stream group by emp_id emit changes;

ksql> select name, sum(vacation_days) from ksql_test_stream group by name emit changes;


