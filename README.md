# cassandra-utilities
- count nulls  
Because Cassandra does not allow to do a `select count(*), column2 from table1 where column1 is null group by column2`
