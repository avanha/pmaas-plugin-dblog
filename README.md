# dblog

A plugin that logs entities that implement the Trackable interface to a database.

## Useful queries

### Index
The plugin creates tables with only the id primary key column, which has an index.

To create additional indexes, for example, on the lastupdatetime column:

```sql
CREATE INDEX
    idx_host_cougar_router_if_pppoe_cl_last_update_time
    ON host_cougar_router_if_pppoe_cl (lastupdatetime)
```

#### TODO List

1. The current implementation works with PostgreSQL.  I'm thinking there could be a plugin (and SPI) that allows
   plugins to contribute a dialect implementation that outputs the SQL required for different operations.
1. Implement diff of table definition vs actual tables and issue SQL calls
      to add missing columns.
1. Add an ability to mark a source field as indexed and have the table create logic create the index.
1. Done: Make it resilient to connection failures.
   1. Detect connection breaks and reconnect.
      1. When the DB went down, we got the following error, and we automatically reconnected when the database
         came online:
         ```
         Error inserting data: unable to begin transaction: dial tcp [::1]:5432: connect: connection refused
         ```
      1. Done: Also make it work for table creation.
      1. Done: See if we can detect a table not found on an insert statement, and
         retry that as well:
         ```
         poller [host_basement_switch_if_1]: Error inserting data: unable to prepare insert statement INSERT INTO host_basement_switch_if_1_bad (Index, Name, PhysicalAddress, IpV4Addresses, BytesIn, BytesOut, PacketsIn, PacketsOut, ErrorsIn, ErrorsOut, DiscardsIn, DiscardsOut, LastUpdateTime) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);: pq: relation "host_basement_switch_if_1_bad" does not exist
         ```
         The error is a PostgreSQL driver specific error with code 42P01, which is
         specific to a table does not exist: https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
   1. Done: Buffer requests and retry on reconnect.
1. Done: Implement support for log on change fields. 
1. Done: Add some unit tests.  go-sqlmock seems like a good tool for this: 
   https://github.com/DATA-DOG/go-sqlmock
1. Done: Add a status page that shows the number of tracked entities, requests executed
   and other pieces of useful information.