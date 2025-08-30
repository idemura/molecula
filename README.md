# Molecula DB

Molecula in a single host SQL DB for iceberg tables. It support S3 as storage and has internal
catalog on sqlite3. Execution layer based on Velox. It focuses on maximum parallel async IO.

It contains several independent libraries:

* s3: Asynchronous S3 client.
* iceberg: Iceberg table library. Doesn't depend on any IO: user provides ByteBuffer
  with data to parse. Uses simdjson to read JSON, avro files reader implemented.
