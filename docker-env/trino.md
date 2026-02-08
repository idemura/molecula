# Setup trino

```bash
docker compose -f trino.yml up -d

# Bring docker env down and destroy volumes:
docker compose -f trino.yml down -v

# Trino CLI:
docker exec -it trino trino --catalog iceberg
docker exec -it trino trino --catalog iceberg --schema dlh
```

Create schema:
```sql
CREATE SCHEMA iceberg.dlh WITH (location = 's3a://datalake/');
```
