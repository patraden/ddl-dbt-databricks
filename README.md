## ddl-dbt-databricks prototype

### image build
```bash
docker build --no-cache -t ddl-databricks-dbt:latest -f dbt.Dockerfile .
```
### run image
```bash
docker run -d -p 8080:8080 ddl-databricks-dbt:latest
```
