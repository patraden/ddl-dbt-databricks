## ddl-dbt-databricks prototype

### image build
```bash
docker build --no-cache -t ddl-databricks-dbt:latest --build-arg PROFILES_FILE=${PROFILES} -f dbt.Dockerfile .
```
### run image
```bash
docker run -d -p 8080:8080 ddl-databricks-dbt:latest
```

### server setup
```bash
sudo useradd -s /bin/bash -d /home/dbt -m -G sudo dbt
sudo passwd dbt # ddl2022

mkdir test-dbt
mkdir prod-dbt
sudo apt install python3-pip
# create vertual envs:
# https://www.freecodecamp.org/news/how-to-set-up-python-virtual-environment-on-ubuntu-20-04/
```

### feedbacks from Nikolay
1. "Create or replace" when model run while "select" - make tests (ensure no "empty" data)
2. "Merge"
   1. Can we ensure that DBT supports merge for delta based on PK
   2. Updates for big chunks on PK
   3. Can we implement custom schema for model updates (increment)
3. Airflow should detect long-running sql queries to postgres
4. mt5.*deals.timestamp
4. mt5.*users.timestamp/timestamptrade
5. mt4.*trades.timestamp
5. mt4.*users.timestamp

### some facts from study:
1. DBT doesn't have an execution engine, so you can not use it to move data from one source to another as it isn't processing data itself
[SO thread](https://stackoverflow.com/questions/63002171/can-dbt-connect-to-different-databases-in-the-same-project)
2. One important caveat is that dbt does not natively support DELETE as a MATCH action, 
If you have a line in your MERGE statement that uses WHEN MATCHED THEN DELETE, 
you’ll want to treat it like an update and add a soft-delete flag, which is then filtered out in a follow-on transformation.
[docs merges](https://docs.getdbt.com/guides/migration/tools/migrating-from-stored-procedures/5-merges)
[docs deletes](https://docs.getdbt.com/guides/migration/tools/migrating-from-stored-procedures/4-deletes)
3. "Create or replace" is not going to create "data vacuum" as per 
[delta lake concurrency](https://docs.delta.io/0.4.0/delta-concurrency.html)
[databricks acid](https://docs.databricks.com/lakehouse/acid.html)
[databricks set isolation](https://docs.databricks.com/optimizations/isolation-level.html#setting-isolation-level)

### problems faces:
1. long-running catalog list in staging due to: "show table extended in staging like '*'"
   [discussion](https://github.com/dbt-labs/dbt-spark/issues/93), [workaround](https://github.com/dbt-labs/dbt-spark/issues/228)

### docs sevrver start
```bash
nohup dbt docs serve &
```

