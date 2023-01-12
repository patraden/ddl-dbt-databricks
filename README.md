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

### docs sevrver start
```bash
dbt docs generate && nohup dbt docs serve &
```

