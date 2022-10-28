# Migration RDS to CloudSql

This code automatize the process of migrate several aws rds instances to gcp cloudsql. 

## Installation

```bash
pipenv install
```

## Usage

### config file

This is an example for config.yaml file with all the necessary parameters to migrate to cloud sql.

```txt
database-name:
  aws-host: pg11db.cmoc13pjl4rh.us-east-2.rds.amazonaws.com
  aws-port: 5432
  aws-replication-password: *******
  aws-replication-username: postgres
  gcp-auto-storage-increase: true
  gcp-database-version: POSTGRES_11
  gcp-disk-type: PD_SSD
  gcp-instance-cpu: 1
  gcp-instance-mem: 3840
  gcp-instance-region: us-east1
  gcp-instance-storage: 20
  gcp-ip-config:
    enableIpv4: true
    authorizedNetworks:
    - label: todos
      value: 0.0.0.0/0
  gcp-migration-type: continuous
  gcp-port: 5432
  gcp-project-id: proyecto-bigdata-318002
  gcp-root-password: 4FQTOMXHCDMG
  gcp-host: 34.139.131.111
```

### migrate

```bash
python dms.py --dbname "database-name"
```

## License

[MIT](https://choosealicense.com/licenses/mit/)