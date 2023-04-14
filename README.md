<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-12.2ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/12.2.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-AWS-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
[![POC](https://img.shields.io/badge/POC-1_day-green?style=for-the-badge)](https://databricks.com/try-databricks)

**Interpreting guidewire CDA as delta table:** 
*Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna 
aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. 
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint 
occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.*

<img src="images/approach.png" width=1000>

___
<antoine.amend@databricks.com>

___

## Approach

<img src="images/reconcile.png" width=1000>

## Usage

```scala
import com.databricks.labs.guidewire.Guidewire
val manifestUri = "s3://bucket/key/manifest.json"
val databasePath = "/path/to/delta/table"
Guidewire.index(manifestUri, databasePath)
```

This command will run on a data increment by default, loading our previous checkpoints stored under 
`${databasePath}/_checkpoints`. Should you need to reindex the whole of guidewire data, please provide optional 
savemode parameter as follows

```scala
import org.apache.spark.sql.SaveMode
Guidewire.index(manifestUri, databasePath, SaveMode.Overwrite)
```

Guidewire files will not be stored but referenced from a delta location that can be defined as an external table

```roomsql
CREATE DATABASE IF NOT EXISTS guidewire;
CREATE EXTERNAL TABLE IF NOT EXISTS guidewire.policy_holders LOCATION '/path/to/delta/table';
```

Finally, we can query guidewire data and access all different versions at different timestamps.

```roomsql
SELECT * FROM guidewire.policy_holders
VERSION AS OF 2
```

## Install

```shell
mvn clean package -Pshaded
```

Following maven standard, add profile `shaded` to generate a standalone jar file with all dependencies included. 
This jar can be installed on a databricks [environment](https://docs.databricks.com/libraries/workspace-libraries.html) 
accordingly.

<img src="https://docs.databricks.com/_images/select-library-aws.png" width="300">
