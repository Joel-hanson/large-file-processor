<h1 align="center">
  Large File Processor
</h1>

## Overview

<p align="center">
    Aim is to build a system which is able to handle long running processes in a distributed fashion.
</p>

## Requirements

1. Python (3.6, 3.7, 3.8, 3.9)
2. SQLAlchemy (1.4.7)
3. Docker
4. Postgres


## Prerequisite

1. The current pipeline is run on postgres database. To pull latest postgres image.

`docker pull postgres`

2. Run a postgres container, Run this command from the working directory.

`docker run -d --name postman-postgres  -v $PWD:/data -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:latest`

3. Check if the container is running

`docker ps`

4. If you need to enter the postgres shell

`docker exec -it postman-postgres psql -U postgres`

## Usage

To pull the products csv file from the drive.

`bash download_products.bash`

To run the whole pipeline.

`python main.py`


## Deliverables

#### a. To run the code just follow [Prerequisite](#Prerequisite) and then run code in [Usage](#Usage).
#### b. There are 3 tables
- `products` (Main single table)
- `temp_products` (This is the staging table which will be deleted after run - TEMPORARY TABLE)
- `aggregate_product_count` (The aggregate table which has the name and also the no. of products)

The schema of the table `products` and `temp_products` are the same.

|   Column      | Type   | Collation   | Nullable   | Default  |
| ------------- | ------ | ----------- | ---------- | ---------|
| name          | text   |             |            |          |
| sku (primary key)           | text   |             | not null   |          | 
| description   | text   |             |            |          |

The schema of the table `aggregate_product_count` is 

|     Column     |  Type   | Collation | Nullable | Default |
|----------------|---------|-----------|----------|---------|
| name           | text    |           |          |         |
| no_of_products | integer |           |          |         |

Recreate these tables using the following ways.

1. Run plain sql query.

```sql
CREATE TABLE IF NOT EXISTS products (name TEXT, sku TEXT PRIMARY KEY, description TEXT)
CREATE TEMPORARY TABLE temp_products (LIKE products)
CREATE TABLE IF NOT EXISTS aggregate_product_count (name TEXT, "no. of products" INTEGER)
```
OR

2. Run using the python method

```python
from main import DataPipeline
dp = DataPipeline()
dp.connect()
dp.create_tables()
```

#### c. Everything from points to achieve is done.

- First I extract ever data into a staging table where we could do some initial transforms.
- After the transformations load those data into a single table with sku as primary key.
- Using this single products table create a aggregate table having the count of products that each user baught.

1. The products table has 500000
Sample:
<!--- needed --->
2. The aggregate table has 222024
Sample:
<!--- needed --->
#### d. Nothing is pending to be done in the points to achieve.

Making of the column sku a primary key is done in the following manner.

1. First I took the duplicate sku from the staging table.
2. These duplicate sku where updated to have a suffix like `-dup-1` or `-dup-2`, etc... This was done so that we don't have to remove duplicate primary key data and its easy to find the duplicate row by checking the sku text with the suffix of `-dup-`. The number in the end is to indicate the current number of the duplicated sku.
3. This data is updated into the products table.

#### e. The improvements.

- I would have looked into how to do this with apache spark or any other framework that support parallel ingestion.
- I would have looked into alternative ways of handling the duplicate values in sku.