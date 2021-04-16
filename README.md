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

2. Run using the defined class

```python
from main import DataPipeline
dp = DataPipeline()
dp.connect()
dp.create_tables()
```

#### c. Everything from points to achieve is done.

1. The products table has 500000
Sample:
<!--- needed --->
2. The aggregate table has 222024
Sample:
<!--- needed --->