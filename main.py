import logging

from sqlalchemy import create_engine

QUERY_LIMIT = 20
log_format = "%(asctime)-15s %(levelname)-8s %(message)s"
log_file = "./logfile.log"
log_level = logging.DEBUG
logging.basicConfig(level=log_level, filename=log_file, filemode="w+",
					format=log_format)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(log_format)
console.setFormatter(formatter) 

logging.getLogger('').addHandler(console) 
logger = logging.getLogger(__name__)


def create_tables(connection):
    create_table_query = "CREATE TABLE IF NOT EXISTS products (name TEXT, sku TEXT PRIMARY KEY, description TEXT)"
    create_temp_table_query = "CREATE TEMPORARY TABLE temp_products (LIKE products)"
    logging.debug("Creating required tables....")
    connection.execute(create_table_query)
    connection.execute(create_temp_table_query)


def copy_data_to_staging_tables(connection):
    create_tables(connection)

    logging.debug("Copying data to staging servers....")
    copy_data_query = """
    COPY temp_products (name, sku, description)
    FROM '/data/products.csv'
    CSV 
    HEADER
    """
    connection.execute(copy_data_query)


def get_duplicate_count(connection):
    logging.debug("Counting row with duplicate sku....")
    get_duplicates_count_query = """
        WITH ct AS (                                                                                                                                            
            SELECT                                     
            ROW_NUMBER() OVER (PARTITION BY sku ORDER BY sku) AS rn,
            sku,
            name,
            description
            FROM temp_products
        )             
        select count(*)                                                               
        FROM   ct
        WHERE  ct.rn > 1;
    """
    duplicate_count = list(connection.execute(get_duplicates_count_query))
    for count in duplicate_count:
        logging.debug("There are {} duplicates".format(*count))
        return count


def handle_duplicate_sku(connection):
    get_duplicate_count(connection)

    logging.debug("Removing duplicates data...")
    handle_duplicate_query = """
        WITH ct AS (
            SELECT 
            ROW_NUMBER() OVER (PARTITION BY sku ORDER BY sku) AS rn,
            sku,
            name,
            description
            FROM temp_products
        )
        UPDATE temp_products 
        SET    sku = ct.sku || CASE WHEN ct.rn = 1 THEN '' ELSE CONCAT('-dup-', (ct.rn-1))::text END
        FROM   ct
        WHERE  ct.rn > 1 and temp_products.sku=ct.sku and temp_products.name=ct.name and temp_products.description=ct.description;
    """
    connection.execute(handle_duplicate_query)
    get_duplicate_count(connection)


def get_product_count(connection):
    result_count = list(connection.execute("SELECT count(*) FROM products"))
    for count in result_count:
        logging.debug("There are {} products".format(*count))
        return count


def transfer_data_to_single_table(connection):
    logging.debug("Migrating update data from staging tables to actual tables...")
    insert_to_products_query = """
    insert into products
    SELECT DISTINCT ON (sku) temp_products.name, temp_products.sku, temp_products.description FROM temp_products LEFT JOIN products
    ON products.sku = temp_products.sku
    WHERE products.sku IS NULL
    """
    connection.execute(insert_to_products_query)

    get_product_count(connection)

    logging.debug("Sample data from products")
    result = list(connection.execute("SELECT * FROM products limit {}".format(QUERY_LIMIT)))
    for row in result:
        logging.debug(row)


def get_aggregate_result(connection):
    logging.debug("A sample of the aggregate table")
    aggregate_query = """
    SELECT name, count(*) FROM products GROUP BY name limit {};
    """.format(QUERY_LIMIT)
    result = connection.execute(aggregate_query)
    for row in result:
        logging.debug(row)


def get_aggregate_table_result(connection):
    select_from_aggregate_query = """
    SELECT * FROM aggregate_product_count ORDER BY name limit {};
    """.format(QUERY_LIMIT)
    result = connection.execute(select_from_aggregate_query)
    for row in result:
        logging.debug(row)


def create_aggregate_table(connection):
    get_aggregate_result(connection)

    logging.debug("Creating an aggregate table called aggregate_product_count....")
    create_aggregate_table = """
    CREATE TABLE IF NOT EXISTS aggregate_product_count (name TEXT, count INTEGER)
    """
    connection.execute(create_aggregate_table)

    insert_aggregate_data_query = """
    INSERT INTO aggregate_product_count SELECT name, count(*) FROM products GROUP BY name;
    """
    connection.execute(insert_aggregate_data_query)


def main(db):
    with db.connect() as connection:
        with connection.begin():
            logging.debug("-------------------------------> 2")
            copy_data_to_staging_tables(connection) # Point to achieve 2
            logging.debug("-------------------------------> 3")
            handle_duplicate_sku(connection) # Point to achieve 3
            logging.debug("-------------------------------> 4")
            transfer_data_to_single_table(connection) # Point to achieve 4
            logging.debug("-------------------------------> 5")
            create_aggregate_table(connection) # Point to achieve 5
            logging.debug("-------------------------------  ")


if __name__ == "__main__":
    db_string = "postgresql://postgres:password@localhost:5432"
    db = create_engine(db_string)
    main(db)