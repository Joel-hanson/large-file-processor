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


# def main(db):
#     with db.connect() as connection:
#         with connection.begin():
#             logging.debug("-------------------------------> 2")
#             copy_data_to_staging_tables(connection) # Point to achieve 2
#             logging.debug("-------------------------------> 3")
#             handle_duplicate_sku(connection) # Point to achieve 3
#             logging.debug("-------------------------------> 4")
#             transfer_data_to_single_table(connection) # Point to achieve 4
#             logging.debug("-------------------------------> 5")
#             create_aggregate_table(connection) # Point to achieve 5
#             logging.debug("-------------------------------  ")


class DataPipeline():
    def __init__(self):
        db_string = "postgresql://postgres:password@localhost:5432"
        self.db = create_engine(db_string)
        self.connection = None
        self.connect()

    def connect(self):
        self.connection = self.db.connect()
        self.connection.begin()

    def close(self):
        self.connection.close()

    def run(self):
        logging.debug("-------------------------------> 2")
        self.copy_data_to_staging_tables() # Point to achieve 2
        logging.debug("-------------------------------> 3")
        self.handle_duplicate_sku() # Point to achieve 3
        logging.debug("-------------------------------> 4")
        self.transfer_data_to_single_table() # Point to achieve 4
        logging.debug("-------------------------------> 5")
        self.create_aggregate_table() # Point to achieve 5
        logging.debug("-------------------------------  ")
        self.close()

    def create_tables(self):
        create_table_query = "CREATE TABLE IF NOT EXISTS products (name TEXT, sku TEXT PRIMARY KEY, description TEXT)"
        create_temp_table_query = "CREATE TEMPORARY TABLE temp_products (LIKE products)"
        logging.debug("Creating required tables....")
        self.connection.execute(create_table_query)
        self.connection.execute(create_temp_table_query)


    def copy_data_to_staging_tables(self):
        self.create_tables()

        logging.debug("Copying data to staging servers....")
        copy_data_query = """
        COPY temp_products (name, sku, description)
        FROM '/data/products.csv'
        CSV 
        HEADER
        """
        self.connection.execute(copy_data_query)


    def get_duplicate_count(self, from_table="temp_products"):
        logging.debug("Counting row with duplicate sku....")
        get_duplicates_count_query = """
            WITH ct AS (                                                                                                                                            
                SELECT                                     
                ROW_NUMBER() OVER (PARTITION BY sku ORDER BY sku) AS rn,
                sku,
                name,
                description
                FROM {}
            )             
            select count(*)                                                               
            FROM   ct
            WHERE  ct.rn > 1;
        """.format(from_table)
        duplicate_count = list(self.connection.execute(get_duplicates_count_query))
        for count in duplicate_count:
            logging.debug("There are {} duplicates".format(*count))
            return count


    def handle_duplicate_sku(self):
        self.get_duplicate_count()

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
        self.connection.execute(handle_duplicate_query)
        self.get_duplicate_count()


    def get_product_count(self):
        result_count = list(self.connection.execute("SELECT count(*) FROM products"))
        for count in result_count:
            logging.debug("There are {} products".format(*count))
            return count


    def transfer_data_to_single_table(self):
        logging.debug("Migrating update data from staging tables to actual tables...")
        insert_to_products_query = """
        insert into products
        SELECT DISTINCT ON (sku) temp_products.name, temp_products.sku, temp_products.description FROM temp_products LEFT JOIN products
        ON products.sku = temp_products.sku
        WHERE products.sku IS NULL
        """
        self.connection.execute(insert_to_products_query)

        self.get_product_count()

        logging.debug("Sample data from products")
        result = list(self.connection.execute("SELECT * FROM products limit {}".format(QUERY_LIMIT)))
        for row in result:
            logging.debug(row)


    def get_aggregate_result(self):
        logging.debug("A sample of the aggregate table")
        aggregate_query = """
        SELECT name, count(*) FROM products GROUP BY name limit {};
        """.format(QUERY_LIMIT)
        result = self.connection.execute(aggregate_query)
        for row in result:
            logging.debug(row)


    def get_aggregate_table_result(self):
        select_from_aggregate_query = """
        SELECT * FROM aggregate_product_count ORDER BY name limit {};
        """.format(QUERY_LIMIT)
        result = self.connection.execute(select_from_aggregate_query)
        for row in result:
            logging.debug(row)


    def create_aggregate_table(self):
        self.get_aggregate_result()

        logging.debug("Creating an aggregate table called aggregate_product_count....")
        create_aggregate_table = """
        CREATE TABLE IF NOT EXISTS aggregate_product_count (name TEXT, count INTEGER)
        """
        self.connection.execute(create_aggregate_table)

        insert_aggregate_data_query = """
        INSERT INTO aggregate_product_count SELECT name, count(*) FROM products GROUP BY name;
        """
        self.connection.execute(insert_aggregate_data_query)


if __name__ == "__main__":
    dp = DataPipeline()
    dp.run()