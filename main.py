import logging

from sqlalchemy import create_engine

QUERY_LIMIT = 20
log_format = "%(asctime)-15s %(levelname)-8s %(message)s"
log_file = "./logfile.log"
log_level = logging.DEBUG
logging.basicConfig(
    level=log_level, filename=log_file, filemode="w+", format=log_format
)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(log_format)
console.setFormatter(formatter)

logging.getLogger("").addHandler(console)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    This class has the whole pipeline with run method to run all the required operation for the making the tables.
    """

    def __init__(self):
        """
        db: stores the engine connected to the postgres db
        query_limit: The limit of how much data should be displayed for demonstration
        connection: A connection to the db to execut the query
        """
        db_string = "postgresql://postgres:password@localhost:5432"
        self.db = create_engine(db_string)
        self.query_limit = QUERY_LIMIT
        self.connection = None
        self.connect()

    def connect(self):
        """
        This method call will make a connection of the db
        """
        if self.connection is None or self.connection.closed:
            self.connection = self.db.connect()
            # self.connection.begin()
        else:
            logger.debug("Connection already open")

    def close(self):
        """
        Method to close the connection
        """
        if self.connection.closed:
            logger.debug("Connection already closed")
        else:
            self.connection.close()

    def run(self):
        """
        This will run all the methods which implements the point of achieve
        """
        logging.debug("-------------------------------> 2")
        self.copy_data_to_staging_tables()  # Point to achieve 2
        logging.debug("-------------------------------> 3")
        self.handle_duplicate_sku()  # Point to achieve 3
        logging.debug("-------------------------------> 4")
        self.insert_or_update_data_to_single_table()  # Point to achieve 4
        logging.debug("-------------------------------> 5")
        self.create_aggregate_table()  # Point to achieve 5
        logging.debug("-------------------------------  ")
        self.close()

    def create_tables(self):
        """
        This method will create the table product and also the staging server called the temp_products as a temporary table which has the same schema as products.
        """
        logging.debug("Creating required tables....")
        create_table_query = "CREATE TABLE IF NOT EXISTS products (name TEXT, sku TEXT PRIMARY KEY, description TEXT)"
        self.connection.execute(create_table_query)

        create_temp_table_query = "CREATE TEMPORARY TABLE temp_products (LIKE products)"
        self.connection.execute(create_temp_table_query)

        logging.debug("Creating an aggregate table called aggregate_product_count....")
        create_aggregate_table = """
            CREATE TABLE IF NOT EXISTS aggregate_product_count (name TEXT, no_of_products INTEGER)
        """
        self.connection.execute(create_aggregate_table)

        create_aggregate_temp_table = """
            CREATE TEMPORARY TABLE temp_aggregate_product_count (LIKE aggregate_product_count)
        """
        self.connection.execute(create_aggregate_temp_table)


    def copy_data_to_staging_tables(self):
        """
        The data from the csv is copied into the staging table temp_products after creating the tables
        """
        self.create_tables()

        logging.debug("Copying data to staging servers....")
        copy_data_query = """
            COPY temp_products (name, sku, description)
            FROM 'products.csv'
            CSV 
            HEADER
        """
        self.connection.execute(copy_data_query)

    def get_duplicate_count(self, from_table="temp_products"):
        """
        This will return the count of how many duplicate sku are present in the product table or in the temp_products table.
        """
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
        """.format(
            from_table
        )
        duplicate_count = list(self.connection.execute(get_duplicates_count_query))
        for count in duplicate_count:
            logging.debug("There are {} duplicates".format(*count))
            return count

    def handle_duplicate_sku(self):
        """
        This method handles the duplicate sku.
        Example:
        entertainment can be present in multiple rows. So on those situations I would add the number with `-dup-` at the end of sku to prevent duplicates.
        like:
        entertainment
        entertainment-dup-1
        entertainment-dup-2
        ....

        Why add number with dup text infront?
        1. The dup is to indicate the duplicate sku
        2. To make sku a primary key
        3. Dropping duplicate would make the aggregate values obselete.
        """
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
        """
        Get the number of all the products in the products table.
        """
        result_count = list(self.connection.execute("SELECT count(*) FROM products"))
        for count in result_count:
            logging.debug("There are {} products".format(*count))
            return count

    def insert_or_update_data_to_single_table(self):
        """
        This method does the ingestion of all the data into a single table `products`.
        Both the update and insert is done so that we are uptodate with the data.
        """
        logging.debug("Migrating update data from staging tables to actual tables...")
        update_to_products_query = """
            UPDATE products
            SET name=temp_products.name, description=temp_products.description
            FROM temp_products
            WHERE products.sku = temp_products.sku;
        """
        self.connection.execute(update_to_products_query)

        insert_to_products_query = """
            INSERT INTO products
            SELECT DISTINCT ON (sku) temp_products.name, temp_products.sku, temp_products.description FROM temp_products LEFT JOIN products
            ON products.sku = temp_products.sku
            WHERE products.sku IS NULL
        """
        self.connection.execute(insert_to_products_query)

        self.get_product_count()

        logging.debug("Sample data from products")
        result = list(
            self.connection.execute(
                "SELECT * FROM products limit {}".format(self.query_limit)
            )
        )
        for row in result:
            logging.debug(row)

    def get_aggregate_result(self):
        """
        Method to get sample aggregate result from products table.
        """
        logging.debug("A sample of the aggregate table")
        aggregate_query = """
        SELECT name, count(*) FROM products GROUP BY name limit {};
        """.format(
            self.query_limit
        )
        result = self.connection.execute(aggregate_query)
        for row in result:
            logging.debug(row)

    def get_aggregate_table_result(self):
        """
        Method to get sample aggregate result from aggregate_product_count table.
        """
        select_from_aggregate_query = """
            SELECT * FROM aggregate_product_count ORDER BY name limit {};
        """.format(
            self.query_limit
        )
        result = self.connection.execute(select_from_aggregate_query)
        for row in result:
            logging.debug(row)

    def create_aggregate_table(self):
        """
        Method to create a sperate aggregate table for the product count.
        """
        self.get_aggregate_result()
        insert_to_temp_table = """
            INSERT INTO temp_aggregate_product_count
            SELECT products.name, count(*) as no_of_products
            FROM products GROUP BY products.name
        """
        self.connection.execute(insert_to_temp_table)

        update_to_products_query = """
            UPDATE aggregate_product_count
            SET name=temp_aggregate_product_count.name, no_of_products=temp_aggregate_product_count.no_of_products
            FROM temp_aggregate_product_count
            WHERE aggregate_product_count.name=temp_aggregate_product_count.name;
        """
        self.connection.execute(update_to_products_query)

        insert_aggregate_data_query = """
            INSERT INTO aggregate_product_count SELECT temp_aggregate_product_count.name, temp_aggregate_product_count.no_of_products FROM temp_aggregate_product_count LEFT JOIN aggregate_product_count
            ON temp_aggregate_product_count.name = aggregate_product_count.name
            WHERE aggregate_product_count.name IS NULL;
        """
        self.connection.execute(insert_aggregate_data_query)


if __name__ == "__main__":
    dp = DataPipeline()
    dp.run()
