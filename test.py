# %% 
from sqlalchemy import create_engine

# %%

db_string = "postgresql://postgres:password@localhost:5432"

db = create_engine(db_string)

with db.connect() as connection:
    with connection.begin():
        # Create
        create_table_query = "CREATE TABLE IF NOT EXISTS products (name TEXT, sku TEXT PRIMARY KEY, description TEXT)"
        create_temp_table_query = "CREATE TEMPORARY TABLE temp_products (LIKE products)"
        # create_temp_table_query = "CREATE TABLE temp_products (LIKE products)" # TODO: add temporary
        copy_data_query = """
        COPY temp_products (name, sku, description)
        FROM '/data/products.csv'
        CSV 
        HEADER
        """
        print("....................")
        print("creating required tables")
        connection.execute(create_table_query)
        connection.execute(create_temp_table_query)
        print("....................")
        print("Copying data to staging servers")
        connection.execute(copy_data_query)


        # remove_duplicate_query = """
        # DELETE  FROM
        #     temp_products primary
        #         USING temp_products secondary
        # WHERE
        #     primary.sku = secondary.sku
        # """
        # connection.execute(remove_duplicate_query)
        print("....................")
        find_duplicates_query = """
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
        duplicate_count = list(connection.execute(find_duplicates_query))
        for count in duplicate_count:
            print("There are {} duplicates".format(*count))

        print("....................")
        print("removing duplicates")
        # # Handle duplicate
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
        print("....................")
        connection.execute(handle_duplicate_query)
        duplicate_count = list(connection.execute(find_duplicates_query))
        for count in duplicate_count:
            print("There are {} duplicates now".format(*count))

        print("....................")
        print("Migrating update data from staging tables to actual tables")
        insert_to_products_query = """
        insert into products
        SELECT DISTINCT ON (sku) temp_products.name, temp_products.sku, temp_products.description FROM temp_products LEFT JOIN products
        ON products.sku = temp_products.sku
        WHERE products.sku IS NULL
        """
        connection.execute(insert_to_products_query)

        print("....................")
        # Read
        result_count = list(connection.execute("SELECT count(*) FROM products"))
        for count in result_count:
            print("There are {} products in the table".format(*count))
        print("....................")