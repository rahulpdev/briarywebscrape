import mysql.connector


class SqlQueries():

    def open_sql_connection(self, host_name, user_name, password, db_name):
        self.db_connection = mysql.connector.connect(
            host=host_name, user=user_name, passwd=password, database=db_name
        )
        self.my_cursor = self.db_connection.cursor()


    def close_sql_connection(self):
        self.db_connection.close()


    def create_tables(self):
        try:
            self.my_cursor.execute('''
                CREATE TABLE product (
                    product_id INT NOT NULL AUTO_INCREMENT,
                    product_title VARCHAR(100) NOT NULL,
                    product_producer VARCHAR(100),
                    wine_type VARCHAR(100),
                    wine_varietal VARCHAR(100),
                    wine_country VARCHAR(100),
                    wine_region VARCHAR(100),
                    wine_sub_region VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT wine_product UNIQUE (product_title, product_producer, wine_type, wine_varietal, wine_country, wine_region, wine_sub_region),
                    PRIMARY KEY (product_id)
                )
            ''')
        except mysql.connector.ProgrammingError:
            pass
        else:
            self.db_connection.commit()

        try:
            self.my_cursor.execute('''
                CREATE TABLE inventory (
                    id INT NOT NULL AUTO_INCREMENT,
                    product_id INT,
                    product_dimension VARCHAR(100) NOT NULL,
                    wine_vintage INT(4),
                    product_price DECIMAL(10, 2) NOT NULL,
                    store_domain VARCHAR(100) NOT NULL,
                    product_image_url VARCHAR(300),
                    product_title VARCHAR(100) NOT NULL,
                    product_producer VARCHAR(100),
                    wine_type VARCHAR(100),
                    wine_varietal VARCHAR(100),
                    wine_country VARCHAR(100),
                    wine_region VARCHAR(100),
                    wine_sub_region VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT wine_inventory UNIQUE (product_dimension, wine_vintage, store_domain, product_title, product_producer, wine_type, wine_varietal, wine_country, wine_region, wine_sub_region),
                    PRIMARY KEY (id),
                    FOREIGN KEY (product_id) REFERENCES product(product_id)
                )
            ''')
        except mysql.connector.ProgrammingError:
            pass
        else:
            self.db_connection.commit()
        

    def update_tables(
        self,
        item_1,
        item_2,
        item_3,
        item_4,
        item_5,
        item_6,
        item_7,
        item_8,
        item_9,
        item_10,
        item_11,
        item_12
        ):
        self.my_cursor.execute('''
            INSERT INTO inventory (
                    product_dimension,
                    wine_vintage,
                    product_price,
                    store_domain,
                    product_image_url,
                    product_title,
                    product_producer,
                    wine_type,
                    wine_varietal,
                    wine_country,
                    wine_region,
                    wine_sub_region
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            product_price = VALUES (product_price),
            last_updated = NOW()            
        ''', (
            item_1,
            item_2,
            item_3,
            item_4,
            item_5,
            item_6,
            item_7,
            item_8,
            item_9,
            item_10,
            item_11,
            item_12
        ))
        self.db_connection.commit()


    def sync_tables(self):
        self.my_cursor.execute('''
            INSERT IGNORE INTO product (
                product_title,
                product_producer,
                wine_type,
                wine_varietal,
                wine_country,
                wine_region,
                wine_sub_region
            )
            SELECT
                product_title,
                product_producer,
                wine_type,
                wine_varietal,
                wine_country,
                wine_region,
                wine_sub_region 
            FROM inventory
            WHERE product_id IS NULL
        ''')

        self.my_cursor.execute('''
            UPDATE inventory
            INNER JOIN product
            ON inventory.product_title = product.product_title
                AND inventory.product_producer = product.product_producer
                AND inventory.wine_type = product.wine_type
                AND inventory.wine_varietal = product.wine_varietal
                AND inventory.wine_country = product.wine_country
                AND inventory.wine_region = product.wine_region
                AND inventory.wine_sub_region = product.wine_sub_region
            SET inventory.product_id = product.product_id
            WHERE inventory.product_id IS NULL
        ''')
        self.db_connection.commit()
