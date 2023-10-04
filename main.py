from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
expected_df = spark.createDataFrame([
    {"product": "Product 1", "category": "Category 1"},
    {"product": "Product 1", "category": "Category 2"},
    {"product": "Product 2", "category": None},
    {"product": "Product 3", "category": "Category 3"},
    {"product": "Product 4", "category": "Category 4"}
])


class Storage:
    def __init__(self):
        self.__products = spark.createDataFrame([
            {"id": 1, "name": "Product 1", "description": "test"},
            {"id": 2, "name": "Product 2", "description": "test"},
            {"id": 3, "name": "Product 3", "description": "test"},
            {"id": 4, "name": "Product 4", "description": "test"},
        ])

        self.__categories = spark.createDataFrame([
            {"id": 1, "name": "Category 1", "description": "test"},
            {"id": 2, "name": "Category 2", "description": "test"},
            {"id": 3, "name": "Category 3", "description": "test"},
            {"id": 4, "name": "Category 4", "description": "test"},
        ])

        self.__product_categories = spark.createDataFrame([
            {"product_id": 1, "category_id": 1},
            {"product_id": 1, "category_id": 2},
            {"product_id": 3, "category_id": 3},
            {"product_id": 4, "category_id": 4},
        ])

    def get_product_category_df(self):
        combs = self.__products.join(
            self.__product_categories,
            self.__products.id == self.__product_categories.product_id,
            how="full").join(
                self.__categories,
                self.__product_categories.category_id == self.__categories.id,
                how="full"
            )

        return combs.select(self.__products.name.alias("product"), self.__categories.name.alias("category"))


storage = Storage()
df = storage.get_product_category_df()
df.show()
