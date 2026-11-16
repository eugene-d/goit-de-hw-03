from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import round as round_
from pyspark.sql.functions import sum as sum_

spark = SparkSession.builder.appName("Purchases").getOrCreate()

users = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products = spark.read.csv("products.csv", header=True, inferSchema=True)

# users.show(5)
# purchases.show(5)
# products.show(5)

users = users.dropna()
purchases = purchases.dropna()
products = products.dropna()

all_sales = purchases.join(products, "product_id").withColumn(
    "total", col("quantity") * col("price")
)

sales_by_category = all_sales.groupBy("category").agg(
    round_(sum_("total"), 2).alias("total_sum")
)
# sales_by_category.show()


age_filter_sales = all_sales.join(users, "user_id").filter(col("age").between(18, 25))

age_filter_category = age_filter_sales.groupBy("category").agg(
    round_(sum_("total"), 2).alias("age_filter_sum")
)
# age_filter_category.show()


age_filter_total = age_filter_sales.agg(sum_("total")).collect()[0][0]

age_filter_share = age_filter_category.withColumn(
    "share_percent", round_(col("age_filter_sum") / age_filter_total * 100, 2)
)
# age_filter_share.show()

top3 = age_filter_share.orderBy(col("share_percent").desc()).limit(3)
top3.show()

spark.stop()
