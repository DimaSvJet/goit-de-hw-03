import gdown
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType

url1 = "https://drive.google.com/file/d/1GN8o8vYP8LLj3A0UWkvbgnf0YjcF3PkV/view"
url2 = "https://drive.google.com/file/d/1im5cGVl1z8ejypTyNJZSQ2lqMGh6tTFN/view"
url3 = "https://drive.google.com/file/d/1cDLTc0KYQfnsDWHLR0cJkF5puBpAo2Uh/view"
users = "users.csv"
purchases = "purchases.csv"
products = "products.csv"
gdown.download(url1, users, quiet=False, fuzzy=True)
gdown.download(url2, purchases, quiet=False, fuzzy=True)
gdown.download(url3, products, quiet=False, fuzzy=True)

spark = SparkSession.builder.appName("goit-de-hw-03").getOrCreate()

users_df = spark.read.csv(users, header=True)
purchases_df = spark.read.csv(purchases, header=True)
products_df = spark.read.csv(products, header=True)

users_df.createTempView("users_view")
purchases_df.createTempView("purchases_view")
products_df.createTempView("products_view")

# users: ['user_id', 'name', 'age', 'email']
# purchases: ['product_id', 'product_name', 'category', 'price']
# products: ['purchase_id', 'user_id', 'product_id', 'date', 'quantity']


purchases_df.select("price").printSchema()
products_df.select("quantity").printSchema()

users_df_no_null = users_df.dropna()
purchases_df_no_null = purchases_df.dropna()
products_df_no_null = products_df.dropna()

catalogy_df = purchases_df_no_null.select(
    "product_id", "product_name", "category", "price").withColumn("price", F.col("price").cast("double"))

goods_df = products_df_no_null.select(
    "purchase_id", "user_id", "product_id", "quantity").withColumn("quantity", F.col("quantity").cast("double"))

# Визначаємо суму покупок для кожної категорії
sum_purchases = catalogy_df.alias("c").join(
    goods_df.alias("g"), on="product_id", how="right").withColumn("sum_purchases", F.col("g.quantity") * F.col("c.price")).groupBy("category").agg(F.round(F.sum("sum_purchases"), 2).alias("sum_purchases"))


sum_purchases.show()

# Визначаємо суму покупок для вікової категорії 15-25 включно
users_df_int = users_df_no_null.select(
    'user_id', 'name', 'age', 'email').withColumn('age', F.col('age').cast('integer'))


sum_purchases_age = goods_df.alias("g").join(users_df_int.alias(
    "u"), on="user_id", how="inner").where((F.col("u.age") >= 18) & (F.col("u.age") <= 25)).join(
    catalogy_df.alias("c"), on="product_id", how="inner").withColumn("sum_purchases", F.col("g.quantity") * F.col("c.price")).groupBy("category").agg(F.round(F.sum("sum_purchases"), 2).alias("sum_purchases"))

sum_purchases_age.show()


# Визначаємо % витрат від суми покупок для вікової категорії 15-25 включно
total_sum = sum_purchases_age.agg(
    F.sum("sum_purchases").alias("total_sum"))

sum_purchases_age_proc = (
    sum_purchases_age
    .crossJoin(total_sum)
    .withColumn("percent", F.round(F.col("sum_purchases")/F.col("total_sum")*100, 2))
    .select("category", "sum_purchases", "percent")
)
sum_purchases_age_proc.show()

sum_purchases_age_proc_desc = (
    sum_purchases_age
    .crossJoin(total_sum)
    .withColumn("percent", F.round(F.col("sum_purchases")/F.col("total_sum")*100, 2))
    .select("category", "sum_purchases", "percent")
    .orderBy(F.desc("percent")).limit(3)
)
sum_purchases_age_proc_desc.show()


spark.stop()
