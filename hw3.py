from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = SparkSession.builder.appName("hw3").getOrCreate()

# 1. Download data
users_df = spark.read.csv('./datasets/users.csv', header=True)
purchases_df = spark.read.csv('./datasets/purchases.csv', header=True)
products_df = spark.read.csv('./datasets/products.csv', header=True)

for dataset in [users_df, purchases_df, products_df]:
    dataset.show(5)

# 2. Delete nulls
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()


# 3. Sum of purchases for each category
print('Task 3')
(purchases_df.join(products_df, purchases_df.product_id == products_df.product_id, how='inner')
            .select('category', 'quantity', 'price')
            .withColumn('sum_of_purchases', col('quantity').cast('int') * col('price'))
            .groupBy('category')
            .agg(round(sum('sum_of_purchases'),1).alias('total_sum_of_purchases'))
            .show())


# 4. Sum of purchases for each category for age 18-25
print('Task 4')

filtered_df = (purchases_df.join(products_df, purchases_df.product_id == products_df.product_id, how='inner')
            .join(users_df, purchases_df.user_id == users_df.user_id, how='inner')
            .select('category', 'quantity', 'price')
            .withColumn('sum_of_purchases', col('quantity').cast('int') * col('price'))
            .where((col('age') >=18) & (col('age') <= 25))
            .groupBy('category')
            .agg(round(sum('sum_of_purchases'), 1).alias('total_sum_of_purchases')))

filtered_df.show()

# 5. Percentage of purchases for each category for age 18-25
print('Task 5')

grand_total = (purchases_df.join(products_df, purchases_df.product_id == products_df.product_id, how='inner')
            .join(users_df, purchases_df.user_id == users_df.user_id, how='inner')
            .select('quantity', 'price')
            .withColumn('sum_of_purchase', col('quantity').cast('int') * col('price'))
            .where((col('age') >=18) & (col('age') <= 25))
            .agg(sum('sum_of_purchase'))
            .collect()[0][0])

result_df = (purchases_df.join(products_df, purchases_df.product_id == products_df.product_id, how='inner')
            .join(users_df, purchases_df.user_id == users_df.user_id, how='inner')
            .select('category', 'quantity', 'price')
            .withColumn('sum_of_purchases', col('quantity').cast('int') * col('price'))
            .where((col('age') >=18) & (col('age') <= 25))
            .groupBy('category')
            .agg(round(sum('sum_of_purchases')/grand_total*100, 2).alias('percentage_of_purchases')))

result_df.show()

# 6. Top-3 categories by percentage of purchases
print('Task 6')
top_3_cat_df = result_df.orderBy(col('percentage_of_purchases').desc()).limit(3)
top_3_cat_df.show()

top_categories = [row['category'] for row in top_3_cat_df.collect()]

print("Top-3 product categories for 18-25 age group:", top_categories)

spark.stop()