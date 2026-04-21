from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Lab3_Spark").getOrCreate()

output = open("output_lab4.txt", "w", encoding="utf-8")

# cau 1 doc du lieu csv va tu suy ra kieu du lieu
customers = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("Customer_List.csv")
orders = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("Orders.csv")
order_items = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("Order_Items.csv")
products = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("Products.csv")
reviews = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("Order_Reviews.csv")

output.write("--- Customer_List Schema ---\n")
for col_name, dtype in customers.dtypes:
    output.write(f"{col_name}: {dtype}\n")
output.write("\n\n")

output.write("--- Order_Items Schema ---\n")
for col_name, dtype in order_items.dtypes:
    output.write(f"{col_name}: {dtype}\n")
output.write("\n\n")

output.write("--- Order_Reviews Schema ---\n")
for col_name, dtype in reviews.dtypes:
    output.write(f"{col_name}: {dtype}\n")
output.write("\n\n")

output.write("--- Orders Schema ---\n")
for col_name, dtype in orders.dtypes:
    output.write(f"{col_name}: {dtype}\n")
output.write("\n\n")

output.write("--- Products Schema ---\n")
for col_name, dtype in products.dtypes:
    output.write(f"{col_name}: {dtype}\n")
output.write("\n\n")

# cau 2 thong ke
total_orders = orders.select("Order_ID").distinct().count()
total_customers = customers.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items.select("Seller_ID").distinct().count()

output.write("2. THONG KE\n")
output.write(f"Tong so don hang: {total_orders}\n")
output.write(f"Tong so khach hang: {total_customers}\n")
output.write(f"Tong so nguoi ban: {total_sellers}\n\n")

# cau 3
orders_country = orders.join(customers, "Customer_Trx_ID") \
    .groupBy("Customer_Country") \
    .agg(countDistinct("Order_ID").alias("Total_Orders")) \
    .orderBy(desc("Total_Orders"))

output.write("3. DON HANG THEO QUOC GIA\n")
for row in orders_country.collect():
    output.write(f"{row['Customer_Country']}: {row['Total_Orders']}\n")
output.write("\n")

# cau 4
orders_time = orders.withColumn("Order_Date", to_timestamp("Order_Purchase_Timestamp")) \
    .withColumn("Year", year("Order_Date")) \
    .withColumn("Month", month("Order_Date"))

orders_group = orders_time.groupBy("Year", "Month") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(asc("Year"), desc("Month"))

output.write("4. SO LUONG DON HANG NHOM THEO NAM, THANG DAT HANG\n")
output.write(f"{'Nam':<10}{'Thang':<10}{'So don hang':<15}\n")
output.write("-" * 35 + "\n")

for row in orders_group.collect():
    output.write(f"{row['Year']:<10}{row['Month']:<10}{row['Total_Orders']:<15}\n")

output.write("\n")

# cau 5
from pyspark.sql.functions import col, avg, count, asc, regexp_extract

reviews_fix = reviews.withColumn(
    "Review_Score",
    regexp_extract(col("Review_Score"), r'\d+', 0).cast("int")
)

reviews_clean = reviews_fix.filter(
    (col("Review_Score").isNotNull()) &
    (col("Review_Score") >= 1) &
    (col("Review_Score") <= 5)
)

avg_result = reviews_clean.agg(avg("Review_Score")).collect()

if len(avg_result) > 0 and avg_result[0][0] is not None:
    avg_score = avg_result[0][0]
else:
    avg_score = 0

review_stats = reviews_clean.groupBy("Review_Score") \
    .agg(count("*").alias("Count")) \
    .orderBy(asc("Review_Score"))

output.write("5. THONG KE DIEM DANH GIA\n")
output.write(f"Diem trung binh: {format(avg_score, '.2f')}\n\n")

output.write("So luong danh gia theo tung muc\n")

for row in review_stats.collect():
    output.write(f"{row['Review_Score']}: {row['Count']}\n")

output.write("\n")

# cau 6
revenue_2024 = orders.join(order_items, "Order_ID") \
    .join(products, "Product_ID") \
    .withColumn("Order_Date", to_timestamp("Order_Purchase_Timestamp")) \
    .filter(year("Order_Date") == 2024) \
    .withColumn("Revenue", col("Price") + col("Freight_Value")) \
    .groupBy("Product_Category_Name") \
    .agg(sum("Revenue").alias("Total_Revenue")) \
    .orderBy(desc("Total_Revenue"))

output.write("6. DOANH THU NAM 2024 THEO DANH MUC SAN PHAM\n")
for row in revenue_2024.collect():
    output.write(f"{row['Product_Category_Name']}: {format(row['Total_Revenue'], '.2f')}\n")
output.close()
spark.stop()
