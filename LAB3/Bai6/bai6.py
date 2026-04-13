from pyspark import SparkContext
import os
import shutil
from datetime import datetime

sc = SparkContext(appName="TimeAnalysis")

def get_year(timestamp):
    return datetime.fromtimestamp(int(timestamp)).year

ratings1 = sc.textFile("ratings_1.txt")
ratings2 = sc.textFile("ratings_2.txt")

ratings = ratings1.union(ratings2) \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 4) \
    .map(lambda x: (get_year(x[3]), (float(x[2]), 1)))

year_sum_count = ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

year_avg = year_sum_count.map(
    lambda x: (x[0], (round(x[1][0] / x[1][1], 2), x[1][1]))
)

final = year_avg.map(
    lambda x: f"{x[0]} | {x[1][0]} | {x[1][1]}"
).sortBy(lambda x: x)

output_tmp = "output_bai6_tmp"
output_final = "output_bai6.txt"

if os.path.exists(output_tmp):
    shutil.rmtree(output_tmp)

final.coalesce(1).saveAsTextFile(output_tmp)

for file in os.listdir(output_tmp):
    if file.startswith("part-"):
        shutil.move(os.path.join(output_tmp, file), output_final)

shutil.rmtree(output_tmp)

sc.stop()
