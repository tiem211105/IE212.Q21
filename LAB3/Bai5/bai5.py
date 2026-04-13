from pyspark import SparkContext
import os
import shutil

sc = SparkContext(appName="OccupationAnalysis")

users = sc.textFile("users.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 4) \
    .map(lambda x: (x[0], x[3]))

occupations = sc.textFile("occupation.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 2) \
    .map(lambda x: (x[0], x[1]))

ratings = sc.textFile("ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 3) \
    .map(lambda x: (x[0], float(x[2])))

user_ratings = users.join(ratings)

occ_rating = user_ratings.map(
    lambda x: (x[1][0], (x[1][1], 1))
)

occ_sum_count = occ_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

occ_avg = occ_sum_count.map(
    lambda x: (x[0], (round(x[1][0] / x[1][1], 2), x[1][1]))
)

final = occ_avg.join(occupations)

final = final.map(
    lambda x: f"{x[1][1]} | {x[1][0][0]} | {x[1][0][1]}"
).sortBy(lambda x: x)

output_tmp = "output_bai5_tmp"
output_final = "output_bai5.txt"

if os.path.exists(output_tmp):
    shutil.rmtree(output_tmp)

final.coalesce(1).saveAsTextFile(output_tmp)

for file in os.listdir(output_tmp):
    if file.startswith("part-"):
        shutil.move(os.path.join(output_tmp, file), output_final)

shutil.rmtree(output_tmp)

sc.stop()
