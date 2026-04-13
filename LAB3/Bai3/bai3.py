from pyspark import SparkContext
import shutil
import os

sc = SparkContext(appName="GenderAnalysis")

users = sc.textFile("users.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda x: (int(x[0]), x[1]))

ratings = sc.textFile("ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

joined = ratings.join(users)

movie_gender = joined.map(lambda x: (
    (x[1][0][0], x[1][1]),  # (movieID, gender)
    (x[1][0][1], 1)         # (rating, count)
))

reduced = movie_gender.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg = reduced.mapValues(lambda x: x[0] / x[1])

movies = sc.textFile("movies.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda x: (int(x[0]), x[1]))

temp = avg.map(lambda x: (x[0][0], (x[0][1], x[1])))

joined_movies = temp.join(movies)

final = joined_movies.map(lambda x: (
    x[1][1],        # movieName
    x[1][0][0],     # gender
    round(x[1][0][1], 2)  # avg rating
))

final_sorted = final.sortBy(lambda x: (x[0], x[1]))

output_dir = "output_tmp"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

final_sorted.map(lambda x: f"{x[0]} | {x[1]} | {x[2]}") \
    .coalesce(1) \
    .saveAsTextFile(output_dir)

for file in os.listdir(output_dir):
    if file.startswith("part-"):
        shutil.move(os.path.join(output_dir, file), "output_bai3.txt")

shutil.rmtree(output_dir)

sc.stop()
