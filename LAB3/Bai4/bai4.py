from pyspark import SparkContext
import os
import shutil

sc = SparkContext(appName="AgeGroupAnalysis")

def get_age_group(age):
    age = int(age)
    if age < 18:
        return "Under 18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 45:
        return "36-45"
    elif age <= 55:
        return "46-55"
    else:
        return "56+"

users = sc.textFile("users.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 3) \
    .map(lambda x: (x[0], get_age_group(x[2])))

ratings = sc.textFile("ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 3) \
    .map(lambda x: (x[0], (x[1], float(x[2]))))

movies = sc.textFile("movies.txt") \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) >= 2) \
    .map(lambda x: (x[0], x[1]))

user_ratings = users.join(ratings)

movie_age_rating = user_ratings.map(
    lambda x: ((x[1][1][0], x[1][0]), (x[1][1][1], 1))
)

movie_age_avg = movie_age_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

movie_age_avg = movie_age_avg.map(
    lambda x: (x[0], round(x[1][0] / x[1][1], 2))
)

final = movie_age_avg.map(
    lambda x: (x[0][0], (x[0][1], x[1]))
).join(movies)

final = final.map(
    lambda x: f"{x[0]} | {x[1][1]} | {x[1][0][0]} | {x[1][0][1]}"
).sortBy(lambda x: x)

output_tmp = "output_bai4_tmp"
output_final = "output_bai4.txt"

if os.path.exists(output_tmp):
    shutil.rmtree(output_tmp)

final.coalesce(1).saveAsTextFile(output_tmp)

for file in os.listdir(output_tmp):
    if file.startswith("part-"):
        shutil.move(os.path.join(output_tmp, file), output_final)

shutil.rmtree(output_tmp)

sc.stop()
