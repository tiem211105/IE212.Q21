from pyspark import SparkContext

sc = SparkContext(appName="MovieRating")

movies = sc.textFile("movies.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda x: (x[0].strip(), x[1].strip())) \
    .collectAsMap()

movies_broadcast = sc.broadcast(movies)

ratings = sc.textFile("ratings_1.txt") \
    .union(sc.textFile("ratings_2.txt")) \
    .map(lambda line: line.split(",")) \
    .map(lambda x: (x[1].strip(), float(x[2].strip())))

stats = ratings \
    .mapValues(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

result = stats.map(lambda x: (
    x[0],
    (
        x[1][0] / x[1][1],
        x[1][1],
        movies_broadcast.value.get(x[0], "Unknown")
    )
))

filtered = result.filter(lambda x: x[1][1] >= 50)

best_movie = filtered.takeOrdered(1, key=lambda x: -x[1][0])

with open("output_bai1.txt", "w", encoding="utf-8") as f:
    f.write("MovieID | Title | Avg | Count\n")

    for movie_id, (avg, cnt, title) in result.collect():
        f.write(f"{movie_id} | {title} | {avg:.2f} | {cnt}\n")

    f.write("\nBest movie (>=50 ratings):\n")
    if best_movie:
        movie_id, (avg, cnt, title) = best_movie[0]
        f.write(f"{movie_id} | {title} | {avg:.2f} | {cnt}\n")
    else:
        f.write("No movie has >= 50 ratings\n")

sc.stop()
