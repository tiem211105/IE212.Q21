from pyspark import SparkContext

sc = SparkContext(appName="GenreAnalysis")

movies = sc.textFile("movies.txt")

movie_genres = (
    movies
    .filter(lambda x: len(x.split(",")) >= 3)
    .map(lambda x: x.split(","))
    .map(lambda x: (x[0], x[2].split("|")))   # (MovieID, [Genres])
)

movie_genres_dict = dict(movie_genres.collect())
bc_movie_genres = sc.broadcast(movie_genres_dict)

ratings = sc.textFile("ratings_1.txt,ratings_2.txt")

ratings_data = (
    ratings
    .filter(lambda x: len(x.split(",")) >= 3)
    .map(lambda x: x.split(","))
    .map(lambda x: (x[1], float(x[2])))   # (MovieID, Rating)
)

genre_ratings = ratings_data.flatMap(
    lambda x: [(g, x[1]) for g in bc_movie_genres.value.get(x[0], [])]
)

genre_avg = (
    genre_ratings
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1])
)

with open("output_bai2.txt", "w") as f:
    f.write("Genre | AvgRating\n")
    for genre, avg in sorted(genre_avg.collect()):
        f.write(f"{genre} | {avg:.2f}\n")

sc.stop()
