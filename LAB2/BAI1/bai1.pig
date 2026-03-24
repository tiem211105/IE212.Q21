reviews = LOAD '/user/tiem23521566/hotel-review.csv'
USING PigStorage(';')
AS (id:int, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

lower_reviews = FOREACH reviews GENERATE id, LOWER(review) AS review;

words = FOREACH lower_reviews GENERATE id, FLATTEN(TOKENIZE(review)) AS word;

stopwords = LOAD '/user/tiem23521566/stopwords.txt'
USING PigStorage()
AS (stopword:chararray);

joined = JOIN words BY word LEFT OUTER, stopwords BY stopword;

filtered = FILTER joined BY stopword IS NULL;

result = FOREACH filtered GENERATE word;

DUMP result;
