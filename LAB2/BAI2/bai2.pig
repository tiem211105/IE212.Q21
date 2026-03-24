reviews = LOAD '/user/tiem23521566/hotel-review.csv'
USING PigStorage(';')
AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

data = FILTER reviews BY id != 'id';

lower_reviews = FOREACH data GENERATE LOWER(review) AS review;

words = FOREACH lower_reviews GENERATE
    FLATTEN(TOKENIZE(review)) AS word;

clean_words = FILTER words BY word IS NOT NULL AND word != '';

group_word = GROUP clean_words BY word;

count_word = FOREACH group_word GENERATE
    group AS word,
    COUNT(clean_words) AS freq;

sorted = ORDER count_word BY freq DESC;

top5 = LIMIT sorted 5;

STORE top5 INTO '/user/tiem23521566/output_top5' USING PigStorage('\t');

group_category = GROUP data BY category;

count_category = FOREACH group_category GENERATE
    group AS category,
    COUNT(data) AS total;

STORE count_category INTO '/user/tiem23521566/output_category' USING PigStorage('\t');

clean_data = FILTER data BY 
    aspect == 'GENERAL' OR
    aspect == 'QUALITY' OR
    aspect == 'PRICES' OR
    aspect == 'CLEANLINESS' OR
    aspect == 'COMFORT' OR
    aspect == 'LOCATION' OR
    aspect == 'FACILITIES' OR
    aspect == 'STYLE&OPTIONS' OR
    aspect == 'DESIGN&FEATURES' OR
    aspect == 'MISCELLANEOUS';

group_aspect = GROUP clean_data BY aspect;

count_aspect = FOREACH group_aspect GENERATE
    group AS aspect,
    COUNT(clean_data) AS total;

STORE count_aspect INTO '/user/tiem23521566/output_aspect' USING PigStorage('\t');
