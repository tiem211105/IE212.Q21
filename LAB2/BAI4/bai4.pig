raw = LOAD 'hotel-review.csv'
USING PigStorage(';')
AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

data = FILTER raw BY id != 'id';

stopwords = LOAD 'stopwords.txt'
USING PigStorage()
AS (word:chararray);

tokens = FOREACH data GENERATE
    sentiment,
    FLATTEN(TOKENIZE(LOWER(review))) AS word;

tokens_clean = FILTER tokens BY word IS NOT NULL;

joined = JOIN tokens_clean BY word LEFT OUTER, stopwords BY word;

filtered = FILTER joined BY stopwords::word IS NULL;

words = FOREACH filtered GENERATE
    tokens_clean::sentiment AS sentiment,
    tokens_clean::word AS word;

grp = GROUP words BY (sentiment, word);

word_count = FOREACH grp GENERATE
    group.sentiment AS sentiment,
    group.word AS word,
    COUNT(words) AS cnt;

pos = FILTER word_count BY sentiment == 'positive';
neg = FILTER word_count BY sentiment == 'negative';

pos_sorted = ORDER pos BY cnt DESC;
neg_sorted = ORDER neg BY cnt DESC;

top5_pos = LIMIT pos_sorted 5;
top5_neg = LIMIT neg_sorted 5;

STORE top5_pos INTO 'output_bai4_pos' USING PigStorage(',');
STORE top5_neg INTO 'output_bai4_neg' USING PigStorage(',');

DUMP top5_pos;
DUMP top5_neg;
