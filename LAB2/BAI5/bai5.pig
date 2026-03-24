raw = LOAD 'hotel-review.csv' 
      USING PigStorage(';') 
      AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

data = FILTER raw BY id != 'id';

tokens = FOREACH data GENERATE 
            sentiment, 
            FLATTEN(TOKENIZE(LOWER(review))) AS word;

tokens_clean = FILTER tokens BY word IS NOT NULL;

grp = GROUP tokens_clean BY (sentiment, word);

word_count = FOREACH grp GENERATE 
                group.sentiment AS sentiment, 
                group.word AS word, 
                COUNT(tokens_clean) AS cnt;

pos = FILTER word_count BY sentiment == 'positive';
neg = FILTER word_count BY sentiment == 'negative';

pos_sorted = ORDER pos BY cnt DESC;
neg_sorted = ORDER neg BY cnt DESC;

pos_top5 = LIMIT pos_sorted 5;
neg_top5 = LIMIT neg_sorted 5;

STORE pos_top5 INTO 'output_bai5_pos' USING PigStorage(',');
STORE neg_top5 INTO 'output_bai5_neg' USING PigStorage(',');
