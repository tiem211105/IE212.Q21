reviews = LOAD '/user/tiem23521566/hotel-review.csv'
USING PigStorage(';')
AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

data = FILTER reviews BY id != 'id';

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

neg = FILTER clean_data BY sentiment == 'negative';

group_neg = GROUP neg BY aspect;

count_neg = FOREACH group_neg GENERATE
    group AS aspect,
    COUNT(neg) AS total;

sorted_neg = ORDER count_neg BY total DESC;

top_neg = LIMIT sorted_neg 1;

pos = FILTER clean_data BY sentiment == 'positive';

group_pos = GROUP pos BY aspect;

count_pos = FOREACH group_pos GENERATE
    group AS aspect,
    COUNT(pos) AS total;

sorted_pos = ORDER count_pos BY total DESC;

top_pos = LIMIT sorted_pos 1;

neg_labeled = FOREACH top_neg GENERATE
    'NEGATIVE_MAX' AS type,
    aspect,
    total;

pos_labeled = FOREACH top_pos GENERATE
    'POSITIVE_MAX' AS type,
    aspect,
    total;

final = UNION neg_labeled, pos_labeled;

final_single = GROUP final ALL;
final_flat = FOREACH final_single GENERATE FLATTEN(final);

STORE final_flat INTO '/user/tiem23521566/output_bai3' USING PigStorage('\t');
