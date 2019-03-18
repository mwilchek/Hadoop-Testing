igrams = LOAD 'i-bigrams/*' AS (bigram:chararray, year:int, count:int, books:int);

group_gram = GROUP igrams BY bigram;

avg_group_gram = FOREACH group_gram{
GENERATE group AS bigram,
SUM(igrams.count) AS total_occurrance,
SUM(igrams.books) AS total_books,
(double)SUM(igrams.count)/(double)SUM(igrams.books) AS avg,
MIN (igrams.year) AS start,
MAX (igrams.year) AS end,
COUNT(igrams) as num_years;
}

orderd_avg_group_gram = ORDER avg_group_gram BY avg DESC, bigram;

subset_gram = FILTER orderd_avg_group_gram BY (start == 1950 AND num_years == 60);

subset_gram_one = FILTER subset_gram BY end == 2009;

top_ordered_avg_group_gram = LIMIT subset_gram_one 50;

STORE top_ordered_avg_group_gram INTO 'pig_Output/' USING PigStorage (',');
