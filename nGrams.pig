-- @author : Aman
-- We are drowning in information while starving for wisdom(insight) - E. O. Wilson

-- Declare the variables related to the location of the files.
%declare VERY_SMALL_DATA_FILE_DIR 'very_small_avro_to_test/'
%declare ONE_MILLION_DATA_FILE_DIR 'one_million/'
%declare FULL_REVIEWS_DATA_FILE_DIR 'full_reviews/'

%declare CURRENT_DATA_FOLDER '$FULL_REVIEWS_DATA_FILE_DIR'

-- Declare a varible for data file
%declare DATA_FILES 's3://amazon-reviews-mbd/avro/$CURRENT_DATA_FOLDER'

sh echo $DATA_FILES
sh echo $CURRENT_DATA_FOLDER


-- The data loading part
reviews = LOAD '$DATA_FILES' USING AvroStorage AS 	
 (marketplace:chararray, 
  customer_id:chararray, 
  review_id:chararray, 
  product_id:chararray, 
  product_parent:chararray, 
  product_title:chararray, 
  star_rating:int, 
  helpful_votes:int, 
  total_votes:int, 
  vine:chararray, 
  verified_purchase:chararray, 
  review_headline:chararray, 
  review_body:chararray, 
  review_date:long, 
  year:int,
  product_category:chararray);

-- Data processing started. 
top_product_parents = group reviews by product_parent;
product_parent_counts = foreach top_product_parents { 
    unique_products = DISTINCT reviews.product_id;
    generate group, COUNT(unique_products) as products_cnt;
};
DESCRIBE product_parent_counts;
top_product_parents = ORDER product_parent_counts BY products_cnt DESC;

top_product_parent_tuple = LIMIT top_product_parents 1;
top_product_parent = foreach top_product_parent_tuple GENERATE $0;

reviews_of_product_parent = FILTER reviews BY product_parent == top_product_parent.$0;

products = FOREACH reviews_of_product_parent GENERATE product_title;
products_we_analyze = GROUP products BY product_title;
products_we_analyze_counts = foreach products_we_analyze { 
    unique_products = DISTINCT products.product_title;
    generate group, COUNT(unique_products) as products_title_cnt;
};
DESCRIBE products_we_analyze_counts;
-- DUMP products_we_analyze_counts;

STORE products_we_analyze_counts INTO 's3://amazon-reviews-mbd/pig/$CURRENT_DATA_FOLDER/products_we_analyze/' using PigStorage(',');

-- Download the jars using wget
REGISTER hdfs://ip-172-31-41-207.us-east-2.compute.internal:8020/user/hadoop/tutorial.jar

reviews_lowered = FOREACH reviews_of_product_parent GENERATE org.apache.pig.tutorial.ToLower(review_body) as review;

ngramed_reviews = FOREACH reviews_lowered GENERATE flatten(org.apache.pig.tutorial.NGramGenerator(review)) as ngram;

ngramed_reviews_valid = FILTER ngramed_reviews BY SIZE(ngram) > 9;

ngrams_tokenize = foreach ngramed_reviews_valid Generate TOKENIZE(ngram) as tokenized_ngram;

tknz = FOREACH ngrams_tokenize GENERATE FLATTEN(tokenized_ngram) as Col_Words;
tknz_group = GROUP tknz by Col_Words;
tknz_count = FOREACH tknz_group GENERATE group AS word, COUNT(tknz.Col_Words) AS word_freq;

tknz_count = FILTER tknz_count BY SIZE(word) > 4;
tknz_count_ordered = ORDER tknz_count BY word_freq DESC;
top_words_used = LIMIT tknz_count_ordered 150;
-- dump top_words_used;

STORE top_words_used INTO 's3://amazon-reviews-mbd/pig/$CURRENT_DATA_FOLDER/top_words/' using PigStorage(',');

-- two grams
two_grams = FILTER ngrams_tokenize BY SIZE(tokenized_ngram) > 1;
two_grams_limit = LIMIT two_grams 150;
-- dump two_grams_limit;

STORE two_grams_limit INTO 's3://amazon-reviews-mbd/pig/$CURRENT_DATA_FOLDER/few_two_grams/' using PigStorage(',');

