-- @author : Aman
-- We are drowning in information while starving for wisdom(insight) - E. O. Wilson

-- The data creation scripts should be run in Amazon Athena.

-- Create a big table in Amazon Athena, the data is loaded from free bucket of S3 in parquet format
CREATE EXTERNAL TABLE all_amazon_reviews(
  marketplace string, 
  customer_id string, 
  review_id string, 
  product_id string, 
  product_parent string, 
  product_title string, 
  star_rating int, 
  helpful_votes int, 
  total_votes int, 
  vine string, 
  verified_purchase string, 
  review_headline string, 
  review_body string, 
  review_date bigint, 
  year int)
PARTITIONED BY (product_category string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://amazon-reviews-pds/parquet/'

-- This should be run with above table creation. This has # 160796570
MSCK REPAIR TABLE all_amazon_reviews


-- Creates a million sample of amazon reviews and stores in S3 # This has 110528317
CREATE TABLE sampledb.full_amazon_reviews
 AS
SELECT DISTINCT 
    * 
FROM all_amazon_reviews
WHERE 
    marketplace = 'US' 
    AND 
    length(review_body) > 100 
    AND 
    length(review_body) < 5000


-- Creates a million sample of amazon reviews and stores in S3
CREATE TABLE sampledb.avro_small_db
WITH (
  format='TEXTFILE',
  external_location='s3://amazon-reviews-mbd/avro/'
) AS
SELECT 
    * 
FROM full_amazon_reviews  TABLESAMPLE BERNOULLI (0.01)  -- 1 = 1% = 1 Million


-- Creates a million sample of amazon reviews and stores in S3
CREATE TABLE sampledb.avro_full_db
WITH (
  format='TEXTFILE',
  external_location='s3://amazon-reviews-mbd/avro_full_db/'
) AS
SELECT 
    * 
FROM full_amazon_reviews  -- 1 = 1% = 1 Million


-- Creates a partitioned table of all amazon reviews and stores in S3, duplicates and invalid reviews are removed.
DROP TABLE IF EXISTS sampledb.full_amazon_reviews
CREATE TABLE sampledb.full_amazon_reviews
WITH (
  format='TEXTFILE',
  external_location='s3://amazon-reviews-mbd/csvs/very_small_sample_to_test/'
) AS
FROM all_amazon_reviews






-- Creating Avro files in S3
CREATE TABLE sampledb.avro_small_db
WITH (
  format='AVRO',
  external_location='s3://amazon-reviews-mbd/avro/very_small_avro_to_test'
) AS
SELECT 
    * 
FROM all_amazon_reviews  TABLESAMPLE BERNOULLI (0.01)


-- One million to Avro
CREATE TABLE sampledb.avro_one_million_db
WITH (
  format='AVRO',
  external_location='s3://amazon-reviews-mbd/avro/one_million/'
) AS
SELECT DISTINCT 
    * 
FROM all_amazon_reviews  TABLESAMPLE BERNOULLI (1)