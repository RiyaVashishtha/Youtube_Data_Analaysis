
# joining the tables
SELECT * FROM "de_youtube_raw"."raw_statistics" a
inner join 
"de_youtube_raw"."cleaned_statistics_reference_data" b on a.category_id = b.id;

# create analytical database
CREATE DATABASE db_youtube_analytics;

# analtytical query
SELECT * FROM "AwsDataCatalog"."db_youtube_analytics"."final-analytics" limit 10;
