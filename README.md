# LambdaDataProcesser
This Lambda function is to be kicked off upon the uploading of a JSON object to an S3 bucket containing columnar data from a database.

  1. JSON table data is dropped in an S3 bucket (e.g., "Staging Bucket")
  
  2. Lambda checks AWS Parameter Store to ensure object has associated metadata tags
  
  3. If so, store tags in memory for later use. If not, move object to an S3 "Exile Bucket" and publish to an SNS topic which notifies subscribers of missing tags.
  
  4. Upload file contents to staging table in Redshift and call an upsert procedure to capture incremental changes to the table.
  
  5. Compress the original JSON file to parquet, partition by "CreatedDate", add metadata tags, and finally copy to final S3 "Processed Bucket" before deleting 
  the file from staging.
  
This process enables end users to access their data via a data warehouse (RedShift), or to analyze the parquet files in S3 (Athena, Amazon ML).
