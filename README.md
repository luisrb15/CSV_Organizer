## CSV Organizer for S3 Bucket 

This code allows you to take the last CSV file in a S3 bucket, process it and copy into a new S3 bucket.
Can be used in a Lambda function, triggered by the upload of a new CSV file.
Also, creates a new CSV file which contains a list of courses and dates. Files must be uploaded to the original s3 bucket with the format YOURCOURSE_INITIALDATE_ENDDATE.csv
