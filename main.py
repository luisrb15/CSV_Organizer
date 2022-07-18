import boto3
import datetime as dt

year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day

original_s3_bucket = ## YOUR S3 BUCKET HERE ##
destination_s3_bucket = ## YOUR DESTINATION S3 BUCKET HERE ##

s3 = boto3.client("s3")

last_modified = s3.list_objects(Bucket=original_s3_bucket)["Contents"][0]
for key in s3.list_objects(Bucket=original_s3_bucket)["Contents"]:
    if key["LastModified"] > last_modified["LastModified"]:
        last_modified = key
last_modified_key = last_modified["Key"]

obj = s3.get_object(Bucket=original_s3_bucket, Key= last_modified_key) # get last object in s3 bucket
file_name = last_modified_key.split("/")[-1] # get last part of key
    

body = obj["Body"].read().decode("utf-8")
body = body.replace("\n\n", " ")
body = body.replace("\n", " ")
header = body.split("\n")[0]
rows = body.split("\n")[1:]

destination_key = f"{year}/{month}/{day}/{file_name}" # create new key

destination_body = header + "\n" + "\n".join(rows) # join header and rows

s3.put_object(Bucket=destination_s3_bucket, Key=destination_key, Body=destination_body)