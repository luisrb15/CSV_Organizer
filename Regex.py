import boto3
import datetime as dt
from BucketS3 import BucketS3
from FileManager import FileManager

year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day
original_s3_bucket = BucketS3.original_s3_bucket
destination_s3_bucket = BucketS3.destination_s3_bucket
s3 = boto3.client("s3")
file_manager = FileManager(original_s3_bucket, s3)
csv_list = file_manager.get_all_csvs('kcfiles/ATBootcamp')
for csv in csv_list:
    result = file_manager.get_csv_fixed(csv, ';')
    # body = body.replace("\n\n", " ")
    # body = body.replace("\n", " ")
    # header = body.split("\n")[0]
    # rows = body.split("\n")[1:]
    # file_name = csv.split("/")[-1]
    # destination_key = destination_s3_bucket[1] + "results/" + f"{year}/{month}/{day}/{file_name}"
    # destination_body = header + "\n" + "\n".join(rows)
    print(result)
# s3.put_object(Bucket=destination_s3_bucket[0], Key=destination_key, Body=destination_body)
