# Luis here is code that we did for the modified csv file I forgot to upload it too

import json
import boto3
import datetime as dt
import re

original_s3_bucket = 'devopslatam02-result-kc-2'
destination_s3_bucket = (original_s3_bucket, "kcfiles/")
s3 = boto3.client("s3")
year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day

csv_list = []
for key in s3.list_objects(Bucket=original_s3_bucket)["Contents"]:
    if re.search("^kcfiles/ATBootcamp.*$", key["Key"]):
        csv_list.append(key['Key'])
        last_modified = key

var1 = s3.list_objects(Bucket=original_s3_bucket)["Contents"][-1]

varKey = csv_list[1]

obj = s3.get_object(Bucket=original_s3_bucket, Key=varKey)

file_name = varKey.split("/")[-1]

modified_file_name = "Modified" + file_name

destination_key = destination_s3_bucket[1] + "results/" + f"{year}/{month}/{day}/{modified_file_name}"
print(original_s3_bucket)
s3.put_object(Bucket=original_s3_bucket, Key=destination_key, Body="test")
