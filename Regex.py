import boto3
import datetime as dt
from BucketS3 import BucketS3
import re

year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day

original_s3_bucket = BucketS3.original_s3_bucket
destination_s3_bucket = BucketS3.destination_s3_bucket
s3 = boto3.client("s3")
last_modified = s3.list_objects(Bucket=original_s3_bucket)["Contents"][0]
csvlist = []
for key in s3.list_objects(Bucket=original_s3_bucket)["Contents"]:
    if re.search("^kcfiles/ATBootcamp.*$", key["Key"]):
        csvlist.append(key['Key'])
        last_modified = key
# print(csvlist)
last_modified_key = csvlist[1]
obj = s3.get_object(Bucket=original_s3_bucket, Key=last_modified_key)  # get last object in s3 bucket
file_name = last_modified_key.split("/")[-1]  # get last part of key
body = obj["Body"].read().decode("utf-8")
# varBody = re.search(r'\"([\s\S]*?)\"', body)
matches = re.finditer(r"\"([^\"]*)\"", body, re.MULTILINE)
array = []
newBody = re.sub(r"\"([^\"]*)\"", '', body)
for matchNum, match in enumerate(matches, start=1):
    for groupNum in range(0, len(match.groups())):
        groupNum = groupNum + 1
        array.append(re.sub(',', ';', match.group(groupNum)))
newBody2 = newBody.split(',')
lenNewBody2 = len(newBody2)
for arrayvar in array:
    for index, varNewBody in enumerate(newBody2):
        if(index>23):
            if varNewBody == '':
                newBody2[index] = arrayvar
                break
newBody3 = ",".join(newBody2)

body = body.replace("\n\n", " ")
body = body.replace("\n", " ")
header = body.split("\n")[0]
rows = body.split("\n")[1:]

destination_key = destination_s3_bucket[1] + "results/" + f"{year}/{month}/{day}/{file_name}"  # create new key
print(destination_key)
destination_body = header + "\n" + "\n".join(rows)  # join header and rows
# s3.put_object(Bucket=destination_s3_bucket[0], Key=destination_key, Body=destination_body)
