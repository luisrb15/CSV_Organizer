import boto3
import datetime as dt
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day

original_s3_bucket = 'bootcamps-2022'
destination_s3_bucket = 'bootcamps-2022-results'

s3 = boto3.client("s3")

def ListBootamps(file_name):
    destination_folder = 'Bootcamps/'
    destination_file_name = "list.csv"
    combined_file_name = destination_folder + destination_file_name
    file_name_list = file_name.split("_")
    header = "Bootcamp name, Initial date, End date"

    objects_key_list = []
    objects_list = s3.list_objects(Bucket=destination_s3_bucket)["Contents"]
    for objects in objects_list:
        objects_key_list.append(objects["Key"])    

    if combined_file_name in objects_key_list:
        file = s3.get_object(Bucket=destination_s3_bucket, Key= destination_folder + destination_file_name)['Body'].read().decode("utf-8")
        rowslist = file.split("\n")[1:]
        new_row = f"{file_name_list[0]}, {file_name_list[1]}, {file_name_list[3].split('.')[0]}"
        if new_row not in rowslist:
            rowslist.append(new_row)
            rowslist.sort()
            rowslist.insert(0, header)
            bodylist = "\n".join(rowslist)
            s3.put_object(Bucket=destination_s3_bucket, Key=destination_folder + destination_file_name, Body=bodylist)
        else: 
            logger.info("This bootcamp already exists in the list")
    else:
        new_row = f"{file_name_list[0]}, {file_name_list[1]}, {file_name_list[3].split('.')[0]}"
        body = header + "\n" + new_row
        s3.put_object(Bucket=destination_s3_bucket, Key=destination_folder + destination_file_name, Body=body)

def CSVProcessor():
    last_modified = s3.list_objects(Bucket=original_s3_bucket)["Contents"][-1]
    for key in s3.list_objects(Bucket=original_s3_bucket)["Contents"]:
        if key["LastModified"] > last_modified["LastModified"]:
            last_modified = key
    last_modified_key = last_modified["Key"]

    obj = s3.get_object(Bucket=original_s3_bucket, Key= last_modified_key) # get last object in s3 bucket
    file_name = last_modified_key.split("/")[-1] # get last part of key
        
    body = obj["Body"].read().decode("utf-8")
    body = body.replace("\n\n", " ")
    body = body.replace("\n", " ")
    body = body.replace("%", "")
    header = body.split("\n")[0]
    rows = body.split("\n")[1:]

    destination_key = f"{year}/{month}/{day}/{file_name}" # create new key

    destination_body = header + "\n" + "\n".join(rows) # join header and rows

    s3.put_object(Bucket=destination_s3_bucket, Key=destination_key, Body=destination_body)
    return file_name

def main():
    file_name = CSVProcessor()
    ListBootamps(file_name)

def lambda_handler(event, context):
    main()
    return "Success"