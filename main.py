import boto3
import datetime as dt
import logging

# from regex import F
from FileManager import FileManager
import re
logger = logging.getLogger()
logger.setLevel(logging.INFO)

year = dt.datetime.now().year
month = dt.datetime.now().month
day = dt.datetime.now().day

original_s3_bucket = 'bootcamps-2022'
destination_s3_bucket = 'bootcamps-2022-results'

s3 = boto3.client("s3")

objects_original_list = s3.list_objects(Bucket=original_s3_bucket)["Contents"]
file_manager = FileManager(original_s3_bucket, s3)

def ObtainFileNames(last_uploaded_keys):
    file_names = []
    for key in last_uploaded_keys:
        file_names.append(key.split("/")[-1])
    logger.info("File names: " + str(file_names))
    return file_names

def ListBootamps(file_names):
    destination_folder = 'Bootcamps/'
    destination_file_name = "list.csv"
    combined_file_name = destination_folder + destination_file_name
    header = "Bootcamp name, Initial date, End date"
    objects_key_list = []
    objects_list = s3.list_objects(Bucket=destination_s3_bucket)["Contents"]
    for objects in objects_list:
        objects_key_list.append(objects["Key"])    
    for file in file_names:        
        file_name_list = file.split("_")
        if combined_file_name in objects_key_list:
            file = s3.get_object(Bucket=destination_s3_bucket, Key= destination_folder + destination_file_name)['Body'].read().decode("utf-8")
            rowslist = file.split("\n")[1:]
            new_row = f"{file_name_list[0]}, {file_name_list[1]}, {file_name_list[3].split('.')[0]}"
            if new_row not in rowslist:
                rowslist.append(new_row)
                rowslist.sort()
                rowslist.insert(0, header)
                bodylist = "\n".join(rowslist)
                logger.info("New bootcamp added to list: " + new_row)
                s3.put_object(Bucket=destination_s3_bucket, Key=destination_folder + destination_file_name, Body=bodylist)
            else: 
                logger.info("This bootcamp already exists in the list")
        else:
            new_row = f"{file_name_list[0]}, {file_name_list[1]}, {file_name_list[3].split('.')[0]}"
            body = header + "\n" + new_row
            s3.put_object(Bucket=destination_s3_bucket, Key=destination_folder + destination_file_name, Body=body)

def CSVProcessor(keys, file_names):
    logger.info("Started CSVProcessor")
    number_of_file = 0
    for key in keys:
        obj = s3.get_object(Bucket=original_s3_bucket, Key= key) # get last object in s3 bucket
        file_name = file_names[number_of_file]
        number_of_file += 1
            
        body = obj["Body"].read().decode("utf-8")
        body = body.replace("\n\n", " ")
        body = body.replace("%", "")
        body = re.sub(r'"([^"]*)"', lambda m: m.group(1).replace('\n', ' ').replace(',', ' '), body)
        rows = body.split("\n")
        for row in rows:
            if row.count(",") == len(row)-1:
                rows.remove(row)
                break
        header = rows[0]
        rows.remove(header)
        body = AddColumn(file_name, header, rows)
        destination_key = f"{year}/{month}/{day}/{file_name}" # create new key

        destination_body = body
        s3.put_object(Bucket=destination_s3_bucket, Key=f'modified/{file_name}', Body=destination_body)
        s3.put_object(Bucket=destination_s3_bucket, Key=destination_key, Body=destination_body)

def AddColumn(file_name, header, rows):
    header = f'{header},Bootcamp name'
    bootcamp_name = file_name.split("_")[0]
    aux = 0
    for row in rows:
        row = row.replace("\n","")
        newRow = f'{row},{bootcamp_name}'
        rows.remove(row)
        rows.insert(aux, newRow)
        aux += 1
    body = header + "\n" + "\n".join(rows)
    return body

def main():
    keys = file_manager.get_all_csvs('ATBootcamp')
    file_names = ObtainFileNames(keys)
    CSVProcessor(keys, file_names)
    ListBootamps(file_names)

def lambda_handler(event, context):
    main()
    return "Success"

# lambda_handler(None, None)