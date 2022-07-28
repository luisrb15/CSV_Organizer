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
athena_client = boto3.client('athena')

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
    logger.info("Started CSVProcessor")
    last_modified = s3.list_objects(Bucket=original_s3_bucket)["Contents"][-1]
    for key in s3.list_objects(Bucket=original_s3_bucket)["Contents"]:
        if key["LastModified"] > last_modified["LastModified"]:
            last_modified = key
    last_modified_key = last_modified["Key"]

    obj = s3.get_object(Bucket=original_s3_bucket, Key= last_modified_key) # get last object in s3 bucket
    file_name = last_modified_key.split("/")[-1] # get last part of key
        
    body = obj["Body"].read().decode("utf-8")
    header = body.split("\n")[0]
    body = body.replace("\n\n", " ")
    body = body.replace("\n", " ")
    body = body.replace("%", "")
    rows = body.split("\n")[1:]

    destination_key = f"{year}/{month}/{day}/{file_name}" # create new key

    destination_body = header + "\n" + "\n".join(rows) # join header and rows

    s3.put_object(Bucket=destination_s3_bucket, Key=destination_key, Body=destination_body)
    return file_name, header

# def create_table(file_name, header):
#     file_name_list = file_name.split("_")
#     first_file_name = file_name_list[0]
    
#     header_list = []
#     for each_header in header:
#         each_header = each_header + " STRING"
#         header_list.append(each_header)
#     print('Header list: ' + str(header_list))
#     query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {first_file_name} ( 
#         {", ".join(header_list)}
#     )
#     ROW FORMAT DELIMITED
#     FIELDS TERMINATED BY ','
#     STORED AS TEXTFILE
#     LOCATION 's3://bootcamps-2022-results/{year}/{month}/{day}/{file_name}'''"""
#     print ('Query: ' + query)
#     athena_client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': 'studentsexample'}, ResultConfiguration={'OutputLocation': 's3://bootcamps-2022-results'})
    



def main():
    CSVProcessorReturn = CSVProcessor()
    file_name = CSVProcessorReturn[0]
    header = CSVProcessorReturn[1].split(",") 
    print('File name: ' + file_name)
    ListBootamps(file_name)
    # create_table(file_name, header)

def lambda_handler(event, context):
    main()
    return "Success"

# lambda_handler(None, None)