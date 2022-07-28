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

objects_original_list = s3.list_objects(Bucket=original_s3_bucket)["Contents"]

def LastModifiedFiles():
    utc_now = dt.datetime.utcnow()
    utc_minus_30 = utc_now - dt.timedelta(seconds=30)
    last_uploaded_keys = []
    for object in objects_original_list:
        object_creation = object['LastModified']
        object_creation = object_creation.replace(tzinfo=None)
        if object_creation > utc_minus_30:
            last_uploaded_keys.append(object['Key'])
    logger.info('Last uploaded keys: ' + str(last_uploaded_keys))
    return last_uploaded_keys

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

def CSVProcessor(last_uploaded_keys, file_names):
    logger.info("Started CSVProcessor")
    number_of_file = 0
    for key in last_uploaded_keys:
        obj = s3.get_object(Bucket=original_s3_bucket, Key= key) # get last object in s3 bucket
        file_name = file_names[number_of_file]
        number_of_file += 1
            
        body = obj["Body"].read().decode("utf-8")
        body = body.replace("\n\n", " ")
        body = body.replace("%", "")
        rows = body.split("\n")
        for row in rows:
            if row.count(",") == len(row)-1:
                rows.remove(row)
                break
        header = rows[0]
        rows.remove(header)

        destination_key = f"{year}/{month}/{day}/{file_name}" # create new key

        destination_body = header + "\n" + "\n".join(rows) # join header and rows

        s3.put_object(Bucket=destination_s3_bucket, Key=destination_key, Body=destination_body)


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
    last_uploaded_keys = LastModifiedFiles()
    file_names = ObtainFileNames(last_uploaded_keys)
    CSVProcessor(last_uploaded_keys, file_names)
    ListBootamps(file_names)
    # create_table(file_name, header)

def lambda_handler(event, context):
    main()
    return "Success"

# lambda_handler(None, None)