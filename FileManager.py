import re


class FileManager:

    def __init__(self, original_s3_bucket, s3):
        self.original_s3_bucket = original_s3_bucket
        self.s3 = s3

    def get_all_modified(self):
        csv_list = []
        for key in self.s3.list_objects(Bucket=self.original_s3_bucket + "results/")["Contents"]:
            if re.search("^Modified.*$", key["Key"]):
                csv_list.append(key['Key'])
        return csv_list

    def get_all_csvs(self, prefix):
        csv_list = []
        for key in self.s3.list_objects(Bucket=self.original_s3_bucket)["Contents"]:
            if re.search("^" + prefix + ".*$", key["Key"]):
                csv_list.append(key['Key'])

        return csv_list

    def get_body_with_format(self, csv_key):
        obj = self.s3.get_object(Bucket=self.original_s3_bucket, Key=csv_key)
        body = obj["Body"].read().decode("utf-8")
        return body

    def get_csv_replaced_with_param(self, csv, param):
        body = self.get_body_with_format(csv)
        matches = re.finditer(r"\"([^\"]*)\"", body, re.MULTILINE)
        body_with_semicolon = []
        for matchNum, match in enumerate(matches, start=1):
            for groupNum in range(0, len(match.groups())):
                groupNum = groupNum + 1
                body_with_semicolon.append(re.sub(',', param, match.group(groupNum)))
        return body_with_semicolon

    def get_csv_fixed(self, csv, param):

        array = self.get_csv_replaced_with_param(csv, param)
        body = self.get_body_with_format(csv)
        body_with_text_removed = re.sub(r"\"([^\"]*)\"", '', body)
        array_body_with_text_removed = body_with_text_removed.split(',')
        for arrayvar in array:
            for index, var_new_body in enumerate(array_body_with_text_removed):
                if index > 23:
                    if var_new_body == '':
                        array_body_with_text_removed[index] = arrayvar
                        break
        result = ",".join(array_body_with_text_removed)
        return result

