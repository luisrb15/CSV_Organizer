variable "aws_region" {
    description = "The AWS region the resources will be deployed to"
    default = "us-east-1"  
}

variable "role"{
    description = "ARN of the role to assume"
    default = "arn:aws:iam::077492956248:role/service-role/AWSGlueServiceRole-athena"
}

variable "database_name" {
    description = "The name of the database to use for the crawler"
    default = "bootcamps-database"
}

variable "crawler_name" {
    description = "The name of the crawler"
    default = "bootcamps-crawler"
}

variable "s3_target_path" {
    description = "The S3 path to use for the crawler"
    default = "s3://bootcamps-2022-results/modified"
}

variable "prefix" {
    description = "The prefix to use for the crawler"
    default = "processed_"
}

variable "update" {
    description = "The update behavior for the crawler"
    default = "UPDATE_IN_DATABASE"
}

variable "delete" {
    description = "The delete behavior for the crawler"
    default = "DEPRECATE_IN_DATABASE"
}

variable "recrawl" {
    description = "The recrawl behavior for the crawler"
    default = "CRAWL_EVERYTHING"
}