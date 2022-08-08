provider "aws" {
  region = var.aws_region
}

resource "aws_glue_crawler" "bootcamps_crawler" {
    database_name = var.database_name
    name = var.crawler_name

    role = var.role

    table_prefix = var.prefix

    configuration = jsonencode(
        {
            Grouping = {
                TableGroupingPolicy = "CombineCompatibleSchemas"
            }
            Version = 1
        }
    ) 

    s3_target {
        path = var.s3_target_path
    }

    schema_change_policy {
        update_behavior = var.update
        delete_behavior = var.delete
    }

    recrawl_policy {
        recrawl_behavior = var.recrawl
    }
}