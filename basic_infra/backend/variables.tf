variable "region"{
	type    = string
	default = "us-west-1"
}

variable "s3_bucket_name"{
	type    = string
	default = "trading-mage-infrastructure-state-bucket"
}

variable "dynamodb_table_name"{
	type    = string
	default = "trading-mage-terraform-lock"
}