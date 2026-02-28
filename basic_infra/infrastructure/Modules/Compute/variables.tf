variable "project_name" { type = string }
variable "app_name" { type = string }

variable "aws_vpc_id" { type = string }
variable "public_subnet_ids" { type = list(string) }
variable "private_subnet_ids" { type = list(string) }
variable "azs"{ type = list(string) }

variable "allowed_ssh_cidr" { type = string }

variable "instance_type" {
	type    = string
	default = "t3.small" 
}

variable "key_name" { type = string }

variable "ecr_repo_url" { type = string }

variable "tv_webhook_secret" {
	type      = string
	sensitive = true
}

