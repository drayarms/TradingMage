variable "region" {
	type    = string
	default = "us-west-1"
}

variable "core_state_bucket" {
	type    = string
	default = "trading-mage-infrastructure-state-bucket"
}

variable "core_state_key" {
	type    = string
	default = "global/basic_infra/terraform.tfstate"
}

variable "core_state_region" {
	type    = string
	default = "us-west-1"
}

variable "name_prefix" {
	type    = string
	default = "trading-mage-observability"
}

variable "key_name" {
	type    = string
	default = "my-aws-ec2-key"
}

variable "allowed_kibana_cidr" {
	type        = string
	description = "Your public IP CIDR for Kibana/SSH access, e.g. x.x.x.x/32"
}

variable "instance_type" {
	type    = string
	default = "t3.small"
}

variable "ebs_size_gb" {
	type    = number
	default = 80
}