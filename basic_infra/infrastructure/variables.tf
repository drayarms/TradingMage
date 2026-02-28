variable "region" {
	type    = string
	default = "us-west-1"
}

variable "project_name" {
	type    = string
	default = "trading-mage"
}

# Minimal: 1 AZ. Can add more later
variable "azs" {
	type    = list(string)
	default = ["us-west-1a"]
}

# Must match length(azs)
variable "public_subnet_cidrs" {
	type    = list(string)
	default = ["10.3.1.0/24"] # Use only odd numbers for third byte
}

# Must match length(azs)
variable "private_subnet_cidrs" {
	type    = list(string)
	default = ["10.3.2.0/24"] # Use only even numbers for third byte
}

variable "allowed_ssh_cidr" { # Injected via CI/CD pipeline
	type        = string
	description = "Your public IP/32 for SSH"
}

variable "instance_type" {
	type    = string
	default = "t3.small"
}

variable "key_name" {
	type        = string
	default     = "my-aws-ec2-key" # This obj was created in CLI like so: aws ec2 import-key-pair \ --key-name my-aws-ec2-key \ --public-key-material fileb://~/.ssh/my-aws-ec2-key.pub
	description = "Existing EC2 KeyPair name"
}


# App/runtime
variable "ecr_repo_name" {
	type    = string
	default = "trading-mage-webhook"
}

variable "tv_webhook_secret" {
	type      = string
	sensitive = true
}
