variable "project_name" {
	type = string
}

variable "vpc_cidr" {
	type = string
	default = "10.3.0.0/16" 
}

variable "azs"{
	type = list(string)
}

variable "public_subnet_cidrs" { 
	type = list(string)
}

variable "private_subnet_cidrs" { 
	type = list(string)
}

variable "enable_nat"{
	type = bool
	default = false
}
