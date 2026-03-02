provider "aws"{
	region = var.region
}

module "network"{
	source = "./Modules/Network"

	#Vars defined in Modules/Network/variables.tf but assinged in various sources, including outputs, CI/CD pipeline, variables.tf, etc
	project_name         = var.project_name # Assigned in variables.tf
	azs                  = var.azs # Assigned in variables.tf
	public_subnet_cidrs  = var.public_subnet_cidrs # Assigned in variables.tf
	private_subnet_cidrs = var.private_subnet_cidrs # Assigned in variables.tf

	#Minimal phase: no NAT to avoid cost
	enable_nat = false
}

module "compute"{
	source = "./Modules/Compute"

	project_name         = var.project_name
	app_name             = var.app_name
	
	#Vars defined in Modules/Compute/variables.tf but assinged in various sources, including outputs, CI/CD pipeline, variables.tf, etc
	aws_vpc_id           = module.network.vpc_id # Assigned in output from network module
	azs                  = var.azs # Assigned in variables.tf
	private_subnet_ids  = module.network.private_subnet_ids # Assigned in output from network module
	public_subnet_ids    = module.network.public_subnet_ids # Assigned in output from network module
	allowed_ssh_cidr     = var.allowed_ssh_cidr # assinged in CI/CD pipeline
	instance_type        = var.instance_type # Assigned in variables.tf

	# Key pair created outside Terraform #
	key_name            = var.key_name

	# ECR
	ecr_repo_url        = aws_ecr_repository.app.repository_url

	# App secrets
	tv_webhook_secret   = var.tv_webhook_secret		
  apca_api_base_url   = var.apca_api_base_url
  apca_api_key_id     = var.apca_api_key_id
  apca_api_secret_key = var.apca_api_secret_key	
}

resource "aws_ecr_repository" "app" {
  name = var.ecr_repo_name
  force_delete = true
}


