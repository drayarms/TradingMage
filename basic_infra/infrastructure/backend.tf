#Terraform cannot use an S3 backend until after the bucket and DynamoDB table already exist. 
#This is why the backend must read the local state of the Backend module first (the bootstrap step).
#Backend must be hardcoded (or templated) because Terraform backends can't depend on variables/outputs.

terraform {
	backend "s3" {
		bucket =  "trading-mage-infrastructure-state-bucket" # Reference from backend module
		key = "global/basic_infra/terraform.tfstate" # Path to file inside the S3 bucket
		region = "us-west-1"
		dynamodb_table = "trading-mage-terraform-lock" # Reference from backend module
		encrypt = true
	}
}

