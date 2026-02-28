terraform { # Terraform CLI version
	required_version = ">= 1.6.0"

	required_providers {
		aws = {
			source = "hashicorp/aws" # Provider plugin Terraform should download
			version = "~> 5.0" # Version of the provider allowed
		}
	}
}