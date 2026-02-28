
provider "aws"{ # Runtime config of provider. Determines where AWS API calls go, which region  resources should be created in, and which creds, endpoints, or aliases should be used. if omitted, tf tries to fall back to env configs. Can work but is fragile and not portable.
	region = var.region
}

# Create S3 bucket for remote state
resource "aws_s3_bucket" "tf_state" { # Stores remote state file (terraform.tfstate)
	bucket = var.s3_bucket_name # Must be globally unique
	#force_destroy = true
}

resource "aws_s3_bucket_versioning" "tf_state"{ # Manages versioning config for existing S3 bucket. Every state file change creates a new version, comes in handy for rolling back.
	bucket = aws_s3_bucket.tf_state.id

	versioning_configuration {
		status = "Enabled"
	}
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" { # Configs deault encryption for the bucket.
	bucket = aws_s3_bucket.tf_state.id

	rule { # Defines an encryption rule
		apply_server_side_encryption_by_default { # Enforces encryption on all objs automatically
			sse_algorithm = "AES256"
		}
	}
}

resource "aws_s3_bucket_public_access_block" "tf_state" { # Controls account-level public access settings for this bucket
	bucket = aws_s3_bucket.tf_state.id

	# Terraform state must never be public. Gives defense-in-depth gaurantee even if someone misconfigs a policy or a future change introduces risk.
	block_public_acls       = true # Prevents public ACLs from being set
	block_public_policy     = true # Prevents public bucket policies
	ignore_public_acls      = true # Ignores any public ACLs that already exist
	restrict_public_buckets = true # Prevents public access even if a policy tries to allow it.
}

# Create DynamoDB table for state locking
resource "aws_dynamodb_table" "tf_lock" {
	name         = var.dynamodb_table_name
	billing_mode = "PAY_PER_REQUEST"
	hash_key     = "LockID" # Primary key used by Terraform to track locks

	attribute {
		name = "LockID"
		type = "S" # LockID as a String
	}
}


# Component	Purpose
# ---------  -------
# S3 Bucket	Stores Terraform state
# Versioning	Allows rollback
# Encryption	Protects sensitive data
# Public Access Block	Prevents accidental exposure
# DynamoDB	Enforces state locking

