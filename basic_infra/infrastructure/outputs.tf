# Outputs form Compute Module
output "ec2_public_ips" {
  value = module.compute.public_ips
}

output "ec2_public_dns" {
  value = module.compute.public_dns
}

# Outputs from Network Module
output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}

output "public_subnet_ids" {
  value = module.network.public_subnet_ids
}

#ECR
output "ecr_repo_url" {
  value = aws_ecr_repository.app.repository_url
}



