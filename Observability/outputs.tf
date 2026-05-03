output "private_ip" {
	value = aws_instance.this.private_ip
}

output "public_ip" {
	value = aws_instance.this.public_ip
}

output "security_group_id" {
	value = aws_security_group.this.id
}

output "kibana_url" {
	value = "http://${aws_instance.this.public_ip}:5601"
}