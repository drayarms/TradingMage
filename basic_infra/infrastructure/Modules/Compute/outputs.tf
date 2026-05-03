output "public_ips" {
  value = [aws_instance.web.public_ip]
}

output "public_dns" {
  value = [aws_instance.web.public_dns]
}

output "app_security_group_id" {
  value = aws_security_group.web_sg.id
}