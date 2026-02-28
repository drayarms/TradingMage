output "public_ips" {
  value = [aws_instance.web.public_ip]
}

output "public_dns" {
  value = [aws_instance.web.public_dns]
}

