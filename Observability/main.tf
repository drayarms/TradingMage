provider "aws" {
	region = var.region
}

data "terraform_remote_state" "core" {
	backend = "s3"

	config = {
		bucket = var.core_state_bucket
		key    = var.core_state_key
		region = var.core_state_region
	}
}

data "aws_ami" "ubuntu" {
	most_recent = true
	owners      = ["099720109477"]

	filter {
		name   = "name"
		values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
	}
}

resource "aws_security_group" "this" {
	name        = "${var.name_prefix}-sg"
	description = "Security group for ELK observability host"
	vpc_id      = data.terraform_remote_state.core.outputs.vpc_id

	ingress {
		description = "SSH from admin IP"
		from_port   = 22
		to_port     = 22
		protocol    = "tcp"
		cidr_blocks = [var.allowed_kibana_cidr]
	}

	ingress {
		description = "Kibana from admin IP"
		from_port   = 5601
		to_port     = 5601
		protocol    = "tcp"
		cidr_blocks = [var.allowed_kibana_cidr]
	}

	ingress {
		description     = "Elasticsearch from app EC2 SG"
		from_port       = 9200
		to_port         = 9200
		protocol        = "tcp"
		security_groups = [data.terraform_remote_state.core.outputs.app_security_group_id]
	}

	egress {
		from_port   = 0
		to_port     = 0
		protocol    = "-1"
		cidr_blocks = ["0.0.0.0/0"]
	}

	tags = {
		Name = "${var.name_prefix}-sg"
	}
}

resource "aws_instance" "this" {
	ami                         = data.aws_ami.ubuntu.id
	instance_type               = var.instance_type
	subnet_id                   = data.terraform_remote_state.core.outputs.public_subnet_ids[0]
	associate_public_ip_address = true
	vpc_security_group_ids      = [aws_security_group.this.id]
	key_name                    = var.key_name
	user_data_replace_on_change = false

	user_data = templatefile("${path.module}/user_data_elk.sh.tpl", {
		ebs_device = "/dev/nvme1n1"
	})

	root_block_device {
		volume_size = 30
		volume_type = "gp3"
	}

	tags = {
		Name = var.name_prefix
	}
}

resource "aws_ebs_volume" "elastic_data" {
	availability_zone = aws_instance.this.availability_zone
	size              = var.ebs_size_gb
	type              = "gp3"
	encrypted         = true

	tags = {
		Name = "${var.name_prefix}-elastic-data"
	}
}

resource "aws_volume_attachment" "elastic_data" {
	device_name = "/dev/sdf"
	volume_id   = aws_ebs_volume.elastic_data.id
	instance_id = aws_instance.this.id
}