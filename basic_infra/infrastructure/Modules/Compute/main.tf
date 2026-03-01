# --- IAM role for EC2 to pull from ECR ---
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_ecr_pull" {
  name               = "${var.project_name}-${var.app_name}-ec2-ecr-pull"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

# Minimal permissions to authenticate to ECR + pull images
data "aws_iam_policy_document" "ecr_pull" {
  statement {
    actions = [
      "ecr:GetAuthorizationToken"
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage"
    ]
    # If you want to lock to a single repo, see note below.
    resources = ["*"]
  }
}

resource "aws_iam_policy" "ecr_pull" {
  name   = "${var.project_name}-${var.app_name}-ecr-pull"
  policy = data.aws_iam_policy_document.ecr_pull.json
}

resource "aws_iam_role_policy_attachment" "ecr_pull" {
  role       = aws_iam_role.ec2_ecr_pull.name
  policy_arn = aws_iam_policy.ecr_pull.arn
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-${var.app_name}-ec2-profile"
  role = aws_iam_role.ec2_ecr_pull.name
}





data "aws_ami" "ubuntu" {
  most_recent = true
  owners = ["099720109477"] #Canonical

  filter {
    name = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_security_group" "web_sg" {
  name        = "${var.project_name}-${var.app_name}-sg"
  description = "Allow HTTP and SSH"
  vpc_id      = var.aws_vpc_id

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH from your IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


# Minimal phase: 1 EC2 instance (first public subnet)
resource "aws_instance" "web" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = var.public_subnet_ids[0]
  associate_public_ip_address = true
  
  vpc_security_group_ids = [aws_security_group.web_sg.id]
  key_name               = var.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  #templatefile() is a Terraform built-in function that lets you treat a file as a parameterized template, render it with variables, and pass the rendered result somewhere (like EC2 user_data). Runs during terraform plan/apply, not on EC2, in CI, or dynamically at runtime
  #In this setup, it’s the bridge between Terraform variables and a bash script.
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    ecr_repo_url      = var.ecr_repo_url
    app_name          = var.app_name
    tv_webhook_secret = var.tv_webhook_secret
  })

  tags = {
    Name = "${var.project_name}-${var.app_name}-ec2"
  }
}


