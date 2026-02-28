locals {
	az_count = length(var.azs)
}

resource "aws_vpc" "this" {
	cidr_block = var.vpc_cidr
	tags = {
		Name = "${var.project_name}-vpc"
	}
}

resource "aws_internet_gateway" "this" {
	vpc_id = aws_vpc.this.id
	tags = {
		Name = "${var.project_name}-igw"
	}
}

resource "aws_subnet" "public" {
	count                   = local.az_count
	vpc_id                  = aws_vpc.this.id
	availability_zone       = var.azs[count.index]
	cidr_block              = var.public_subnet_cidrs[count.index]
	map_public_ip_on_launch = true

	tags = {
		Name = "${var.project_name}-public-${var.azs[count.index]}"
	}
}

resource "aws_subnet" "private" {
	count                   = local.az_count
	vpc_id                  = aws_vpc.this.id
	availability_zone       = var.azs[count.index]
	cidr_block              = var.private_subnet_cidrs[count.index]

	tags = {
		Name = "${var.project_name}-private-${var.azs[count.index]}"
	}
}

resource "aws_route_table" "public" {
	vpc_id = aws_vpc.this.id

	route {
		cidr_block = "0.0.0.0/0"
		gateway_id = aws_internet_gateway.this.id
	}

	tags = {
		Name = "${var.project_name}-public-rt"
	}
}

resource "aws_route_table_association" "public" {
	count = local.az_count
	subnet_id = aws_subnet.public[count.index].id
	route_table_id = aws_route_table.public.id
}

# NAT is optional (disable in minimal phase to avoid cost)
resource "aws_eip" "nat" {
	count  = var.enable_nat ? local.az_count : 0
	domain = "vpc"
}

resource "aws_nat_gateway" "this" {
	count         = var.enable_nat ? local.az_count : 0
	allocation_id = aws_eip.nat[count.index].id
	subnet_id     = aws_subnet.public[count.index].id

	depends_on = [aws_internet_gateway.this]

	tags = {
		Name = "${var.project_name}-nat-${var.azs[count.index]}"
	}
}

resource "aws_route_table" "private" {
	count  = local.az_count
	vpc_id = aws_vpc.this.id

	# Only add default route if NAT is needed
	dynamic "route" {
		for_each = var.enable_nat ? [1] : []
		content {
			cidr_block     = "0.0.0.0/0"
			nat_gateway_id = aws_nat_gateway.this[count.index].id
		}
	}

	tags = {
		Name = "${var.project_name}-private-rt-${var.azs[count.index]}"
	}
}

resource "aws_route_table_association" "private" {
	count          = local.az_count
	subnet_id      = aws_subnet.private[count.index].id
	route_table_id = aws_route_table.private[count.index].id
}

