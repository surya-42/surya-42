terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-1"
}

# Create a VPC
resource "aws_vpc" "myvpc" {
  cidr_block       = "10.0.0.0/16"
  instance_tenancy = "default"

  tags = {
    Name = "my-vpc-tr"
  }
}
# create ubnet
resource "aws_subnet" "pubsub" {
  vpc_id     = aws_vpc.myvpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "eu-west-1a"

  tags = {
    Name = "my-vpc-pub-sn"
  }
}

resource "aws_subnet" "pvtsub" {
  vpc_id     = aws_vpc.myvpc.id
  cidr_block = "10.0.2.0/24"
  availability_zone = "eu-west-1b"

  tags = {
    Name = "my-vpc-pvt-sn"
  }
}
#IGW
resource "aws_internet_gateway" "trgw" {
  vpc_id = aws_vpc.myvpc.id

  tags = {
    Name = "my-vpc-igw"
  }
}

#Pub RT
resource "aws_route_table" "pubrt" {
  vpc_id = aws_vpc.myvpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.trgw.id
  }


  tags = {
    Name = "my-vpc-pub-rt"
  }
}
#Sub Association

resource "aws_route_table_association" "pubsubas" {
  subnet_id      = aws_subnet.pubsub.id
  route_table_id = aws_route_table.pubrt.id
}

#ec2

resource "aws_eip" "myvpcec2" {
  domain   = "vpc"
}

#natgw 

resource "aws_nat_gateway" "myntgw" {
  allocation_id = aws_eip.myvpcec2.id
  subnet_id     = aws_subnet.pubsub.id

  tags = {
    Name = "pvt-ng"
  }


}

#pvt rt
resource "aws_route_table" "pvtrt" {
  vpc_id = aws_vpc.myvpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_nat_gateway.myntgw.id
  }


  tags = {
    Name = "my-vpc-pub-rt"
  }
}

#Sub Association

resource "aws_route_table_association" "pvtsubas" {
  subnet_id      = aws_subnet.pvtsub.id
  route_table_id = aws_route_table.pvtrt.id
}


#security grp
resource "aws_security_group" "allow_all" {
  name        = "allow_all"
  description = "Allow TLS inbound traffic and all outbound traffic"
  vpc_id      = aws_vpc.myvpc.id

  tags = {
    Name = "allow_all"
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_all_ipv4" {
  security_group_id = aws_security_group.allow_all.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 22
  ip_protocol       = "tcp"
  to_port           = 10000
}



resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.allow_all.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}


#instance creation

resource "aws_instance" "ec21" {
  ami                     = "ami-0905a3c97561e0b69"
  instance_type           = "t2.micro"
  subnet_id               = aws_subnet.pubsub.id
  vpc_security_group_ids  = [aws_security_group.allow_all.id]
  key_name                = "my-key"
  associate_public_ip_address = true

}

resource "aws_instance" "ec22" {
  ami                     = "ami-0905a3c97561e0b69"
  instance_type           = "t2.micro"
  subnet_id               = aws_subnet.pvtsub.id
  vpc_security_group_ids  = [aws_security_group.allow_all.id]
  key_name                = "my-key"

}





