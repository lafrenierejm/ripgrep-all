terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "instance_count" {
  type    = number
  default = 2
}

resource "aws_instance" "web" {
  count         = var.instance_count
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t3.micro"

  tags = {
    Name = "web-${count.index}"
  }

  user_data = <<-EOT
    #!/bin/bash
    echo hello
  EOT

  security_groups = [aws_security_group.web.id, "default"]
  name            = join("-", ["web", var.region])
  monitoring      = var.production ? true : false
}

output "instance_ids" {
  value = [for i in aws_instance.web : i.id]
}
