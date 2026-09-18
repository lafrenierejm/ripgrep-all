region         = "us-east-1"
instance_count = 3
production     = false

allowed_cidrs = ["10.0.0.0/8", "192.168.0.0/16"]

tags = {
  Owner = "platform"
  Team  = "infra"
}
