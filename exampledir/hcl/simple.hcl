# Generic HCL: scalars, collections, blocks, heredocs, and escapes
name    = "John"
age     = 30
ratio   = 1.5
enabled = true
nothing = null

// Flow-style and multi-line arrays
tags = ["developer", "rust", "python"]
ports = [
  80,
  443,
]
empty_array  = []
empty_object = {}

tags_map = {
  Name = "foo"
  Env  = "prod"
}

person {
  name = "Alice"
  age  = 25

  address {
    city = "Springfield"
  }
}

locals {}

description = <<EOF
line one

line three
EOF

escaped = "tab\there \"quoted\""
