# A team's production stack, as the 2026-160-AWS example runs it.

resource "aws_launch_template" "web" {
  name          = "web"
  image_id      = "ami-0web20261020aaaa1"
  instance_type = "t3.medium"
}

resource "aws_autoscaling_group" "web" {
  name                = "web-asg"
  desired_capacity    = 3
  target_group_arns   = [aws_lb_target_group.web.arn]
  launch_template {
    id      = aws_launch_template.web.id
    version = "$Latest"
  }
}

resource "aws_eks_node_group" "prod" {
  cluster_name    = "prod"
  node_group_name = "prod-ng"
  ami_type        = "AL2023_x86_64_STANDARD"
  release_version = "1.31.0-20261015"
  scaling_config {
    desired_size = 4
    max_size     = 6
    min_size     = 2
  }
}

resource "aws_ecs_service" "api" {
  name        = "api"
  launch_type = "FARGATE"
}

resource "aws_lambda_function" "thumbs" {
  function_name = "thumbs"
  runtime       = "python3.12"
  handler       = "app.handler"
}

resource "aws_rds_cluster" "orders" {
  cluster_identifier = "orders"
  engine             = "aurora-postgresql"
  engine_version     = "16.4"
}
