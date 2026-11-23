resource "aws_s3_bucket" "uploads" {
  bucket = "harbor-receipt-uploads"
}

data "archive_file" "receipts" {
  type        = "zip"
  source_dir  = "${path.module}/../harbor-api/src/receipts"
  output_path = "${path.module}/receipts.zip"
}

resource "aws_lambda_function" "receipts" {
  function_name = "harbor-receipts"
  filename      = data.archive_file.receipts.output_path
  handler       = "index.handler"
  runtime       = "nodejs20.x"
  role          = aws_iam_role.receipts.arn

  environment {
    variables = {
      RECEIPTS_BUCKET = aws_s3_bucket.uploads.bucket
    }
  }
}

resource "aws_s3_bucket_notification" "uploads" {
  bucket = aws_s3_bucket.uploads.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.receipts.arn
    events              = ["s3:ObjectCreated:*"]
  }
}

resource "aws_iam_role" "receipts" {
  name               = "harbor-receipts"
  assume_role_policy = "{}"
}
