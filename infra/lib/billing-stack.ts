import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";

interface BillingStackProps extends cdk.StackProps {
  stage: string;
  gatewayFunction: lambda.Function;
}

export class BillingStack extends cdk.Stack {
  public readonly invoiceBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: BillingStackProps) {
    super(scope, id, props);

    const prefix = `d3ftly-${props.stage}`;

    // S3 bucket for invoice PDFs (presigned URL downloads)
    this.invoiceBucket = new s3.Bucket(this, "InvoiceBucket", {
      bucketName: `${prefix}-invoices`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          // Keep invoices for 7 years (tax compliance)
          expiration: cdk.Duration.days(2555),
        },
      ],
      removalPolicy:
        props.stage === "prod"
          ? cdk.RemovalPolicy.RETAIN
          : cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: props.stage !== "prod",
    });

    // Gateway needs read/write for invoice generation + presigned URL downloads
    this.invoiceBucket.grantReadWrite(props.gatewayFunction);

    new cdk.CfnOutput(this, "InvoiceBucketName", {
      value: this.invoiceBucket.bucketName,
    });
  }
}
