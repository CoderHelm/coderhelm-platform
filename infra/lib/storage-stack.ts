import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as kms from "aws-cdk-lib/aws-kms";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

interface StorageStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
}

export class StorageStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    const encryptionKey = new kms.Key(this, "BucketKey", {
      alias: `${props.cfg.prefix}-s3`,
      description: "Coderhelm S3 encryption key",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.bucket = new s3.Bucket(this, "DataBucket", {
      bucketName: props.cfg.dataBucketName,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      removalPolicy: props.cfg.retainData
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: !props.cfg.retainData,
      lifecycleRules: [
        {
          // Clean up old versions after 30 days
          noncurrentVersionExpiration: cdk.Duration.days(30),
        },
        {
          // Move old run artifacts to Infrequent Access after 90 days
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(90),
            },
          ],
        },
      ],
    });

    new cdk.CfnOutput(this, "BucketName", { value: this.bucket.bucketName });
  }
}
