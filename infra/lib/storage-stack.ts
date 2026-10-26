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
      // Knowledge base files and brain chat attachments are uploaded from
      // the browser with short-lived signed PUT URLs (bound to the object's
      // key, size and type by the gateway); nothing else is allowed. The
      // dashboard's origins when known at synth; otherwise any origin, since
      // the signed URL itself is the credential.
      cors: [
        {
          allowedMethods: [s3.HttpMethods.PUT],
          allowedOrigins:
            props.cfg.split?.corsOrigins ??
            (props.cfg.appUrl ? [props.cfg.appUrl] : ["*"]),
          allowedHeaders: ["content-type", "x-amz-*"],
          maxAge: 3000,
        },
      ],
      lifecycleRules: [
        {
          // Clean up old versions after 30 days
          noncurrentVersionExpiration: cdk.Duration.days(30),
        },
        {
          // Hybrid relay: event bodies held while a forward is retried
          // (about 24 hours); anything left is gone after two days.
          prefix: "relay/",
          expiration: cdk.Duration.days(2),
          noncurrentVersionExpiration: cdk.Duration.days(1),
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
