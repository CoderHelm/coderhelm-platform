import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as kms from "aws-cdk-lib/aws-kms";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { hostedKeyPolicy, KEY_PENDING_WINDOW } from "./key-policy";

interface StorageStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
}

/** How long access logs are kept. */
export const ACCESS_LOG_DAYS = 90;

/**
 * The install's access log bucket, by name: the stacks that log into it
 * (web, worker) depend on the storage stack instead of importing it, so no
 * export ties them together.
 */
export function logBucketOf(scope: Construct, cfg: ResolvedConfig): s3.IBucket {
  return s3.Bucket.fromBucketAttributes(scope, "AccessLogBucket", {
    bucketName: cfg.logBucketName,
    // CloudFront's standard logging names its bucket this way (bucket.s3.amazonaws.com).
    bucketRegionalDomainName: `${cfg.logBucketName}.s3.${cdk.Aws.URL_SUFFIX}`,
  });
}

/**
 * Turns on S3 server access logging for `bucket`, into the install's log
 * bucket under `s3/<name>/`. Set on the bucket itself (not CDK's
 * serverAccessLogsBucket, which would grant log delivery with an ACL): the
 * log bucket's policy lets S3's log delivery write for this install's buckets.
 */
export function logAccessTo(bucket: s3.Bucket, cfg: ResolvedConfig, name: string): void {
  (bucket.node.defaultChild as s3.CfnBucket).loggingConfiguration = {
    destinationBucketName: cfg.logBucketName,
    logFilePrefix: `s3/${name}/`,
  };
}

export class StorageStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  /** Where the install's buckets and its CloudFront distribution write their access logs. */
  public readonly logBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    // Access logs: S3 server access logs of the install's buckets (s3/<bucket>/)
    // and the web distribution's standard logs (cloudfront/). SSE-S3, since
    // log delivery can't write to an SSE-KMS bucket; ACLs on, since
    // CloudFront's standard logging writes with one (BucketOwnerPreferred:
    // the install owns every object). Logs are operational, not data: the
    // bucket goes with the stack, writes denied first and then emptied
    // (autoDeleteObjects), so a late log delivery can't stop the delete.
    this.logBucket = new s3.Bucket(this, "LogBucket", {
      bucketName: props.cfg.logBucketName,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: "expire-access-logs",
          expiration: cdk.Duration.days(ACCESS_LOG_DAYS),
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
        },
      ],
    });
    // S3 log delivery for this install's own buckets only (the other stacks
    // name the bucket rather than import it, so CDK can't add this for them).
    this.logBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: "S3ServerAccessLogsFromThisInstall",
        principals: [new iam.ServicePrincipal("logging.s3.amazonaws.com")],
        actions: ["s3:PutObject"],
        resources: [this.logBucket.arnForObjects("s3/*")],
        conditions: {
          StringEquals: { "aws:SourceAccount": this.account },
          ArnLike: { "aws:SourceArn": `arn:${this.partition}:s3:::${props.cfg.prefix}-*` },
        },
      })
    );

    const encryptionKey = new kms.Key(this, "BucketKey", {
      alias: `${props.cfg.prefix}-s3`,
      description: "Coderhelm S3 encryption key",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      pendingWindow: KEY_PENDING_WINDOW,
      policy: hostedKeyPolicy(this, props.cfg, "symmetric"),
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

    logAccessTo(this.bucket, props.cfg, "data");
    this.bucket.node.addDependency(this.logBucket);

    new cdk.CfnOutput(this, "BucketName", { value: this.bucket.bucketName });
    new cdk.CfnOutput(this, "LogBucketName", { value: this.logBucket.bucketName });
  }
}
