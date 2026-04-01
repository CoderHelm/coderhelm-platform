import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as route53Targets from "aws-cdk-lib/aws-route53-targets";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as ses from "aws-cdk-lib/aws-ses";
import { Construct } from "constructs";

interface ApiStackProps extends cdk.StackProps {
  stage: string;
  table: dynamodb.TableV2;
  runsTable: dynamodb.TableV2;
  analyticsTable: dynamodb.TableV2;
  eventsTable: dynamodb.TableV2;
  usersTable: dynamodb.TableV2;
  jiraTokensTable: dynamodb.TableV2;
  jiraEventsTable: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  jiraConfigTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  infraTable: dynamodb.TableV2;
  billingTable: dynamodb.TableV2;
  bannersTable: dynamodb.TableV2;
  bucket: s3.Bucket;
}

export class ApiStack extends cdk.Stack {
  public readonly gatewayFunction: lambda.Function;
  public readonly ticketQueue: sqs.Queue;
  public readonly ciFixQueue: sqs.Queue;
  public readonly feedbackQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;
    const gatewayAssetPath =
      process.env.GATEWAY_ZIP ?? "../services/gateway/target/lambda/gateway";

    // --- Cognito User Pool ---

    const userPool = new cognito.UserPool(this, "UserPool", {
      userPoolName: `${prefix}-users`,
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: {
        email: { required: true, mutable: true },
        fullname: { required: false, mutable: true },
      },
      passwordPolicy: {
        minLength: 8,
        requireUppercase: true,
        requireLowercase: true,
        requireDigits: true,
        requireSymbols: false,
      },
      mfa: cognito.Mfa.OPTIONAL,
      mfaSecondFactor: { sms: false, otp: true },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      email: cognito.UserPoolEmail.withSES({
        fromEmail: "noreply@coderhelm.com",
        fromName: "Coderhelm",
        sesRegion: "us-east-1",
        sesVerifiedDomain: "coderhelm.com",
      }),
      removalPolicy:
        props.stage === "prod"
          ? cdk.RemovalPolicy.RETAIN
          : cdk.RemovalPolicy.DESTROY,
    });

    // Google identity provider
    // TODO: Set these values in Secrets Manager at coderhelm/{stage}/secrets:
    //   google_client_id     — from https://console.cloud.google.com/apis/credentials
    //   google_client_secret — same page
    //   Callback URL: https://<cognito-domain>.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
    const googleSecrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "GoogleSecrets",
      `coderhelm/${props.stage}/secrets`
    );

    const googleProvider = new cognito.UserPoolIdentityProviderGoogle(
      this,
      "GoogleProvider",
      {
        userPool,
        clientId: googleSecrets
          .secretValueFromJson("google_client_id")
          .unsafeUnwrap(),
        clientSecretValue: googleSecrets.secretValueFromJson(
          "google_client_secret"
        ),
        scopes: ["openid", "email", "profile"],
        attributeMapping: {
          email: cognito.ProviderAttribute.GOOGLE_EMAIL,
          fullname: cognito.ProviderAttribute.GOOGLE_NAME,
          profilePicture: cognito.ProviderAttribute.GOOGLE_PICTURE,
        },
      }
    );

    const dashboardUrl =
      props.stage === "prod"
        ? "https://app.coderhelm.com"
        : "http://localhost:3000";

    const apiUrl =
      props.stage === "prod"
        ? "https://api.coderhelm.com"
        : "http://localhost:3001";

    const userPoolClient = new cognito.UserPoolClient(this, "UserPoolClient", {
      userPool,
      userPoolClientName: `${prefix}-dashboard`,
      authFlows: {
        userPassword: true,
        userSrp: true,
      },
      oAuth: {
        flows: { authorizationCodeGrant: true },
        scopes: [
          cognito.OAuthScope.OPENID,
          cognito.OAuthScope.EMAIL,
          cognito.OAuthScope.PROFILE,
        ],
        callbackUrls: [`${apiUrl}/auth/google/callback`],
        logoutUrls: [dashboardUrl],
      },
      supportedIdentityProviders: [
        cognito.UserPoolClientIdentityProvider.COGNITO,
        cognito.UserPoolClientIdentityProvider.GOOGLE,
      ],
      generateSecret: true,
    });

    userPoolClient.node.addDependency(googleProvider);

    // Cognito custom domain (prod: auth.coderhelm.com, dev: hosted UI prefix)
    let userPoolDomain: cognito.UserPoolDomain;
    if (props.stage === "prod") {
      const hostedZone = route53.HostedZone.fromLookup(this, "AuthZone", {
        domainName: "coderhelm.com",
      });

      const authCert = new acm.Certificate(this, "AuthCertificate", {
        domainName: "auth.coderhelm.com",
        validation: acm.CertificateValidation.fromDns(hostedZone),
      });

      userPoolDomain = userPool.addDomain("UserPoolDomain", {
        customDomain: {
          domainName: "auth.coderhelm.com",
          certificate: authCert,
        },
      });

      new route53.ARecord(this, "AuthAliasRecord", {
        zone: hostedZone,
        recordName: "auth",
        target: route53.RecordTarget.fromAlias(
          new route53Targets.UserPoolDomainTarget(userPoolDomain)
        ),
      });
    } else {
      userPoolDomain = userPool.addDomain("UserPoolDomain", {
        cognitoDomain: {
          domainPrefix: `coderhelm-${props.stage}`,
        },
      });
    }

    // --- SQS Queues ---

    // Dead letter queue (shared)
    const dlq = new sqs.Queue(this, "DLQ", {
      queueName: `${prefix}-dlq`,
      retentionPeriod: cdk.Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });

    this.ticketQueue = new sqs.Queue(this, "TicketQueue", {
      queueName: `${prefix}-tickets`,
      visibilityTimeout: cdk.Duration.minutes(16), // > Lambda timeout
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    this.ciFixQueue = new sqs.Queue(this, "CiFixQueue", {
      queueName: `${prefix}-ci-fix`,
      visibilityTimeout: cdk.Duration.minutes(16),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    this.feedbackQueue = new sqs.Queue(this, "FeedbackQueue", {
      queueName: `${prefix}-feedback`,
      visibilityTimeout: cdk.Duration.minutes(16),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    // --- Secrets ---

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `coderhelm/${props.stage}/secrets`
    );

    // --- Gateway Lambda (Rust) ---

    const gatewayLogGroup = new logs.LogGroup(this, "GatewayLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-gateway`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.gatewayFunction = new lambda.Function(this, "Gateway", {
      functionName: `${prefix}-gateway`,
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(gatewayAssetPath),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      logGroup: gatewayLogGroup,
      environment: {
        STAGE: props.stage,
        TABLE_NAME: props.table.tableName,
        RUNS_TABLE_NAME: props.runsTable.tableName,
        ANALYTICS_TABLE_NAME: props.analyticsTable.tableName,
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
        USERS_TABLE_NAME: props.usersTable.tableName,
        JIRA_TOKENS_TABLE_NAME: props.jiraTokensTable.tableName,
        JIRA_EVENTS_TABLE_NAME: props.jiraEventsTable.tableName,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        BILLING_TABLE_NAME: props.billingTable.tableName,
        BANNERS_TABLE_NAME: props.bannersTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        TICKET_QUEUE_URL: this.ticketQueue.queueUrl,
        CI_FIX_QUEUE_URL: this.ciFixQueue.queueUrl,
        FEEDBACK_QUEUE_URL: this.feedbackQueue.queueUrl,
        DLQ_URL: dlq.queueUrl,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        SES_FROM_ADDRESS: "noreply@coderhelm.com",
        SES_TEMPLATE_PREFIX: `coderhelm-${props.stage}`,
        MODEL_ID: "us.anthropic.claude-sonnet-4-6",
        COGNITO_USER_POOL_ID: userPool.userPoolId,
        COGNITO_CLIENT_ID: userPoolClient.userPoolClientId,
        COGNITO_DOMAIN: userPoolDomain.domainName,
        RUST_LOG: "info",
      },
    });

    // Permissions
    props.table.grantReadWriteData(this.gatewayFunction);
    props.runsTable.grantReadWriteData(this.gatewayFunction);
    props.analyticsTable.grantReadData(this.gatewayFunction);
    props.eventsTable.grantReadWriteData(this.gatewayFunction);
    props.usersTable.grantReadWriteData(this.gatewayFunction);
    props.jiraTokensTable.grantReadWriteData(this.gatewayFunction);
    props.jiraEventsTable.grantReadWriteData(this.gatewayFunction);
    props.plansTable.grantReadWriteData(this.gatewayFunction);
    props.jiraConfigTable.grantReadWriteData(this.gatewayFunction);
    props.reposTable.grantReadWriteData(this.gatewayFunction);
    props.settingsTable.grantReadWriteData(this.gatewayFunction);
    props.infraTable.grantReadWriteData(this.gatewayFunction);
    props.billingTable.grantReadWriteData(this.gatewayFunction);
    props.bannersTable.grantReadData(this.gatewayFunction);
    props.bucket.grantReadWrite(this.gatewayFunction);
    this.ticketQueue.grantSendMessages(this.gatewayFunction);
    this.ciFixQueue.grantSendMessages(this.gatewayFunction);
    this.feedbackQueue.grantSendMessages(this.gatewayFunction);
    dlq.grantConsumeMessages(this.gatewayFunction);
    secrets.grantRead(this.gatewayFunction);

    // Bedrock access for plan chat
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel", "bedrock:Converse"],
        resources: [
          `arn:aws:bedrock:*::foundation-model/*`,
          `arn:aws:bedrock:*:${this.account}:inference-profile/*`,
        ],
      })
    );

    // Cognito access for auth flows
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "cognito-idp:SignUp",
          "cognito-idp:ConfirmSignUp",
          "cognito-idp:InitiateAuth",
          "cognito-idp:RespondToAuthChallenge",
          "cognito-idp:ForgotPassword",
          "cognito-idp:ConfirmForgotPassword",
          "cognito-idp:ChangePassword",
          "cognito-idp:GetUser",
          "cognito-idp:AdminGetUser",
          "cognito-idp:AdminUpdateUserAttributes",
          "cognito-idp:AdminDisableUser",
          "cognito-idp:AdminDeleteUser",
          "cognito-idp:AdminCreateUser",
          "cognito-idp:AdminSetUserMFAPreference",
          "cognito-idp:AssociateSoftwareToken",
          "cognito-idp:VerifySoftwareToken",
          "cognito-idp:DescribeUserPoolClient",
        ],
        resources: [userPool.userPoolArn],
      })
    );

    // --- HTTP API Gateway ---

    const httpApi = new apigatewayv2.HttpApi(this, "HttpApi", {
      apiName: `${prefix}-api`,
      corsPreflight: {
        allowOrigins: [
          props.stage === "prod"
            ? "https://app.coderhelm.com"
            : "http://localhost:3000",
        ],
        allowMethods: [
          apigatewayv2.CorsHttpMethod.GET,
          apigatewayv2.CorsHttpMethod.POST,
          apigatewayv2.CorsHttpMethod.PUT,
          apigatewayv2.CorsHttpMethod.DELETE,
          apigatewayv2.CorsHttpMethod.OPTIONS,
        ],
        allowHeaders: ["Content-Type", "Authorization", "Cookie"],
        allowCredentials: true,
      },
    });

    const lambdaIntegration =
      new integrations.HttpLambdaIntegration(
        "GatewayIntegration",
        this.gatewayFunction
      );

    // Webhook routes
    httpApi.addRoutes({
      path: "/webhooks/github",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/jira/{proxy+}",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/jira",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/integrations/jira/forge-register",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/stripe",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Auth routes
    httpApi.addRoutes({
      path: "/auth/{proxy+}",
      methods: [apigatewayv2.HttpMethod.GET, apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Dashboard API routes
    httpApi.addRoutes({
      path: "/api/{proxy+}",
      methods: [
        apigatewayv2.HttpMethod.GET,
        apigatewayv2.HttpMethod.POST,
        apigatewayv2.HttpMethod.PUT,
        apigatewayv2.HttpMethod.DELETE,
      ],
      integration: lambdaIntegration,
    });

    // Outputs
    new cdk.CfnOutput(this, "ApiUrl", {
      value: httpApi.apiEndpoint,
    });

    new cdk.CfnOutput(this, "UserPoolId", {
      value: userPool.userPoolId,
    });

    new cdk.CfnOutput(this, "UserPoolClientId", {
      value: userPoolClient.userPoolClientId,
    });

    // --- Custom domain: api.coderhelm.com ---
    if (props.stage === "prod") {
      const apiDomain = "api.coderhelm.com";

      const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
        domainName: "coderhelm.com",
      });

      const certificate = new acm.Certificate(this, "ApiCertificate", {
        domainName: apiDomain,
        validation: acm.CertificateValidation.fromDns(hostedZone),
      });

      const customDomain = new apigatewayv2.DomainName(this, "ApiCustomDomain", {
        domainName: apiDomain,
        certificate,
      });

      new apigatewayv2.ApiMapping(this, "ApiMapping", {
        api: httpApi,
        domainName: customDomain,
      });

      new route53.ARecord(this, "ApiAliasRecord", {
        zone: hostedZone,
        recordName: "api",
        target: route53.RecordTarget.fromAlias(
          new route53Targets.ApiGatewayv2DomainProperties(
            customDomain.regionalDomainName,
            customDomain.regionalHostedZoneId
          )
        ),
      });

      new cdk.CfnOutput(this, "ApiCustomDomainUrl", {
        value: `https://${apiDomain}`,
      });
    }
  }
}
