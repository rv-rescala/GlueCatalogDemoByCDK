import { Construct } from 'constructs';
import {
  aws_s3,
  aws_athena,
  Stack,
  StackProps,
  RemovalPolicy,
  aws_glue,
  aws_iam,
  aws_ec2,
  aws_lambda,
  aws_rds,
  SecretValue
} from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import { Cluster } from 'aws-cdk-lib/aws-ecs';
import { ServerlessCluster } from 'aws-cdk-lib/aws-rds';
import { Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';

const PyMySQL_LAYER = 'arn:aws:lambda:ap-northeast-1:770693421928:layer:Klayers-p38-PyMySQL:1'
const STACK_NAME = "GlueCatalogDemo"
const AURORA_DB_NAME = "GlueCatalogDemo"
const GLUE_DB_NAME = "gule_catalog_demo_db"

function getVPC(stack: Stack) {
  return new aws_ec2.Vpc(stack, 'VPC', {});
}

function getSecretPolicy(clusterSecretSecretArn: string) {
  return new aws_iam.PolicyStatement({
    effect: aws_iam.Effect.ALLOW,
    actions: ['secretsmanager:GetSecretValue'],
    resources: [clusterSecretSecretArn],
  });
}

function createConnectionRole(stack: Stack, clusterSecretSecretArn: string) {
  const role = new aws_iam.Role(stack, `${STACK_NAME}GlueConnectionRole`, {
    assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
  });

  // for SecretsManager
  const secretPolicy = getSecretPolicy(clusterSecretSecretArn)
  role.addToPolicy(secretPolicy);
  return role
}

function createLmbdaRole(stack: Stack, clusterSecretSecretArn: string) {
  const role = new aws_iam.Role(stack, 'CreateUserTableLambdaRole', {
    assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
  });

  // for SecretsManager
  const secretPolicy = getSecretPolicy(clusterSecretSecretArn)
  role.addToPolicy(secretPolicy);

  // for RDS
  const rdsAccessPolicy = new aws_iam.PolicyStatement({
    effect: aws_iam.Effect.ALLOW,
    actions: ['rds-data:ExecuteStatement', 'rds-data:BatchExecuteStatement', 'rds-data:BeginTransaction', 'rds-data:CommitTransaction', 'rds-data:RollbackTransaction', 'secretsmanager:GetResourcePolicy', 'secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret'],
    resources: ['*'],
  });
  role.addToPolicy(rdsAccessPolicy);

  // for ec2AccessPolicy
  const ec2AccessPolicy = new aws_iam.PolicyStatement({
    effect: aws_iam.Effect.ALLOW,
    actions: ['ec2:CreateNetworkInterface', 'ec2:DescribeNetworkInterfaces', 'ec2:DeleteNetworkInterface'],
    resources: ['*'],
  });
  role.addToPolicy(ec2AccessPolicy);
  return role
}

function createLambda(stack: Stack, vpc: aws_ec2.Vpc, cluster: ServerlessCluster, name: string) {
  const secretARNKey = `DB_SECRET_ARN_${name}`;
  const lambdaFunction = new aws_lambda.Function(stack, 'CreateUserTableLambda', {
    runtime: aws_lambda.Runtime.PYTHON_3_8,
    handler: 'create_user_table.handler',
    code: aws_lambda.Code.fromAsset('lambda'),
    timeout: cdk.Duration.seconds(30),
    environment: {
      [secretARNKey]: cluster.secret?.secretArn || '',
      DB_CLUSTER_ARN: cluster.clusterArn
    },
    layers: [aws_lambda.LayerVersion.fromLayerVersionArn(stack, 'PyMySQL_LAYER', PyMySQL_LAYER)],
    role: createLmbdaRole(stack, cluster.secret?.secretArn || ''),
    vpc: vpc
  });
  return lambdaFunction
}

function createServerlessCluster(stack: Stack, vpc: aws_ec2.Vpc, name: string) {
  const cluster = new aws_rds.ServerlessCluster(stack, `SC_${name}`, {
    engine: aws_rds.DatabaseClusterEngine.auroraMysql({
      version: aws_rds.AuroraMysqlEngineVersion.of('5.7.mysql_aurora.2.12.2')
    }),
    vpc,
    defaultDatabaseName: AURORA_DB_NAME,
    scaling: {
      autoPause: cdk.Duration.minutes(5),
      minCapacity: aws_rds.AuroraCapacityUnit.ACU_1,
      maxCapacity: aws_rds.AuroraCapacityUnit.ACU_8,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    enableDataApi: true
  });
  cluster.addRotationSingleUser({ automaticallyAfter: cdk.Duration.days(30) });
  cluster.connections.allowDefaultPortFromAnyIpv4('Open to the world');

  return cluster
}

function createGlueDatabase(stack: Stack) {
  // Create a Glue CfnDatabase
  const database = new aws_glue.CfnDatabase(stack, `${STACK_NAME}CfnDatabase`, {
    catalogId: cdk.Aws.ACCOUNT_ID,
    databaseInput: {
      name: GLUE_DB_NAME,
    },
  });
  return database
}

function createGlueConnection(stack: Stack, cluster: ServerlessCluster, vpc: aws_ec2.Vpc) {
  // Add an inbound rule to the security group to open all ports for itself
  const jdbcConnectionUrl = `jdbc:mysql://${cluster.clusterEndpoint.hostname}:${cluster.clusterEndpoint.port}`;

  // Get all security groups in the VPC
  // Create a security group that allows all inbound and outbound traffic
  const allTrafficSecurityGroup = new aws_ec2.SecurityGroup(stack, 'AllTrafficSecurityGroupForGlueConnection', {
    vpc,
    description: 'Allow all inbound and outbound traffic',
    allowAllOutbound: true,
  });

  // Allow all inbound traffic
  allTrafficSecurityGroup.addIngressRule(aws_ec2.Peer.anyIpv4(), aws_ec2.Port.allTraffic(), 'Allow all inbound traffic');

  const connection = new aws_glue.CfnConnection(stack, `${STACK_NAME}CfnConnection`, {
    catalogId: cdk.Aws.ACCOUNT_ID,
    connectionInput: {
      name: `${STACK_NAME}Connection`,
      connectionType: "JDBC",
      description: `A Glue connection to the Aurora Serverless RDS database`,
      connectionProperties: {
        JDBC_CONNECTION_URL: jdbcConnectionUrl, // Fix the protocol
        JDBC_ENFORCE_SSL: 'false',
        SECRET_ID: cluster.secret?.secretName || '',
      },
      physicalConnectionRequirements: {
        availabilityZone: vpc.availabilityZones[0],
        securityGroupIdList: [allTrafficSecurityGroup.securityGroupId],
        subnetId: vpc.privateSubnets[0].subnetId,
      }
    },
  });
  return connection
}

function createGlueCrawler(stack: Stack, glueDatabase: aws_glue.CfnDatabase, cluster: ServerlessCluster) {
  // Create a Glue Crawler to crawl the database
  const glueServiceRole = new aws_iam.Role(stack, `${STACK_NAME}GlueServiceRole`, {
    assumedBy: new aws_iam.ServicePrincipal("glue.amazonaws.com"),
    managedPolicies: [
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
    ],
  });
  const secretPolicy = getSecretPolicy(cluster.secret?.secretArn || '')
  glueServiceRole.addToPolicy(secretPolicy);

  const crawler = new aws_glue.CfnCrawler(stack, `${STACK_NAME}CfnCrawler`, {
    name: `${AURORA_DB_NAME}UserCrawler`,
    role: glueServiceRole.roleArn,
    databaseName: GLUE_DB_NAME,
    targets: {
      s3Targets: [],
      jdbcTargets: [{
        connectionName: `${STACK_NAME}Connection`,
        path: "GlueCatalogDemo/user/%",
        exclusions: []
      }]
    }
  });
  return crawler
}

export class GlueCatalogDemoStack extends Stack {
  constructor(scope: cdk.App, id: string, props?: StackProps) {
    super(scope, id, props);

    const vpc = getVPC(this)

    const cluster = createServerlessCluster(this, vpc, STACK_NAME);
    const lambdaFunction = createLambda(this, vpc, cluster, STACK_NAME);
    const glueDatabase = createGlueDatabase(this);
    const glueConnection = createGlueConnection(this, cluster, vpc);
    const glueCrawler = createGlueCrawler(this, glueDatabase, cluster);
  }
}