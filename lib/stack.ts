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
} from 'aws-cdk-lib';
import * as glue_alpha from '@aws-cdk/aws-glue-alpha';
import * as cdk from 'aws-cdk-lib';


const PyMySQL_LAYER = 'arn:aws:lambda:ap-northeast-1:770693421928:layer:Klayers-p38-PyMySQL:1'
const STACK_NAME = "GlueCatalogDemo"

function getVPC(stack: Stack) {
  return new aws_ec2.Vpc(stack, 'VPC', {});
}

function getLambdaRole(stack: Stack, clusterSecretSecretArn: string) {
  const role = new aws_iam.Role(stack, 'CreateUserTableLambdaRole', {
    assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
  });

  // for SecretsManager
  const getSecretPolicy = new aws_iam.PolicyStatement({
    effect: aws_iam.Effect.ALLOW,
    actions: ['secretsmanager:GetSecretValue'],
    resources: [clusterSecretSecretArn],
  });
  role.addToPolicy(getSecretPolicy);

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

function createLambda(stack: Stack, vpc: aws_ec2.Vpc, clusterSecretSecretArn: string, name: string) {
  const secretARNKey = `DB_SECRET_ARN_${name}`;
  const lambdaFunction = new aws_lambda.Function(stack, 'CreateUserTableLambda', {
    runtime: aws_lambda.Runtime.PYTHON_3_8,
    handler: 'create_user_table.handler',
    code: aws_lambda.Code.fromAsset('lambda'),
    timeout: cdk.Duration.seconds(30),
    environment: {
      secretARNKey: clusterSecretSecretArn,
    },
    layers: [aws_lambda.LayerVersion.fromLayerVersionArn(stack, 'PyMySQL_LAYER', PyMySQL_LAYER)],
    role: getLambdaRole(stack, clusterSecretSecretArn),
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
    defaultDatabaseName: `SC_${name}`,
    scaling: {
      autoPause: cdk.Duration.minutes(5),
      minCapacity: aws_rds.AuroraCapacityUnit.ACU_1,
      maxCapacity: aws_rds.AuroraCapacityUnit.ACU_8,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
  cluster.addRotationSingleUser({ automaticallyAfter: cdk.Duration.days(30) });
  return cluster
}

export class GlueCatalogDemoStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const vpc = getVPC(this)
    const cluster = createServerlessCluster(this, vpc, STACK_NAME)
    const lambdaFunction = createLambda(this, vpc, cluster.secret?.secretArn || '', STACK_NAME)

  }
}