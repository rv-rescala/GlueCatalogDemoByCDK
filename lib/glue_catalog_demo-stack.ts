import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as rds from '@aws-cdk/aws-rds';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iam from '@aws-cdk/aws-iam';

const PyMySQL_LAYER = 'arn:aws:lambda:ap-northeast-1:770693421928:layer:Klayers-p38-PyMySQL:1'

function getVPC(stack: cdk.Stack) {
  return new ec2.Vpc(stack, 'VPC', {});
}

function getLambdaRole(stack: cdk.Stack, clusterSecretSecretArn: string) {
  const role = new iam.Role(stack, 'CreateUserTableLambdaRole', {
    assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  });

  // for SecretsManager
  const getSecretPolicy = new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['secretsmanager:GetSecretValue'],
    resources: [clusterSecretSecretArn],
  });
  role.addToPolicy(getSecretPolicy);

  // for RDS
  const rdsAccessPolicy = new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['rds-data:ExecuteStatement', 'rds-data:BatchExecuteStatement', 'rds-data:BeginTransaction', 'rds-data:CommitTransaction', 'rds-data:RollbackTransaction', 'secretsmanager:GetResourcePolicy', 'secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret'],
    resources: ['*'],
  });
  role.addToPolicy(rdsAccessPolicy);

  // for ec2AccessPolicy
  const ec2AccessPolicy = new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['ec2:CreateNetworkInterface'],
    resources: ['*'],
  });
  role.addToPolicy(ec2AccessPolicy);
  return role
}

function createLambda(stack: cdk.Stack, vpc: ec2.IVpc, clusterSecretSecretArn: string, name: string) {
  const secretARNKey = `DB_SECRET_ARN_${name}`;
  const lambdaFunction = new lambda.Function(stack, 'CreateUserTableLambda', {
    runtime: lambda.Runtime.PYTHON_3_8,
    handler: 'create_user_table.handler',
    code: lambda.Code.fromAsset('lambda'),
    timeout: cdk.Duration.seconds(30),
    environment: {
      secretARNKey: clusterSecretSecretArn,
    },
    layers: [lambda.LayerVersion.fromLayerVersionArn(stack, 'PyMySQL_LAYER', PyMySQL_LAYER)],
    role: getLambdaRole(stack, clusterSecretSecretArn),
    vpc: vpc
  });
  return lambdaFunction
}

function createServerlessCluster(stack: cdk.Stack, vpc: ec2.IVpc, name: string) {
  const cluster = new rds.ServerlessCluster(stack, `SC_${name}`, {
    engine: rds.DatabaseClusterEngine.auroraMysql({
      version: rds.AuroraMysqlEngineVersion.of('5.7.mysql_aurora.2.12.2')
    }),
    vpc,
    defaultDatabaseName: `SC_${name}`,
    scaling: {
      autoPause: cdk.Duration.minutes(5),
      minCapacity: rds.AuroraCapacityUnit.ACU_1,
      maxCapacity: rds.AuroraCapacityUnit.ACU_8,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
  cluster.addRotationSingleUser({ automaticallyAfter: cdk.Duration.days(30) });
  return cluster
}

export class GlueCatalogDemoStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = getVPC(this)
    const cluster = createServerlessCluster(this, vpc, "GlueCatalogDemo")
    const lambdaFunction = createLambda(this, vpc, cluster.secret?.secretArn || '', "GlueCatalogDemo")
  }
}