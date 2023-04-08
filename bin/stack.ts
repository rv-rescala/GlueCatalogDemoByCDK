import * as cdk from 'aws-cdk-lib';
import { GlueCatalogDemoStack } from '../lib/stack';


const app = new cdk.App();
new GlueCatalogDemoStack(app, 'GlueCatalogDemoStack');
app.synth();