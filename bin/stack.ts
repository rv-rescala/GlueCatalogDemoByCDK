import * as cdk from '@aws-cdk/core';
import { GlueCatalogDemoStack } from '../lib/stack';


const app = new cdk.App();
new GlueCatalogDemoStack(app, 'GlueCatalogDemoStack');
app.synth();