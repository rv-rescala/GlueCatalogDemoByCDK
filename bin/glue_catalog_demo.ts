import * as cdk from '@aws-cdk/core';
import { GlueCatalogDemoStack } from '../lib/glue_catalog_demo-stack';

const app = new cdk.App();
new GlueCatalogDemoStack(app, 'GlueCatalogDemoStack');
app.synth();