#!/usr/bin/env node
import "source-map-support/register";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { buildApp } from "../lib/app";
import { chooseConfigFile, ConfigError, loadConfigFile, resolveConfig } from "../lib/config";

// Which deployment to build comes from one config file:
//   cdk deploy -c config=../coderhelm.config.json   (or CODERHELM_CONFIG=...)
// With neither, the hosted deployment's own config is used
// (config/hosted.prod.json; `-c stage=<name>` picks config/local.json).
const app = new cdk.App();

const choice = chooseConfigFile({
  contextConfig: app.node.tryGetContext("config"),
  envConfig: process.env.CODERHELM_CONFIG,
  stage: app.node.tryGetContext("stage"),
  infraDir: path.join(__dirname, ".."),
});

let cfg;
try {
  cfg = loadConfigFile(choice.file);
  if (choice.stage && choice.stage !== cfg.stage) {
    cfg = resolveConfig({ ...cfg.raw, stage: choice.stage });
  }
} catch (err) {
  if (err instanceof ConfigError) {
    console.error(`${choice.file}\n${err.message}`);
    process.exit(1);
  }
  throw err;
}

buildApp(app, cfg);
