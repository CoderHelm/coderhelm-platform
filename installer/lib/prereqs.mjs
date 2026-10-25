// What must be on the machine before an install: Node 20+, the AWS CLI v2
// with credentials for the target account, and tar. Docker and Rust are not
// needed (the release ships built Lambda code).

/** Every missing prerequisite, as messages; empty when all are there. */
export async function checkPrereqs(runner, { account, nodeVersion = process.versions.node } = {}) {
  const problems = [];
  const major = Number(nodeVersion.split(".")[0]);
  if (!(major >= 20)) problems.push(`Node.js 20 or newer is needed (found ${nodeVersion}).`);

  const aws = await runner.run("aws", ["--version"], { capture: true, allowFail: true, dryRunAnswer: "aws-cli/2.17.0" }).catch(() => null);
  const awsOut = aws ? `${aws.stdout}${aws.stderr}` : "";
  if (!aws || aws.code !== 0) problems.push("The AWS CLI v2 is needed: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html");
  else if (!/aws-cli\/2\./.test(awsOut)) problems.push(`The AWS CLI must be version 2 (found: ${awsOut.trim()}).`);

  let identity = null;
  if (aws && aws.code === 0) {
    identity = await runner.json("aws", ["sts", "get-caller-identity", "--output", "json"], {
      allowFail: true,
      dryRunAnswer: JSON.stringify({ Account: account ?? "000000000000", Arn: "arn:aws:iam::000000000000:user/dry-run" }),
    });
    if (!identity) problems.push("No AWS credentials found. Sign in (aws sso login, or set AWS_PROFILE) and run this again.");
    else if (account && identity.Account !== account) {
      problems.push(`Your AWS credentials are for account ${identity.Account}, but the config deploys to ${account}. Switch profiles or fix "account".`);
    }
  }

  const tar = await runner.run("tar", ["--version"], { capture: true, allowFail: true, dryRunAnswer: "tar" }).catch(() => null);
  if (!tar || tar.code !== 0) problems.push("tar is needed to unpack the release.");
  return { problems, identity };
}
