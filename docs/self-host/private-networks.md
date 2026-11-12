---
title: Private networks
description: Read a search cluster that is reachable only inside a VPC, such as an OpenSearch domain with a VPC endpoint. CoderHelm runs a small read-only lookup function in subnets you choose, added through the install's reviewed update.
order: 14.5
---

A self-hosted install's functions run outside any VPC, so a data source whose address is inside one (an OpenSearch domain with a `vpc-…` endpoint, a cluster on private addresses) can't be reached over the internet. A **private network** fixes that: CoderHelm deploys a small read-only lookup function into subnets of that VPC, and the data source reads through it.

Today a private network carries **search sources** (OpenSearch and Elasticsearch). Database sources don't need one: they read through the RDS and Redshift Data APIs, which work for databases in private subnets.

## What gets deployed

Each private network is one install stack parameter, `PrivateNetwork1`, `PrivateNetwork2` or `PrivateNetwork3`, with the value `vpc-<id>:subnet-<id>[,subnet-<id>...]` (one VPC, up to six of its subnets). Setting one adds, and clearing it removes:

| Resource | Name | What it may do |
|---|---|---|
| Lambda function | `coderhelm-<name>-reach-<n>` | Carries one lookup at a time: HTTPS only, only to names that resolve to private addresses, ports 443 and 9200, GET and POST, no redirects, 25 seconds and 2 MB at most |
| Security group | `coderhelm-<name>-reach-<n>` | Outbound tcp 443 and 9200 only; nothing inbound |
| IAM role | `coderhelm-<name>-reach-<n>` | Its own log, and the network interfaces Lambda places for it (its own code is denied those) |
| Log group | `/aws/lambda/coderhelm-<name>-reach-<n>` | 30 days |

Only the install's gateway and worker may invoke the function. It decides nothing about what is read: every rule of a search source (the indexes you listed, no scripts, size limits, hidden fields) is applied by the gateway and worker before a request is built, exactly as for a cluster on the internet.

The function needs no internet access and no NAT: it only talks to the cluster inside the VPC. Lambda reaches it through AWS's own API.

Two VPCs need two networks: a function sits in one VPC. A staging and a production domain in different VPCs use networks 1 and 2.

## Add one

1. **Data sources**, the search source, **Private network**. For an AWS OpenSearch address, CoderHelm describes the domain (`es:DescribeDomain`) and fills in its VPC and subnets. If a network already runs in that VPC, choose it; otherwise **Add private network** proposes the next free one with the domain's own subnets.
2. The install prepares a change set that changes only that stack parameter on the running release. It waits in **Settings, System** for an admin to confirm, like a version update, and lists exactly what it adds (the four resources above).
3. Once it has applied, pick the network on the source and **Test connection**.

The first time, an AWS admin applies the install's **access update** before step 2 can apply: the deploy role and the permissions boundary gain the calls that create the function's security group and network interfaces (EC2 `CreateSecurityGroup`, outbound rules, `CreateNetworkInterface` and the describes Lambda makes). **Settings, System, Permissions** has the one link. Until then the change set stops before it changes anything and says which call is missing.

## The security group rule

The cluster's own security group must let the function in on tcp 443. The source's card checks it (`ec2:DescribeSecurityGroups`, read-only):

- **Already allowed** when a rule allows 443 from the network's security group, or from address ranges that hold every one of the network's subnets. Putting the network in the domain's own subnets usually means a rule for those subnets already covers it.
- Otherwise the card shows the one rule to add, as a console link and as a command an AWS admin runs:

```bash
aws ec2 authorize-security-group-ingress --region <region> --group-id <the cluster's security group> \
  --ip-permissions 'IpProtocol=tcp,FromPort=443,ToPort=443,UserIdGroupPairs=[{GroupId=<the network's security group>,Description=CoderHelm private network read-only lookups}]'
```

CoderHelm never changes your cluster's security group itself.

## Test connection

For a source on a private network, **Test connection** checks each hop from inside the network and names the first that fails:

| Check | Fails when |
|---|---|
| An https address | The address isn't https, holds a user or password, is an IP address, or uses a port other than 443 or 9200 |
| Find the name inside the network | The name doesn't resolve (the domain was deleted or replaced), or resolves to a public address (it isn't private: read it over the internet) |
| Connect on the port (security groups) | Nothing answers: the cluster's security group or a network ACL drops the function |
| Secure connection (TLS) | The certificate isn't valid for the name |
| Sign in to the cluster | The user and password aren't accepted |
| Read the indexes | The user can't read the indexes you listed |

## When a domain moves or is deleted

AWS gives a domain a new address when it is replaced, and a domain moved into a VPC gets a `vpc-…` endpoint. A source still pointing at the old address says so: the address no longer exists, the domain may have been deleted or replaced. When the install's own account has a domain of the same name, or one with a similar name (for example `orders-os` for a deleted `orders-os2`), the message names its current endpoint and whether it is reachable only inside a VPC.

## On CoderHelm's hosted service

The hosted service runs outside your AWS account, so it can't reach inside your VPC. A private cluster needs a read-only https address on the internet, or a self-hosted install in the cluster's account with a private network.
