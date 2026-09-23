#!/usr/bin/env python3
"""
Setup script for data-notebooks example on IdC-based domains.

Ensures the VPC networking (S3 gateway endpoint, NAT gateway) and
Lake Formation permissions are configured for SageMaker notebook
workflow execution.

All operations are idempotent — safe to run multiple times.

Usage:
    python idc_domain_project_setup.py \
        --manifest example-notebooks/manifest.yaml

    # Dry run:
    python idc_domain_project_setup.py \
        --manifest example-notebooks/manifest.yaml --dry-run
"""

import argparse
import os
import re
import sys
import time

import boto3
import yaml

DATABASE_NAME = "sagemaker_sample_db"
STAGE_NAME = "test-idc"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def substitute_env_vars(text):
    """Substitute ${VAR} and ${VAR:default} patterns."""

    def replace(match):
        expr = match.group(1)
        if ":" in expr:
            var_name, default = expr.split(":", 1)
            return os.environ.get(var_name, default)
        return os.environ.get(expr, match.group(0))

    return re.sub(r"\$\{([^}]+)\}", replace, text)


def load_manifest(path):
    with open(path) as f:
        return yaml.safe_load(substitute_env_vars(f.read()))


# ---------------------------------------------------------------------------
# Project / domain resolution
# ---------------------------------------------------------------------------


def get_project_info(domain_id, project_name, region):
    """Resolve project role ARN and VPC info from the tooling environment."""
    dz = boto3.client("datazone", region_name=region)

    projects = dz.list_projects(domainIdentifier=domain_id)
    project_id = None
    for p in projects.get("items", []):
        if p["name"] == project_name:
            project_id = p["id"]
            break
    if not project_id:
        print(f"❌ Project '{project_name}' not found")
        return None

    print(f"📋 Project: {project_name} ({project_id})")

    envs = dz.list_environments(
        domainIdentifier=domain_id, projectIdentifier=project_id
    )
    for env in envs.get("items", []):
        env_name = env.get("name", "").lower()
        if "tooling" not in env_name and "tool" not in env_name:
            continue
        detail = dz.get_environment(domainIdentifier=domain_id, identifier=env["id"])
        resources = {
            r["name"]: r["value"] for r in detail.get("provisionedResources", [])
        }
        role_arn = resources.get("userRoleArn")
        vpc_id = resources.get("vpcId")
        sm_domain_id = resources.get("sageMakerDomainId")
        if role_arn:
            print(f"   Role: {role_arn}")
            if vpc_id:
                print(f"   VPC:  {vpc_id}")
            return {
                "project_id": project_id,
                "role_arn": role_arn,
                "vpc_id": vpc_id,
                "sm_domain_id": sm_domain_id,
            }

    print("❌ No userRoleArn found in tooling environment")
    return None


def is_idc_domain(role_arn):
    return role_arn.split("/")[-1].startswith("datazone_usr_role_")


def get_sagemaker_domain_vpc(sm_domain_id, region):
    """Get VPC info from SageMaker domain if not in provisioned resources."""
    sm = boto3.client("sagemaker", region_name=region)
    resp = sm.describe_domain(DomainId=sm_domain_id)
    return {
        "vpc_id": resp.get("VpcId"),
        "subnet_ids": resp.get("SubnetIds", []),
        "network_mode": resp.get("AppNetworkAccessType"),
    }


# ---------------------------------------------------------------------------
# VPC networking setup (S3 gateway endpoint + NAT gateway)
# ---------------------------------------------------------------------------


def setup_vpc_interface_endpoints(vpc_id, region, workflow_sg_ids=None, dry_run=False):
    """Ensure VPC has required interface endpoints for notebook execution.

    In VpcOnly mode with network isolation, notebook containers need
    VPC endpoints to reach AWS services. Without these, the Jupyter
    kernel times out trying to start (60s timeout).

    ``workflow_sg_ids`` are the security groups the MWAA Serverless workers use
    (from the domain-scoped VPC connection). They are authorized inbound 443 on
    the endpoint SG so the workers can actually reach the endpoints.
    """
    workflow_sg_ids = workflow_sg_ids or []
    ec2 = boto3.client("ec2", region_name=region)

    print(f"\n🔌 VPC interface endpoints setup for {vpc_id}")

    required_services = [
        f"com.amazonaws.{region}.sagemaker.runtime",
        f"com.amazonaws.{region}.ecr.api",
        f"com.amazonaws.{region}.ecr.dkr",
        f"com.amazonaws.{region}.logs",
        f"com.amazonaws.{region}.sagemaker.api",
        f"com.amazonaws.{region}.sts",
        f"com.amazonaws.{region}.datazone",
    ]

    # Get existing endpoints
    endpoints = ec2.describe_vpc_endpoints(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["VpcEndpoints"]
    existing_services = {
        e["ServiceName"] for e in endpoints if e["State"] in ("available", "pending")
    }

    # Get subnets and security groups
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])[
        "Subnets"
    ]
    subnet_ids = [s["SubnetId"] for s in subnets]

    # Find the VPC endpoint security group (or the environment SG)
    sgs = ec2.describe_security_groups(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["SecurityGroups"]

    # Look for existing endpoint SG (used by existing interface endpoints)
    endpoint_sg_id = None
    env_sg_id = None
    for ep in endpoints:
        if ep.get("VpcEndpointType") == "Interface":
            for g in ep.get("Groups", []):
                endpoint_sg_id = g["GroupId"]
                break
            if endpoint_sg_id:
                break

    # Find the environment/project SG (datazone-* named)
    for sg in sgs:
        if sg["GroupName"].startswith("datazone-"):
            env_sg_id = sg["GroupId"]
            break

    # If no endpoint SG found, use the environment SG
    sg_for_endpoints = endpoint_sg_id or env_sg_id
    if not sg_for_endpoints:
        print("   ❌ No suitable security group found for VPC endpoints")
        return

    print(f"   Endpoint SG: {sg_for_endpoints}")
    if env_sg_id and env_sg_id != sg_for_endpoints:
        print(f"   Environment SG: {env_sg_id}")

    # Ensure endpoint SG allows inbound 443 from every SG that reaches the
    # endpoints. This MUST include the workflow's VPC-connection security group
    # (workflow_sg_ids): MWAA Serverless workers run with that SG, and if it is
    # not allowed inbound on the endpoints, every AWS API call the worker makes
    # via PrivateLink (sagemaker.api, sts, datazone, ...) silently times out and
    # the notebook task hangs without ever launching a job or emitting logs.
    source_sgs = []
    if env_sg_id:
        source_sgs.append(env_sg_id)
    source_sgs.extend(workflow_sg_ids or [])
    if endpoint_sg_id:
        for src_sg in dict.fromkeys(source_sgs):  # de-dup, preserve order
            if src_sg and src_sg != endpoint_sg_id:
                _ensure_sg_ingress(ec2, endpoint_sg_id, src_sg, dry_run)

    # Create missing endpoints
    for service in required_services:
        if service in existing_services:
            print(f"   ✓ {service.split('.')[-1]} endpoint exists")
            continue
        if dry_run:
            print(f"   [DRY RUN] Would create {service.split('.')[-1]} endpoint")
            continue
        try:
            resp = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                VpcEndpointType="Interface",
                ServiceName=service,
                SubnetIds=subnet_ids,
                SecurityGroupIds=[sg_for_endpoints],
                PrivateDnsEnabled=True,
            )
            ep_id = resp["VpcEndpoint"]["VpcEndpointId"]
            print(f"   ✅ Created {service.split('.')[-1]} endpoint: {ep_id}")
        except ec2.exceptions.ClientError as e:
            if "already exists" in str(e).lower() or "RouteAlreadyExists" in str(e):
                print(f"   ✓ {service.split('.')[-1]} endpoint already exists")
            else:
                print(f"   ⚠️ Failed to create {service.split('.')[-1]}: {e}")


def _ensure_sg_ingress(ec2, endpoint_sg_id, env_sg_id, dry_run=False):
    """Ensure the endpoint SG allows inbound HTTPS from the environment SG."""
    sg = ec2.describe_security_groups(GroupIds=[endpoint_sg_id])["SecurityGroups"][0]
    for rule in sg.get("IpPermissions", []):
        if rule.get("FromPort") == 443 or rule.get("IpProtocol") == "-1":
            for pair in rule.get("UserIdGroupPairs", []):
                if pair["GroupId"] == env_sg_id:
                    print(f"   ✓ Endpoint SG already allows inbound from env SG")
                    return

    if dry_run:
        print(
            f"   [DRY RUN] Would add inbound 443 rule from {env_sg_id} to {endpoint_sg_id}"
        )
        return

    ec2.authorize_security_group_ingress(
        GroupId=endpoint_sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 443,
                "ToPort": 443,
                "UserIdGroupPairs": [{"GroupId": env_sg_id}],
            }
        ],
    )
    print(
        f"   ✅ Added inbound 443 from env SG ({env_sg_id}) to endpoint SG ({endpoint_sg_id})"
    )


def get_workflow_network_config(domain_id, region):
    """Resolve the network config MWAA Serverless workflows actually run with.

    Workflows inherit their subnets AND security group from the domain-scoped
    VPC connection (the same source the SMUS CI/CD CLI resolves). These are:
      - the subnets that MUST have a NAT route so notebooks reach the internet
        (pip installs, external datasets), and
      - the security group the workers use, which MUST be allowed inbound on
        the interface VPC endpoints or every AWS API call from the worker
        silently times out.

    Returns (subnet_ids, security_group_ids). Both empty if no VPC connection.
    """
    dz = boto3.client("datazone", region_name=region)
    try:
        resp = dz.list_connections(
            domainIdentifier=domain_id, type="VPC", scope="DOMAIN"
        )
    except Exception as e:
        print(f"   ⚠️ Could not list VPC connections: {e}")
        return [], []

    subnet_ids = []
    security_group_ids = []
    for conn in resp.get("items", []):
        vpc_props = (conn.get("props") or {}).get("vpcProperties") or {}
        subnet_ids.extend(s for s in (vpc_props.get("subnetIds") or []) if s)
        sg = vpc_props.get("securityGroupId")
        if sg:
            security_group_ids.append(sg)
    # De-duplicate while preserving order.
    return (
        list(dict.fromkeys(subnet_ids)),
        list(dict.fromkeys(security_group_ids)),
    )


def setup_vpc_networking(vpc_id, region, workflow_subnet_ids=None, dry_run=False):
    """Ensure VPC has S3 gateway endpoint and NAT gateway. Idempotent.

    ``workflow_subnet_ids`` are the subnets the MWAA Serverless workflow runs
    in (from the domain-scoped VPC connection). These are guaranteed a NAT
    route so notebooks retain outbound internet access even though the subnets
    are private (MWAA requires private subnets). The NAT gateway is never
    placed in one of these subnets.
    """
    ec2 = boto3.client("ec2", region_name=region)
    workflow_subnet_ids = workflow_subnet_ids or []

    print(f"\n🌐 VPC networking setup for {vpc_id}")
    if workflow_subnet_ids:
        print(f"   Workflow subnets (need NAT egress): {workflow_subnet_ids}")

    # --- Gather existing state ---
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])[
        "Subnets"
    ]
    subnet_ids = [s["SubnetId"] for s in subnets]

    route_tables = ec2.describe_route_tables(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["RouteTables"]

    endpoints = ec2.describe_vpc_endpoints(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["VpcEndpoints"]

    nat_gateways = ec2.describe_nat_gateways(
        Filter=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["NatGateways"]
    active_nats = [n for n in nat_gateways if n["State"] in ("available", "pending")]

    # Find the main route table (with IGW route — used for the NAT gateway subnet)
    main_rt = None
    for rt in route_tables:
        for assoc in rt.get("Associations", []):
            if assoc.get("Main"):
                main_rt = rt
                break

    # --- S3 Gateway Endpoint ---
    s3_service = f"com.amazonaws.{region}.s3"
    s3_endpoints = [
        e
        for e in endpoints
        if e["ServiceName"] == s3_service and e["State"] == "available"
    ]

    if s3_endpoints:
        print(f"   ✓ S3 gateway endpoint exists: {s3_endpoints[0]['VpcEndpointId']}")
    elif dry_run:
        print(f"   [DRY RUN] Would create S3 gateway endpoint")
    else:
        all_rt_ids = [rt["RouteTableId"] for rt in route_tables]
        resp = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName=s3_service,
            RouteTableIds=all_rt_ids,
            VpcEndpointType="Gateway",
        )
        print(
            f"   ✅ Created S3 gateway endpoint: {resp['VpcEndpoint']['VpcEndpointId']}"
        )

    # --- NAT Gateway ---
    if active_nats:
        nat = active_nats[0]
        print(f"   ✓ NAT gateway exists: {nat['NatGatewayId']} ({nat['State']})")
        nat_gw_id = nat["NatGatewayId"]
        nat_subnet = nat["SubnetId"]
    elif dry_run:
        print(f"   [DRY RUN] Would create NAT gateway + EIP")
        return
    else:
        # Pick a subnet for the NAT gateway. It must NOT be a workflow subnet
        # (those must stay private, routing through the NAT — not host it) and
        # must be able to reach the internet gateway. Prefer a subnet on the
        # main/IGW route table; fall back to any non-workflow subnet.
        main_subnet_ids = [
            s for s in subnet_ids if s not in workflow_subnet_ids
        ] or subnet_ids
        nat_subnet = main_subnet_ids[0]
        eip = ec2.allocate_address(Domain="vpc")
        print(f"   ✅ Allocated EIP: {eip['AllocationId']}")

        nat_resp = ec2.create_nat_gateway(
            SubnetId=nat_subnet, AllocationId=eip["AllocationId"]
        )
        nat_gw_id = nat_resp["NatGateway"]["NatGatewayId"]
        print(f"   ⏳ Creating NAT gateway: {nat_gw_id} (waiting...)")

        waiter = ec2.get_waiter("nat_gateway_available")
        waiter.wait(NatGatewayIds=[nat_gw_id])
        print(f"   ✅ NAT gateway available: {nat_gw_id}")

    # --- Private route table for subnets that need NAT egress ---
    # The workflow subnets MUST route through the NAT (they run the notebooks
    # that need internet). We associate them first/explicitly rather than
    # relying on "all subnets except the NAT's", which previously missed the
    # exact subnets the workflow used. The NAT's own subnet is excluded (it
    # must keep its IGW route so the NAT can reach the internet).
    priority_subnets = [s for s in workflow_subnet_ids if s != nat_subnet]
    other_subnets = [
        s for s in subnet_ids if s != nat_subnet and s not in priority_subnets
    ]
    # Workflow subnets first, then the rest (preserves prior broad behavior).
    private_subnets = priority_subnets + other_subnets

    if priority_subnets:
        print(f"   Ensuring NAT route for workflow subnets: {priority_subnets}")
    elif workflow_subnet_ids:
        print(
            "   ⚠️ Workflow subnet(s) coincide with the NAT subnet; "
            "verify NAT placement manually"
        )

    if not private_subnets:
        print(f"   ✓ No additional subnets to configure")
        return

    # Check if a private route table with NAT route already exists
    private_rt = None
    for rt in route_tables:
        if rt == main_rt:
            continue
        for route in rt.get("Routes", []):
            if route.get("NatGatewayId") == nat_gw_id:
                private_rt = rt
                break

    if private_rt:
        print(f"   ✓ Private route table exists: {private_rt['RouteTableId']}")
    elif dry_run:
        print(f"   [DRY RUN] Would create private route table with NAT route")
        return
    else:
        rt_resp = ec2.create_route_table(VpcId=vpc_id)
        private_rt = rt_resp["RouteTable"]
        rt_id = private_rt["RouteTableId"]
        ec2.create_route(
            RouteTableId=rt_id,
            DestinationCidrBlock="0.0.0.0/0",
            NatGatewayId=nat_gw_id,
        )
        print(f"   ✅ Created private route table: {rt_id} with NAT route")

        # Add S3 endpoint to the new route table
        s3_endpoints_now = ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": [s3_service]},
            ]
        )["VpcEndpoints"]
        if s3_endpoints_now:
            ec2.modify_vpc_endpoint(
                VpcEndpointId=s3_endpoints_now[0]["VpcEndpointId"],
                AddRouteTableIds=[rt_id],
            )
            print(f"   ✅ Added S3 endpoint to private route table")

    # Associate private subnets with the private route table
    rt_id = private_rt["RouteTableId"]
    existing_assoc_subnets = {
        a["SubnetId"] for a in private_rt.get("Associations", []) if a.get("SubnetId")
    }

    for subnet_id in private_subnets:
        if subnet_id in existing_assoc_subnets:
            print(f"   ✓ Subnet {subnet_id} already associated")
            continue
        if dry_run:
            print(f"   [DRY RUN] Would associate {subnet_id}")
            continue
        ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_id)
        print(f"   ✅ Associated {subnet_id} with private route table")


# ---------------------------------------------------------------------------
# Lake Formation permissions
# ---------------------------------------------------------------------------


def ensure_lakeformation_admin(region):
    """Ensure the current caller is a Lake Formation admin."""
    lf = boto3.client("lakeformation", region_name=region)
    sts = boto3.client("sts")

    caller_arn = sts.get_caller_identity()["Arn"]
    if "assumed-role" in caller_arn:
        parts = caller_arn.split("/")
        role_arn = (
            parts[0].replace(":sts:", ":iam:").replace(":assumed-role", ":role")
            + "/"
            + parts[1]
        )
    else:
        role_arn = caller_arn

    settings = lf.get_data_lake_settings()
    admins = settings.get("DataLakeSettings", {}).get("DataLakeAdmins", [])
    admin_arns = [a.get("DataLakePrincipalIdentifier") for a in admins]

    if role_arn in admin_arns:
        print(f"   ✓ Caller is already a Lake Formation admin")
        return
    admins.append({"DataLakePrincipalIdentifier": role_arn})
    lf.put_data_lake_settings(DataLakeSettings={"DataLakeAdmins": admins})
    print(f"   ✅ Added {role_arn} as Lake Formation admin")


def has_lf_permission(lf, role_arn, resource, required_perms):
    """Check if a Lake Formation grant already exists."""
    try:
        resp = lf.list_permissions(
            Principal={"DataLakePrincipalIdentifier": role_arn},
            Resource=resource,
        )
        for entry in resp.get("PrincipalResourcePermissions", []):
            granted = set(entry.get("Permissions", []))
            if set(required_perms).issubset(granted):
                return True
    except Exception:
        pass
    return False


def setup_lakeformation(role_arn, region, dry_run=False):
    """Grant Lake Formation permissions on sagemaker_sample_db. Idempotent."""
    lf = boto3.client("lakeformation", region_name=region)
    glue = boto3.client("glue", region_name=region)

    print(f"\n🔐 Lake Formation setup for {DATABASE_NAME}")

    ensure_lakeformation_admin(region)

    # Verify database exists
    try:
        glue.get_database(Name=DATABASE_NAME)
    except Exception:
        print(f"   ❌ Database '{DATABASE_NAME}' does not exist — skipping LF grants")
        return False

    # Database DESCRIBE
    db_resource = {"Database": {"Name": DATABASE_NAME}}
    if has_lf_permission(lf, role_arn, db_resource, ["DESCRIBE"]):
        print(f"   ✓ Database DESCRIBE already granted")
    elif dry_run:
        print(f"   [DRY RUN] Would grant DESCRIBE on database")
    else:
        try:
            lf.grant_permissions(
                Principal={"DataLakePrincipalIdentifier": role_arn},
                Resource=db_resource,
                Permissions=["DESCRIBE"],
            )
            print(f"   ✅ Granted DESCRIBE on database")
        except Exception as e:
            print(f"   ⚠️ Database grant: {e}")

    # Table permissions
    try:
        tables = glue.get_tables(DatabaseName=DATABASE_NAME).get("TableList", [])
    except Exception as e:
        print(f"   ❌ Error listing tables: {e}")
        return False

    for table in tables:
        name = table["Name"]
        tbl_resource = {"Table": {"DatabaseName": DATABASE_NAME, "Name": name}}
        if has_lf_permission(lf, role_arn, tbl_resource, ["SELECT", "DESCRIBE"]):
            print(f"   ✓ Table {name}: SELECT, DESCRIBE already granted")
        elif dry_run:
            print(f"   [DRY RUN] Would grant SELECT, DESCRIBE on table {name}")
        else:
            try:
                lf.grant_permissions(
                    Principal={"DataLakePrincipalIdentifier": role_arn},
                    Resource=tbl_resource,
                    Permissions=["SELECT", "DESCRIBE"],
                )
                print(f"   ✅ Granted SELECT, DESCRIBE on table {name}")
            except Exception as e:
                print(f"   ⚠️ Table {name}: {e}")

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Setup IdC domain infrastructure for data-notebooks example"
    )
    parser.add_argument(
        "--manifest",
        default="examples/analytic-workflow/data-notebooks/manifest-idc.yaml",
        help="Path to manifest YAML",
    )
    parser.add_argument(
        "--stage", default=STAGE_NAME, help="Stage name (default: test-idc)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done"
    )
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    stage_config = manifest.get("stages", {}).get(args.stage)
    if not stage_config:
        print(f"❌ Stage '{args.stage}' not found in manifest")
        sys.exit(1)

    domain_config = stage_config["domain"]
    region = domain_config["region"]
    project_name = stage_config["project"]["name"]

    # Resolve domain ID from id or name
    domain_id = domain_config.get("id")
    if not domain_id:
        domain_name = domain_config.get("name")
        if not domain_name:
            print("❌ Stage must have domain.id or domain.name")
            sys.exit(1)
        dz = boto3.client("datazone", region_name=region)
        domains = dz.list_domains()
        for d in domains.get("items", []):
            if d["name"] == domain_name:
                domain_id = d["id"]
                break
        if not domain_id:
            print(f"❌ Domain '{domain_name}' not found in {region}")
            sys.exit(1)

    print(f"📋 Domain: {domain_id} ({region})")

    info = get_project_info(domain_id, project_name, region)
    if not info:
        sys.exit(1)

    if not is_idc_domain(info["role_arn"]):
        print(f"\n✓ IAM domain detected — no IdC-specific setup needed")
        sys.exit(0)

    print(f"\n🔍 IdC domain detected")

    # Resolve VPC from SageMaker domain if not in provisioned resources
    vpc_id = info.get("vpc_id")
    if not vpc_id and info.get("sm_domain_id"):
        sm_info = get_sagemaker_domain_vpc(info["sm_domain_id"], region)
        vpc_id = sm_info.get("vpc_id")
        if sm_info.get("network_mode") == "PublicInternetOnly":
            print(f"   ✓ PublicInternetOnly mode — VPC networking setup not needed")
            vpc_id = None

    # 1. VPC networking
    if vpc_id:
        # Resolve the subnets + security group the workflow actually runs with
        # (from the domain-scoped VPC connection). The subnets need a NAT route
        # (private but internet-capable) and the SG must be allowed inbound on
        # the interface endpoints, or worker API calls silently time out.
        workflow_subnet_ids, workflow_sg_ids = get_workflow_network_config(
            domain_id, region
        )
        setup_vpc_networking(
            vpc_id,
            region,
            workflow_subnet_ids=workflow_subnet_ids,
            dry_run=args.dry_run,
        )
        setup_vpc_interface_endpoints(
            vpc_id,
            region,
            workflow_sg_ids=workflow_sg_ids,
            dry_run=args.dry_run,
        )
    else:
        print(f"\n🌐 No VPC — skipping networking setup")

    # 2. Lake Formation permissions
    setup_lakeformation(info["role_arn"], region, dry_run=args.dry_run)

    print(f"\n✅ Setup complete")


if __name__ == "__main__":
    main()
