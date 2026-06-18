# Release Images And ECR

This guide explains how image publishing works for the NotifyHub and how to use the release workflow with either the default registry or Amazon ECR.

## What This Guide Covers

- how the release workflow is triggered
- how versioned images are published
- how to publish to a custom registry
- how to publish to Amazon ECR with GitHub OIDC
- how Helm values should point at the published images

## Default Release Flow

The release workflow runs when you push a tag that starts with `v`, such as `v1.2.3`.

That workflow publishes images for:

- `api`
- `worker`
- `callback-gateway`
- `migrate`
- `connector-email`
- `connector-sms`
- `connector-webhook`
- `connector-push`
- `connector-whatsapp`

The published tag is the semantic version without the leading `v`.

Example:

```bash
git tag v1.2.3
git push origin v1.2.3
```

## Manual Release Dispatch

You can also run the workflow manually from GitHub Actions.

Provide:

- `version`
- `registry`
- `namespace`

Optional ECR-specific inputs:

- `aws-region`
- `aws-role-arn`

## Registry Modes

### 1. GHCR

If you do not override `registry`, the workflow publishes to GHCR under the repository namespace.

Example image name:

```text
ghcr.io/<owner>/<repo>/api:1.2.3
```

### 2. Custom Registry With Username And Password

For non-ECR registries, provide these repository or environment secrets:

- `REGISTRY_USERNAME`
- `REGISTRY_PASSWORD`

Use this mode for registries that expect direct username/password login.

Example manual inputs:

- `version = v1.2.3`
- `registry = registry.example.com`
- `namespace = platform/notification-control-plane`

Example image name:

```text
registry.example.com/platform/notification-control-plane/api:1.2.3
```

### 3. Amazon ECR With GitHub OIDC

If the registry host matches ECR, the workflow switches to AWS federation automatically.

To use this mode:

1. Create an IAM role that trusts GitHub Actions OIDC.
2. Allow that role to push to the target ECR repository.
3. Pass the role ARN and AWS region when you dispatch the workflow.

Example manual inputs:

- `version = v1.2.3`
- `registry = 123456789012.dkr.ecr.ap-south-1.amazonaws.com`
- `namespace = platform/notification-control-plane`
- `aws-region = ap-south-1`
- `aws-role-arn = arn:aws:iam::123456789012:role/github-actions-ecr-publish`

Example image name:

```text
123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/api:1.2.3
```

## Helm Image Values

The Helm chart should point at the same registry and tag for every image.

Example:

```yaml
images:
  api:
    repository: 123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/api
    tag: "1.2.3"
  worker:
    repository: 123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/worker
    tag: "1.2.3"
  callback-gateway:
    repository: 123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/callback-gateway
    tag: "1.2.3"
```

## Recommended Release Checklist

Before you deploy a release:

1. Run the test suite.
2. Confirm the image build validation job is green.
3. Publish the version tag or run the manual release dispatch.
4. Update Helm values to point at the same tag.
5. Install or upgrade the chart.
6. Run a smoke notification after deploy.

## Notes

- The workflow does not store ECR passwords.
- GHCR uses the built-in GitHub token.
- Non-ECR registries still use explicit registry credentials.
