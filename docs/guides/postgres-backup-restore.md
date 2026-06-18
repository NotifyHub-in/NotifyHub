# Postgres Backup And Restore Verification

This guide explains how to verify that the NotifyHub database can be backed up and restored safely before public launch.

It covers two complementary checks:

1. a logical backup test using `pg_dump` and `pg_restore`
2. an AWS RDS snapshot restore test for the production database path

Use both if you can. The logical test is fast and easy to repeat. The snapshot test is the one that matters for production recovery.

## Why This Matters

The platform depends on Postgres for:

- notification requests
- delivery attempts
- scheduled retries
- dead letters
- provider accounts and bindings
- templates and routing policies
- webhook subscriptions and webhook delivery attempts
- channel events for WhatsApp replies and future inbound events

If restore is not tested, then a successful backup only proves that data was copied, not that it can actually be recovered.

## What You Need

- access to the AWS account that owns the production or staging database
- the RDS instance identifier
- a shell with `aws`, `psql`, `pg_dump`, and `pg_restore`
- the database connection string
- permission to create a temporary restore target

## Recommended Test Order

1. take a logical backup
2. restore the logical backup into a clean database
3. verify the restored data
4. take an RDS snapshot
5. restore the snapshot into a temporary instance
6. point a test control-plane instance at the restored database
7. run a smoke test against the restored stack

## Step 1: Identify The Database

Record the production or staging Postgres instance details:

- instance identifier
- region
- engine version
- VPC and subnet group
- security group
- username
- database name

Example:

```bash
export AWS_PROFILE=<aws-profile>
export AWS_REGION=ap-south-1
export RDS_INSTANclient_ID=notification-control-plane-postgres
export DB_NAME=notification_control_plane
export DB_USER=postgres
export DB_HOST=<your-rds-endpoint>
export DB_PORT=5432
```

## Step 2: Run A Logical Backup

Create a dump from the source database:

```bash
pg_dump \
  --format=custom \
  --no-owner \
  --no-acl \
  --file=notification-control-plane.backup.dump \
  "postgresql://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
```

If you want a plain SQL dump instead:

```bash
pg_dump \
  --no-owner \
  --no-acl \
  --file=notification-control-plane.backup.sql \
  "postgresql://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
```

## Step 3: Restore The Logical Backup Into A Fresh Database

Create a blank database on a test Postgres instance, then restore the dump.

Custom format:

```bash
createdb "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test"

pg_restore \
  --no-owner \
  --no-acl \
  --dbname="postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" \
  notification-control-plane.backup.dump
```

Plain SQL format:

```bash
psql \
  "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" \
  -f notification-control-plane.backup.sql
```

## Step 4: Verify The Restored Logical Backup

Run a small set of read checks on the restored database:

```bash
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT COUNT(*) FROM notification_requests;"
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT COUNT(*) FROM delivery_attempts;"
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT COUNT(*) FROM provider_bindings;"
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT COUNT(*) FROM webhook_subscriptions;"
```

Then verify a few representative records:

```bash
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT request_id, status, channel FROM notification_requests ORDER BY created_at DESC LIMIT 5;"
psql "postgresql://${DB_USER}@${TEST_DB_HOST}:${DB_PORT}/notification_control_plane_restore_test" -c "SELECT provider_key, channel, enabled FROM provider_bindings ORDER BY created_at DESC LIMIT 5;"
```

## Step 5: Take An RDS Snapshot

Create a manual snapshot of the source RDS instance:

```bash
export SNAPSHOT_ID=notification-control-plane-$(date +%Y%m%d-%H%M%S)

aws rds create-db-snapshot \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RDS_INSTANclient_ID" \
  --db-snapshot-identifier "$SNAPSHOT_ID"
```

Wait until it is ready:

```bash
aws rds wait db-snapshot-completed \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-snapshot-identifier "$SNAPSHOT_ID"
```

## Step 6: Restore The Snapshot Into A Temporary Instance

Restore the snapshot to a temporary RDS instance with a different name:

```bash
export RESTORE_INSTANclient_ID=notification-control-plane-restore-test

aws rds restore-db-instance-from-db-snapshot \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RESTORE_INSTANclient_ID" \
  --db-snapshot-identifier "$SNAPSHOT_ID" \
  --db-instance-class db.t3.small \
  --no-multi-az
```

Wait until the restored instance is available:

```bash
aws rds wait db-instance-available \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RESTORE_INSTANclient_ID"
```

## Step 7: Open Network Access To The Restore Target

Make sure the security group attached to the restored instance allows your test client or EKS cluster to reach port `5432`.

If you restore into a shared test VPC, update the security group rules temporarily for the test window only.

## Step 8: Point A Test Control Plane At The Restored Database

Use the restored instance endpoint in a temporary deployment or test namespace.

Example:

```bash
export RESTORED_DB_HOST=<restored-rds-endpoint>
export DATABASE_URL="postgres://${DB_USER}:<password>@${RESTORED_DB_HOST}:5432/${DB_NAME}?sslmode=require"
```

Then run a small control-plane smoke test against the restored database:

```bash
go test ./tests/integration -run TestNotificationRequestAcceptedFlow -count=1
```

Or run a targeted API smoke request if you are checking the restored data manually.

## Step 9: Verify That Important Data Survived

Confirm the restored control plane can still read the core records:

- notification requests
- delivery attempts
- provider accounts
- provider bindings
- callback routes
- templates
- routing policies
- webhook subscriptions
- channel events

Useful API checks:

```bash
curl -s http://localhost:8080/v1/provider-bindings
curl -s http://localhost:8080/v1/routing-policies
curl -s http://localhost:8080/v1/templates
curl -s http://localhost:8080/v1/webhook-subscriptions
curl -s "http://localhost:8080/v1/channel-events?limit=10"
```

## Step 10: Validate A Live Smoke Send After Restore

After the restored stack is up, send a tiny smoke notification through one production-supported channel:

- email
- SMS
- WhatsApp
- push

Then confirm:

- the API accepts the request
- the worker creates a delivery attempt
- the callback gateway can reconcile the callback if the provider sends one
- lifecycle webhooks still reach the consumer endpoint

## Cleanup After The Test

When you are done with the restore test:

1. delete the temporary database or restore instance
2. remove any temporary security-group changes
3. keep the snapshot if you want it as a recovery point
4. record the test outcome in your release checklist

Example cleanup:

```bash
aws rds delete-db-instance \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RESTORE_INSTANclient_ID" \
  --skip-final-snapshot
```

## What A Passing Test Looks Like

The backup/restore test is successful when:

- the dump restores without schema errors
- the restored instance starts cleanly
- the control plane can connect to it
- the API can read the expected rows
- a smoke notification works after restore
- no secrets or config values are corrupted by the restore

## What To Treat As A Failure

Treat the run as failed if:

- restore needs manual schema repair
- restored rows are missing or malformed
- the control plane boots but cannot read core tables
- provider bindings or secret references are lost
- notification smoke tests fail after restore

## Related Docs

- [AWS Operations Runbook](/docs/guides/aws-operations-runbook.md)
- [AWS Shutdown Runbook](/docs/guides/aws-shutdown-runbook.md)
- [Deploy To AWS EKS](/docs/guides/deploy-to-aws-eks.md)
- [Deploy To Production](/docs/guides/deploy-to-production.md)
