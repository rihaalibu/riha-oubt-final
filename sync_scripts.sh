#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Config (edit these)
# -----------------------------

AWS_REGION=us-east-1
BUCKET_NAME=rihastoragestack-s3datalakee27c7386-ukc8jklwd0uj
S3_PREFIX=scripts
LOCAL_DIR=./glue_jobs

# -----------------------------
# Sanity checks
# -----------------------------
if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "❌ Local directory not found: $LOCAL_DIR"
    exit 1
fi

echo "🔄 Syncing Glue scripts..."
echo "   Local : $LOCAL_DIR"
echo "   S3    : s3://$BUCKET_NAME/$S3_PREFIX/"

# -----------------------------
# Sync
# -----------------------------
aws s3 sync "$LOCAL_DIR" "s3://$BUCKET_NAME/$S3_PREFIX/" \
    --region "$AWS_REGION" \
    --exact-timestamps \
    --only-show-errors \
    --sse aws:kms

echo "✅ Glue scripts synced successfully"
