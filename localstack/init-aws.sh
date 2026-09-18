#!/bin/bash
set -euo pipefail

awslocal s3api create-bucket --bucket sec-13f-lake --region us-east-1

echo "LocalStack bootstrap complete: s3://sec-13f-lake created."
