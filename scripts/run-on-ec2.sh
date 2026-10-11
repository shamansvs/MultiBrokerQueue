#!/usr/bin/env bash
set -euo pipefail

bucket="${1:?S3 bucket is required}"
region="${2:?AWS region is required}"
artifact_path="${3:?Artifact path is required}"

base_dir="/home/ssm-user/multibroker"
config_path="$base_dir/application.properties"

if [[ ! -f "$config_path" ]]; then
  echo "Configuration file not found: $config_path" >&2
  exit 1
fi

# Не дозволяємо двом автоматичним запускам працювати одночасно.
exec 9>"$base_dir/run.lock"

if ! flock -n 9; then
  echo "Another application run is already in progress" >&2
  exit 1
fi

# Окрема директорія для кожного запуску.
mkdir -p "$base_dir/runs"
run_dir=$(mktemp -d "$base_dir/runs/run-XXXXXXXX")
cd "$run_dir"

echo "Working directory: $run_dir"

# Завантажуємо JAR саме з поточного запуску GitHub Actions.
aws s3 cp \
  "s3://$bucket/artifacts/$artifact_path/application.jar" \
  application.jar \
  --region "$region" \
  --only-show-errors

# Прибираємо завантажений JAR після завершення; CSV залишаємо.
trap 'rm -f -- "$run_dir/application.jar"' EXIT

# Обмежуємо виконання програми вісьмома хвилинами.
timeout --kill-after=15s 8m \
  java -Duser.timezone=Europe/Kyiv \
  -jar application.jar "$config_path"

# Завантаження виконається лише після успішного завершення Java.
results_uri="s3://$bucket/results/$artifact_path"

aws s3 cp valid.csv "$results_uri/valid.csv" \
  --region "$region" \
  --only-show-errors

aws s3 cp invalid.csv "$results_uri/invalid.csv" \
  --region "$region" \
  --only-show-errors

echo "Results uploaded to: $results_uri/"