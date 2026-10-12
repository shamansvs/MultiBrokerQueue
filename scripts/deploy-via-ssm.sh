#!/usr/bin/env bash
set -euo pipefail

: "${S3_BUCKET:?S3_BUCKET variable is required}"
: "${AWS_REGION:?AWS_REGION variable is required}"
: "${EC2_INSTANCE_ID:?EC2_INSTANCE_ID variable is required}"

artifact_path="${GITHUB_SHA}/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"

# Ці команди виконуватимуться на EC2.
remote_script='
set -euo pipefail
script_path=$(mktemp)
trap "rm -f -- \"$script_path\"" EXIT

aws s3 cp \
  "s3://$1/artifacts/$3/run-on-ec2.sh" \
  "$script_path" \
  --region "$2" \
  --only-show-errors

bash "$script_path" "$1" "$2" "$3"
'

parameters=$(jq -n \
  --arg script "$remote_script" \
  --arg bucket "$S3_BUCKET" \
  --arg region "$AWS_REGION" \
  --arg path "$artifact_path" \
  '{
    commands: [
      ([
        "runuser", "-u", "ssm-user", "--",
        "bash", "-c", $script, "multibroker",
        $bucket, $region, $path
      ] | @sh)
    ],
    executionTimeout: ["600"]
  }')

# Надсилаємо команду на EC2.
command_id=$(aws ssm send-command \
  --instance-ids "$EC2_INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "$parameters" \
  --timeout-seconds 60 \
  --query "Command.CommandId" \
  --output text)

echo "SSM command: $command_id"

# Чекаємо результату не довше 12 хвилин.
deadline=$((SECONDS + 720))

while (( SECONDS < deadline )); do
  if result=$(aws ssm get-command-invocation \
    --command-id "$command_id" \
    --instance-id "$EC2_INSTANCE_ID" \
    --output json 2>&1); then

    status=$(jq -r '.Status' <<< "$result")

    case "$status" in
      Success)
        jq -r '.StandardOutputContent, .StandardErrorContent' <<< "$result"
        jq -e '.ResponseCode == 0' <<< "$result" > /dev/null
        echo "Application finished successfully"
        exit 0
        ;;
      Pending|InProgress|Delayed|Cancelling)
        ;;
      *)
        jq -r '.StatusDetails, .StandardOutputContent, .StandardErrorContent' <<< "$result"
        exit 1
        ;;
    esac
  else
    if [[ "$result" != *InvocationDoesNotExist* ]]; then
      printf '%s\n' "$result" >&2
      exit 1
    fi
  fi

  sleep 5
done

echo "Timed out waiting for SSM command: $command_id" >&2
exit 1