#!/bin/bash

set -e
set -u 

EMAIL_ADDRESS=$1

# Create SNS topic and subscription
echo 'Creating SNS topic...'
SNS_TOPIC=$(aws sns create-topic --name test-error-alerts | jq .TopicArn | tr -d '"')
echo "Creating subscription for $EMAIL_ADDRESS"
SUBSCRIPTION=$(aws sns subscribe --topic-arn "${SNS_TOPIC}" --protocol email --notification-endpoint "${EMAIL_ADDRESS}" | jq .SubscriptionArn | tr -d '"')

echo 'Topic and subscription created'

echo "SNS Topic ARN: ${SNS_TOPIC}"
echo "Subscription: ${SUBSCRIPTION}"

set +u 
set +e 
