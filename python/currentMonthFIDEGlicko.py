import json
import boto3
import datetime

def lambda_handler(event, context):
    # Get current UTC date
    now = datetime.datetime.utcnow()
    year = now.year
    month = now.month

    # Create the input JSON for the step function
    input_payload = {
        "year": 2024,
        "month": month
    }

    # Create a Step Functions client
    sf_client = boto3.client('stepfunctions')

    # Start execution of the step function with the input payload
    response = sf_client.start_execution(
        stateMachineArn='arn:aws:states:us-east-2:961341531973:stateMachine:FideScraperFlow',
        input=json.dumps(input_payload)
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Step function execution started.",
            "executionArn": response.get('executionArn')
        })
    }
