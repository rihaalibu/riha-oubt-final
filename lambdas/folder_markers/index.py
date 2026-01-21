import boto3

s3 = boto3.client("s3")


def handler(event, context):
    bucket = event["ResourceProperties"]["Bucket"]
    prefixes = event["ResourceProperties"]["Prefixes"]

    for p in prefixes:
        s3.put_object(Bucket=bucket, Key=f"{p.rstrip('/')}/")

    return {"PhysicalResourceId": "FolderMarkers"}
