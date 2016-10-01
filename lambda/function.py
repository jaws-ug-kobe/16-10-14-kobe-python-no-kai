import boto3

S3_BUCKET = '16-10-14-kobe-python-no-kai'
SNS_TOPIC = '16-10-14-kobe-python-no-kai'

def lambda_handler(event, context):

	s3_object_body = get_s3_object_body(event)
	if not s3_object_body:
		return 'message is empty'
	else:
		published = publish_message_to_sns(s3_object_body.decode('utf-8'))

	return "published" if published == 200 else "failed"
	
def get_s3_object_body(event):

	s3 = boto3.resource('s3')
	s3_bucket = s3.Bucket(S3_BUCKET)
	
	s3_object_key = event['Records'][0]['s3']['object']['key']
	s3_object = s3_bucket.Object(s3_object_key)
	
	response = s3_object.get()
	return response['Body'].read()

def publish_message_to_sns(message):
	
	sns = boto3.resource('sns')
	sns_topic_arn = sns.create_topic(Name=SNS_TOPIC).arn
	
	response = sns.Topic(sns_topic_arn).publish(Subject="TEXT UPLOADED",Message=message)
  return response['ResponseMetadata']['HTTPStatusCode']
