import boto3
from PIL import Image

# Create an S3 client
s3 = boto3.client('s3')
sqs = boto3.resource('sqs')

receive_bucket = 'apalia-image-store'
target_bucket = 'apalia-image-store-treated'

queue = sqs.get_queue_by_name(QueueName='terraform-example-queue')

def black_and_white(input_image_path,
                    output_image_path):
    color_image = Image.open(input_image_path)
    bw = color_image.convert('L')
    bw.save(output_image_path)



# Process messages by printing out body and optional author name
while True: 
    for message in queue.receive_messages(MessageAttributeNames=['Author']):
        # Get the custom author message attribute if it was set
        #with open('filename', 'wb') as data:
        local_filepath = '~/' + message.body
        s3.download_file(receive_bucket, message.body, message.body)

        print(message.body)
        new_file_path = "~/bw_"  + message.body
    	black_and_white(message.body, "bw_"  + message.body)
        s3.upload_file("bw_"  + message.body, target_bucket, "bw_"  + message.body)
        # Let the queue know that the message is processed
        message.delete()
