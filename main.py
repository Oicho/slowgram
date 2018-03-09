import os
from google.cloud import pubsub
from google.cloud import storage
from PIL import Image


#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\Oicho\\PycharmProjects\\pubsub\\credentials.json"

client = storage.Client(project='Apalia')
bucket_source = client.get_bucket('apalia_image_store_bucket')

bucket_dest = client.get_bucket('apalia_image_store_treated_bucket')


subscriber = pubsub.SubscriberClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id="hip-myth-197414",
    topic='images',  # Set this to something appropriate.
)
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id="hip-myth-197414",
    sub='images',  # Set this to something appropriate.
)
try:
    subscriber.create_subscription(
        name=subscription_name, topic=topic_name)
    print("Adding Subscription " + subscription_name)
except :
    print("Subscription already exists. SUBSCRIBING")

subscription = subscriber.subscribe(subscription_name)


def black_and_white(input_image_path,
                    output_image_path):
    color_image = Image.open(input_image_path)
    bw = color_image.convert('L')
    bw.save(output_image_path)




def callback(message):
    file_name = message.data.decode("utf-8")
    file_name = file_name.replace("New file to process", "").replace("'", "").replace('"', '')
    print(file_name)
    blob = bucket_source.get_blob(file_name)
    local_filepath = os.getcwd() + os.sep + file_name
    with open(local_filepath, 'wb') as file_obj:
        blob.download_to_file(file_obj)
        file_obj.close()
    print(file_name + " Downloaded")
    new_file_name = "bw_" + file_name
    black_and_white(file_name, new_file_name)
    blob2 = bucket_dest.blob(new_file_name)
    blob2.upload_from_filename(new_file_name)

    print(new_file_name + " Uploaded")


    message.ack()

print("Listening....")
future = subscription.open(callback)
future.result()
