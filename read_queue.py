import boto3
import os
from PIL import Image
import requests


def process_image(source_image_url, image_output_dir):
    # http://docs.python-requests.org/en/latest/_static/requests-sidebar.png
    image_request = requests.get(source_image_url, stream=True)
    if image_request.status_code == 200:
        with open(image_output_dir, 'wb') as f:
            for chunk in image_request:
                f.write(chunk)

    size = (128, 128)

    path_parts = os.path.splitext(image_output_dir)
    outfile = path_parts[0] + ".thumbnail"
    if image_output_dir != outfile:
        try:
            im = Image.open(image_output_dir)
            im.thumbnail(size)
            outfile = outfile + "." + im.format.lower()
            im.save(outfile, im.format)
        except IOError:
            print("cannot create thumbnail for", image_output_dir)


# see https://boto3.readthedocs.org/en/latest/guide/sqs.html
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='ImageManipulationWorkQueue')
print('Queue URL: {0}'.format(queue.url))

here = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(here, 'images')

# Infinite loop which will continually watch work queue for new work items
while True:
    print('Looking for Work Item...')
    # read messages
    # see http://boto3.readthedocs.org/en/latest/reference/services/sqs.html#SQS.Client.receive_message
    for message in queue.receive_messages(WaitTimeSeconds=10, MaxNumberOfMessages=1):
        message_body = message.body
        # Print out the body and author (if set)
        print('Found Work Item, Body: {0}'.format(message_body))

        # get reference to the download image directory
        this_directory = os.path.abspath(os.path.dirname(__file__))
        image_path = os.path.join(this_directory, 'images', 'downloaded_image')
        process_image(message_body, image_path)

        # Once you receive the message, you must delete it from the queue to acknowledge that you processed
        # the message and no longer need it
        # See: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/DeleteMessage.html
        message.delete()
