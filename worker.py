import boto3
import os
from PIL import Image
import requests
import hashlib
from urllib.parse import urlparse, unquote

# ======= Excuse my procedural coding style ========
# I felt it better to keep this sample as simple as possible, working under
# the assumption that many of those exploring this file will not have
# Python as one of their primary languages.
# So rather than going full OO and requiring the reader to understand Python
# name spacing and packaging, I've kept it to a single, procedural file,
# one that can easily be refactored to a more appropriate style.


def get_images_bucket_name():
    """
    Retrieves the name of the S3 image storage bucket from the environment

    :return: The AWS name of the bucket
    """
    if 'S3_BUCKET_NAME' in os.environ:
        s3_bucket_name = os.environ['S3_BUCKET_NAME'].strip()
    else:
        raise Exception('No "S3_BUCKET_NAME" environment variable found')
    return s3_bucket_name


def parse_img_src_url(image_src_url):
    """
    Normalizes the source image URL and also produces a local image name derived from said URL.

    :param image_src_url:
    :return: Dictionary of img_src_url: <image's source URL>, img_local_name: <Local image filename>
    """
    image_src_url = image_src_url.strip()
    source_image_url_parts = urlparse(image_src_url)
    source_image_basename = os.path.basename(unquote(source_image_url_parts.path))
    image_name = '{0}-{1}'.format(hashlib.md5(image_src_url.encode('utf-8')).hexdigest(), source_image_basename)
    return {
        'img_src_url': image_src_url,
        'img_local_name': image_name
    }


def process_image(source_image_url, original_img_path):
    # http://docs.python-requests.org/en/latest/_static/requests-sidebar.png
    image_request = requests.get(source_image_url, stream=True)
    if image_request.status_code == 200:
        with open(original_img_path, 'wb') as f:
            for chunk in image_request:
                f.write(chunk)

    size = (128, 128)

    path_parts = os.path.splitext(original_img_path)
    thumb_img_path = path_parts[0] + ".thumbnail"
    if original_img_path != thumb_img_path:
        try:
            im = Image.open(original_img_path)
            im.thumbnail(size)
            thumb_img_path = thumb_img_path + "." + im.format.lower()
            im.save(thumb_img_path, im.format)
            return thumb_img_path
        except IOError:
            print("cannot create thumbnail for", original_img_path)


def put_to_s3(s3_bucket_name, image_name, img_local_path):
    s3 = boto3.resource('s3')
    with open(img_local_path, 'rb') as image_bytes:
        # https://boto3.readthedocs.org/en/latest/reference/services/s3.html#S3.Client.put_object
        s3.Bucket(s3_bucket_name).put_object(Key=image_name, Body=image_bytes, ACL='public-read')


s3_bucket_name = get_images_bucket_name()

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
        image_source = parse_img_src_url(message_body)

        processed_image_path = process_image(image_source['img_src_url'], image_path)
        put_to_s3(s3_bucket_name,
                  "THUMB-{0}".format(image_source['img_local_name']),
                  processed_image_path)

        # Once you receive the message, you must delete it from the queue to acknowledge that you processed
        # the message and no longer need it
        # See: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/DeleteMessage.html
        message.delete()
