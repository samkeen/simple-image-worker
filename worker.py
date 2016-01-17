import boto3
import os
from PIL import Image
import requests
import json


# ======= Excuse my procedural coding style ========
# I felt it better to keep this sample as simple as possible, working under the assumption that many of those
# exploring this file may not have Python as one of their primary languages.
#
# So rather than going full Object Oriented and requiring the reader to understand Python name spacing and
# packaging, and Object model,  I've kept it to a single, procedural file, one that can easily be refactored to a
# more appropriate style by the reader.


def get_required_env_var(var_name):
    """Retrieves an env var's value from the environment, RuntimeError if not found

    :param var_name:
    :return: String ENV var's value
    """
    if var_name in os.environ:
        value = os.environ[var_name].strip()
    else:
        raise RuntimeError('No "{}" environment variable found'.format(var_name))
    return value


def download_image(source_image_url, download_image_name):
    local_img_path = os.path.join(this_directory, 'images', download_image_name)
    image_request = requests.get(source_image_url, stream=True)
    if image_request.status_code == 200:
        with open(local_img_path, 'wb') as f:
            for chunk in image_request:
                f.write(chunk)
    return local_img_path


def process_image(original_img_path):
    size = (128, 128)
    path_parts = os.path.splitext(original_img_path)
    thumb_img_path = path_parts[0] + ".thumbnail"
    if original_img_path != thumb_img_path:
        im = Image.open(original_img_path)
        im.thumbnail(size)
        thumb_img_path = thumb_img_path + "." + im.format.lower()
        im.save(thumb_img_path, im.format)
        return thumb_img_path


def cleanup_local_images(downloaded_img_local_path, processed_image_path):
    if os.path.isfile(downloaded_img_local_path):
        os.remove(downloaded_img_local_path)
    if os.path.isfile(processed_image_path):
        os.remove(processed_image_path)


def put_to_s3(s3_bucket_name, image_name, img_local_path):
    s3 = boto3.resource('s3')
    with open(img_local_path, 'rb') as image_bytes:
        # https://boto3.readthedocs.org/en/latest/reference/services/s3.html#S3.Client.put_object
        s3.Bucket(s3_bucket_name).put_object(Key=image_name, Body=image_bytes, ACL='public-read')


s3_bucket_name = get_required_env_var('S3_BUCKET_NAME')
sqs_name = get_required_env_var('WORK_QUEUE_NAME')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=sqs_name)
print('Targeting Queue "{}" at URL {}'.format(sqs_name, queue.url))

"""
Workload messages are JSON in this format:
{
  "img_src_url": "https://full/url/to/image.jpg",
  "img_local_name": "18aebe245d17e627c8b6fd958c262dda-image.jpg"
}
"""
while True:
    print('Looking for Work Item...')
    # read messages
    # see http://boto3.readthedocs.org/en/latest/reference/services/sqs.html#SQS.Client.receive_message
    for message in queue.receive_messages(WaitTimeSeconds=10,
                                          VisibilityTimeout=30,
                                          MaxNumberOfMessages=1):
        try:
            message_body = message.body
            work_item = json.loads(message.body)
            print('Found Work Item, Body: {0}'.format(message_body))
            this_directory = os.path.abspath(os.path.dirname(__file__))
            if 'img_src_url' not in message_body or 'img_local_name' not in message_body:
                raise RuntimeError(
                        '"img_src_url" and/or "img_local_name" not found in sqs message body: {}'.format(message_body))
            downloaded_img_local_path = download_image(work_item['img_src_url'], work_item['img_local_name'])
            # put original image to S3
            put_to_s3(s3_bucket_name,
                      "ORIGINAL-{0}".format(work_item['img_local_name']),
                      downloaded_img_local_path)
            # put thumb image to S3
            processed_image_path = process_image(downloaded_img_local_path)
            put_to_s3(s3_bucket_name,
                      "THUMB-{0}".format(work_item['img_local_name']),
                      processed_image_path)
            # See: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/DeleteMessage.html
            message.delete()
            cleanup_local_images(downloaded_img_local_path, processed_image_path)

        except Exception as e:
            import traceback

            print("Exception: {}".format(e))
            traceback.print_exc()
            # since message.delete() did not occur, allow the VisibilityTimeout to expire in the SQS queue
