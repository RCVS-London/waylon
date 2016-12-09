import boto3
import logging
import os
import sys
import json
import importlib
import settings
import uuid
import dlcs
from requests import post, get, auth
from collections import OrderedDict
import dlcs.image_collection


s3_client = None


def main():

    global s3_client

    logging.basicConfig(filename="waylon-ingest.log",
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', )
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    p_ = importlib.import_module(settings.PARSER_PATH)
    parser = p_.Parser(space=settings.CURRENT_SPACE)
    s3_client = boto3.client('s3')

    input_queue = get_input_queue()
    if input_queue is None:
        logging.error("Could not obtain input queue")

    messages_per_fetch = settings.MESSAGES_PER_FETCH

    while True:
        if os.path.exists('/tmp/waylon-stop.txt'):
            sys.exit()
        messages = input_queue.receive_messages(MaxNumberOfMessages=messages_per_fetch, VisibilityTimeout=120,
                                                WaitTimeSeconds=20)

        if len(messages) > 0:
            for message in messages:
                result = process_message(message, parser)
                message.delete()
                if not result:
                    send_error_message(message)


# initialise pool processes with instance of the configured parser

def process_message(message, parser):

    logging.debug("Processing message")
    try:
        # extract the bucket and key of new file from the notification message
        bucket, key = get_file_details_from_message(message.body)

        # download the new file to a temporary location
        filename = download_file(bucket, key)

        # use the configured parser to extract metadata and ImageCollections for DLCS registration
        response = parser.parse(str(key), filename)

        # process parse results
        process_results(response, parser)

        # delete temporary file
        os.remove(filename)

    except Exception as e:
        logging.exception(e)
        return False

    logging.debug("Message processed")
    return True


def process_results(response, parser):

    for collection in response.collections:
        collection_works = collection.works
        for collection_work in collection_works:
            process_work(collection_work, parser)
    for work in response.works:
        process_work(work, parser)


def process_work(work, parser):

    store_work_metadata(work)
    remove_existing_images(work, parser)
    register_work_imagecollection(work)


def remove_existing_images(work, parser):

    manifest_url = parser.get_images_for_work_path(work.id)
    response = get(manifest_url)

    if not response.status_code == 200:
        logging.error("Could not get manifest to remove existing images")
        return

    result_string = response.text
    result = json.loads(result_string, object_pairs_hook=OrderedDict)
    images = []
    for image_id in result:
        images.append(dlcs.image_collection.Image(id=str(image_id)))
    image_collection = dlcs.image_collection.ImageCollection(images)
    collection_json = json.dumps(image_collection.to_json_dict())
    authorisation = auth.HTTPBasicAuth(settings.DLCS_API_KEY, settings.DLCS_API_SECRET)
    delete_response = post(settings.DLCS_DELETE_PATH, data=collection_json, auth=authorisation)
    if not delete_response.status_code == 200:
        logging.error("Error requesting existing image deletion")
        return


def store_work_metadata(work):

    data = {
        'meta': work.work_metadata,

    }
    if len(work.toc) > 0:
        data['toc'] = work.toc
    if len(work.flags) > 0:
        data['flags'] = work.flags
    if len(work.image_metadata) > 0:
        data['image_metadata'] = work.image_metadata
    json_data = json.dumps(data)
    s3_client.put_object(Bucket=settings.META_S3, Key='work-' + work.id, Body=json_data)


def register_work_imagecollection(work):

    dlcs.client.register_collection(work.image_collection)


def get_file_details_from_message(message_body):

    bucket_name, key = None, None
    message = json.loads(message_body)
    records = message.get('Records')
    if records is not None and len(records) > 0:
        s3 = records[0].get('s3')
        if s3 is not None:
            bucket = s3.get('bucket')
            if bucket is not None:
                bucket_name = bucket.get('name')
            s3_object = s3.get('object')
            if s3_object is not None:
                key = s3_object.get('key')
    return bucket_name, key


def download_file(bucket, key):

    tmp_path = settings.TMP_PATH
    if not tmp_path.endswith('/'):
        tmp_path += '/'
    filename = tmp_path + 'waylon_' + str(uuid.uuid4())
    s3_client.download_file(bucket, key, filename)
    return filename


def send_error_message(message):

    print str(message)


def get_input_queue():

    sqs_client = boto3.resource('sqs', settings.SQS_REGION)
    queue = sqs_client.get_queue_by_name(QueueName=settings.INPUT_QUEUE)
    return queue


if __name__ == "__main__":
    main()
