import importlib
import settings
import boto3
import json
import requests
import logging
from flask import Flask, request
from flask_cors import CORS
from collections import OrderedDict

application = Flask(__name__)
app = application
CORS(app)


def main():

    logging.basicConfig(filename="waylon-fuse.log",
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', )
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    app.run(threaded=True, debug=True, port=80, host='0.0.0.0')
    logging.info("Waylon-Fuse server started")


@app.route('/work/<manifest_reference>')
def get_manifest_for_work(manifest_reference):

    p_ = importlib.import_module(settings.PARSER_PATH)
    parser = p_.Parser(space=settings.CURRENT_SPACE)

    s3_client = boto3.client('s3')
    logging.error("S3 client %s" % (s3_client,))
    logging.debug("Request recieved for manifest reference: " + str(manifest_reference))
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    work_reference = manifest_reference.replace('.manifest', '')
    data = load_work_meta(s3_client, work_reference)
    if data is None:
        logging.error("Work data not found: " + str(work_reference))
        return "work not found", 500

    # use named query to get manifest from dlcs
    path = parser.get_manifest_path_from_reference(work_reference)
   
    # get manifest
    req = requests.get(path)
    if req.status_code is not 200:
        logging.error("Error obtaining manifest")
        return "error", 500

    else:
        manifest_string = req.text
        manifest = json.loads(manifest_string, object_pairs_hook=OrderedDict)
        update_manifest_ids(manifest)

        canvas_mapping = generate_canvas_map(manifest)

        # decorate manifest with meta
        decorate_manifest_metadata(data, manifest)

        decorate_manifest_toc(data, manifest, canvas_mapping)

        decorate_manifest_image_metadata(data, manifest)

        parser.custom_decoration(data, manifest)

        # return manifest
        return json.dumps(manifest), 200, {'Content-Type': 'text/css; charset=utf-8'}


def update_manifest_ids(manifest):

    work_id = request.url.replace('.manifest', '')
    manifest['@id'] = work_id
    manifest['sequences'][0]['@id'] = work_id + '/sequences/0'
    canvas_index = 0
    for canvas in manifest['sequences'][0]['canvases']:
        canvas_id = work_id + '/canvas/' + str(canvas_index)
        canvas['@id'] = canvas_id
        for image in canvas['images']:
            image['on'] = canvas_id
        canvas_index += 1


def decorate_manifest_metadata(data, manifest):

    manifest['metadata'] = data['meta']


def decorate_manifest_image_metadata(data, manifest):

    image_metadata = data['image_metadata']
    canvases = manifest['sequences'][0]['canvases']

    canvas_label_field = None
    flags = data.get('flags')
    if flags is not None:
        canvas_label_field = flags.get('Canvas_Label_Field')

    for image_index_string in image_metadata:
        image_index = int(image_index_string)
        canvases[image_index]['metadata'] = image_metadata[image_index_string]
        if canvas_label_field is None:
            canvases[image_index]['label'] = str(image_index + 1)
        else:
            page = ""
            for i in image_metadata[image_index_string]:
                label = i.get('label')
                if label is not None and label == canvas_label_field:
                    val = i.get('value')
                    if val is not None:
                        page = val
            canvases[image_index]['label'] = page


def decorate_manifest_toc(data, manifest, canvas_mapping):

    structures = []
    toc = data.get('toc')
    if toc is None:
        return
    r = 0
    for entry in toc.keys():
        structure = {
                '@type': 'sc:Range',
                '@id': request.base_url + '/range/r-' + str(r),
                'label': entry,
                'canvases': map(lambda e: canvas_mapping[e], toc[entry])}
        r += 1
        structures.append(structure)
    manifest['structures'] = structures


def generate_canvas_map(manifest):

    canvases = manifest['sequences'][0]['canvases']
    mapping = dict(map(lambda (i, x): (i, x['@id']), enumerate(canvases)))
    return mapping


@app.route('/collection/<collection_reference>')
def get_collection(collection_reference):
    # not implemented
    pass


def load_work_meta(s3_client, reference_id):

    try:
        logging.error('ref id : %s in bucket %s' % (reference_id, settings.META_S3))
        obj = s3_client.get_object(Bucket=settings.META_S3, Key='work-' + str(reference_id))
        return json.loads(obj['Body'].read(), object_pairs_hook=OrderedDict)
    except:
	logging.exception("error obtaining metadata")
        return None

if __name__ == "__main__":
    main()
