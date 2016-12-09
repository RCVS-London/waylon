from dlcs.queue_response import Batch
from requests import post, auth
import settings


def register_collection(image_collection):

    authorisation = auth.HTTPBasicAuth(settings.DLCS_API_KEY, settings.DLCS_API_SECRET)
    url = settings.DLCS_ENTRY + 'customers/' + str(settings.DLCS_CUSTOMER_ID) + '/queue'
    json = image_collection.as_json()
    response = post(url, data=json, auth=authorisation)
    batch = Batch(response.json())

    return batch
