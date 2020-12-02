import os
from gcloud import storage

try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"
except:
    print("Please check if authentication file is available")

client = storage.Client()
bucket = client.get_bucket('dc-rh-bucketdata')

# place where it will be stored into bucket
blob = bucket.blob('data/2018/10/rh_201810.csv')


# file which has to be uploaded from local system
blob.upload_from_filename('/local/data/rh_201810.csv.csv')


print('File pushed to Cloud Storage Bucket.')
