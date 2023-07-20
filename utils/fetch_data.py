import os
import boto3
from tqdm import tqdm


class S3DataDownloader:
    def __init__(self, bucket_name, prefix, save_path):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.save_path = save_path

    def download_files(self, last_200=False):
        # Fetch the total number of files in the S3 bucket

        objects, s3_client = self._get_all_files()
        total_files = len(objects)

        # Select the desired number of objects to download
        if last_200:
            print(f"--Downloading additional 200 files. Total files found: {total_files}")
            objects_to_download = objects[-200:]
        else:
            print(f"--Downloading 19K files. Total files found: {total_files}")
            objects_to_download = objects[:19000]

        # Create the save path directory
        os.makedirs(self.save_path, exist_ok=True)

        # Iterate over each object to download
        for obj in tqdm(objects_to_download):
            filename = obj['Key']
            file = os.path.basename(filename)
            s3_client.download_file(self.bucket_name, filename, os.path.join(self.save_path, file))

    def _get_all_files(self):
        # Create a session using machine credentials or EC2 instance profile
        session = boto3.Session()

        # Create an S3 client using the session
        s3_client = session.client('s3')

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

        objects = []
        for page in pages:
            objects += sorted(page['Contents'], key=lambda obj: obj['LastModified'])

        return objects, s3_client
