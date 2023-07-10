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
        total_files = self._get_total_files()

        if last_200:
            self._download_last_200_files()

        if total_files <= 200:
            print("Total number of files in the bucket is less than or equal to 200. Downloading all files.")
            self._download_files(num_files=total_files)
        else:
            print(f"Downloading all files except the last 200. Total files: {total_files}")
            self._download_files(num_files=total_files - 200)

    def _download_last_200_files(self):
        self._download_files(num_files=200)

    def _download_files(self, num_files):
        # Create a session using machine credentials or EC2 instance profile
        session = boto3.Session()

        # Create an S3 client using the session
        s3_client = session.client('s3')

        # List objects in the specified S3 bucket
        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix)

        # Sort the objects by their last modified date
        objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])

        # Select the desired number of objects to download
        objects_to_download = objects[-num_files:]

        # Create the save path directory
        os.makedirs(self.save_path, exist_ok=True)

        # Iterate over each object to download
        for obj in tqdm(objects_to_download):
            filename = obj['Key']
            file = os.path.basename(filename)
            s3_client.download_file(self.bucket_name, filename, os.path.join(self.save_path, file))

    def _get_total_files(self):
        # Create a session using machine credentials or EC2 instance profile
        session = boto3.Session()

        # Create an S3 client using the session
        s3_client = session.client('s3')

        # List objects in the specified S3 bucket
        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix)

        return len(response['Contents'])
