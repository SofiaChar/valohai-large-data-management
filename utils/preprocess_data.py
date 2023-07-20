import os
import json
import tarfile
import shutil

import valohai


class S3DataPreprocessor:
    def __init__(self, save_path):
        self.save_path = save_path
        self.out_path = valohai.outputs()

    def preprocess_data(self):
        # Since we're working with bin files here, we do random change to them
        # You can do any preprocessing in this method

        files = os.listdir(self.save_path)
        print(f'--Preprocessing files...')
        for f in files:
            fp = os.path.join(self.save_path, f)
            with open(fp, 'rb') as file:
                data = bytearray(file.read())

            # Modify the byte at index 10
            data[10] = 0x55

            with open(fp, 'wb') as file:
                file.write(data)

    def tar_directory(self, tarname, set_production_alias=False, tar_mode='w'):
        print(f'--Archiving: {tarname}')

        # Get the path for the output TAR file
        out_path = self.out_path.path(tarname)
        with tarfile.open(out_path, tar_mode) as tarhandle:
            # Add each file in the save path to the TAR file
            for file in os.scandir(self.save_path):
                tarhandle.add(file.path, recursive=False)

        if set_production_alias:
            self._save_metadata(tarname)

    def update_existing_tar(self, new_tar_name):
        print(f'--Updating an archive')

        # Get the path of the input production TAR dataset (tell Valohai not to unzip our archive)
        input_prod_tar_dataset = valohai.inputs('production_dataset').path(process_archives=False)

        # Copy the input TAR dataset to the new path
        new_path = self.out_path.path(new_tar_name)
        shutil.copy(input_prod_tar_dataset, new_path)

        # Append the contents of the save path to the new TAR dataset
        self.tar_directory(new_tar_name, tar_mode='a')

        print(f"--New version of the dataset '{new_tar_name}' saved to outputs.")

    def _save_metadata(self, tarname):
        # Define the metadata for the TAR file
        metadata = {
            "valohai.alias": 'production_dataset'
        }
        metadata_path = self.out_path.path(f'{tarname}.metadata.json')

        # Save the metadata to a JSON file
        with open(metadata_path, 'w') as outfile:
            json.dump(metadata, outfile)
