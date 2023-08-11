import os
import json
import tarfile
import shutil
import valohai


class S3DataProcessor:
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

    def tar_directory(self, tarname, alias, tar_mode='w'):
        print(f'--Archiving: {tarname}')

        # Get the path for the output TAR file
        out_path = self.out_path.path(tarname)
        with tarfile.open(out_path, tar_mode) as tarhandle:
            # Add each file in the save path to the TAR file
            for file in os.scandir(self.save_path):
                tarhandle.add(file.path, recursive=False)

        self._save_metadata(tarname, alias)

    def save_updated_dataset_by_appending_tar(self, new_tar_name, alias):
        print(f'--Updating an archive')

        # Get the path of the input production TAR dataset (tell Valohai not to unzip our archive)
        input_prod_tar_dataset = valohai.inputs('production_dataset').path(process_archives=False)

        # Copy the input TAR dataset to the new path
        new_path = self.out_path.path(new_tar_name)
        shutil.copy(input_prod_tar_dataset, new_path)

        # Append the contents of the save path to the new TAR dataset
        self.tar_directory(new_tar_name, alias, tar_mode='a')

        print(f"--New version of the dataset '{new_tar_name}' saved to datasets.")

    def save_updated_dataset_as_two_tars(self, new_tar_name, alias):
        print(f'--Updating an archive')

        # Get the path of the input production TAR dataset (tell Valohai not to unzip our archive)
        input_prod_tar_dataset = valohai.inputs('production_dataset').path(process_archives=False)
        prod_dataset_name = os.path.basename(input_prod_tar_dataset)

        # Copy the input TAR dataset to the outputs (saving the old name)
        new_path = self.out_path.path(prod_dataset_name)
        shutil.copy(input_prod_tar_dataset, new_path)

        # Tar new data into separate file (it will save metadata in self.tar_directory)
        self.tar_directory(new_tar_name, alias, tar_mode='w')

        print(f"--New version of the dataset '{new_tar_name}' saved to datasets.")

        # Save metadata for input_prod_tar_dataset
        self._save_metadata(prod_dataset_name, alias=alias)

    def _save_metadata(self, tarname, alias):
        version_name = self._get_version_name()

        # Define the metadata for the TAR file
        metadata = {
            "valohai.dataset-versions": [{
                'uri': f"dataset://large-dataset/{version_name}",
                'targeting_aliases': [alias],
            }]
        }
        metadata_path = self.out_path.path(f'{tarname}.metadata.json')

        # Save the metadata to a JSON file
        with open(metadata_path, 'w') as outfile:
            json.dump(metadata, outfile)

    @staticmethod
    def _get_version_name():
        # Get the project name and the execution ID

        f = open('/valohai/config/execution.json')
        exec_details = json.load(f)

        project_name = exec_details['valohai.project-name'].split('/')[1]
        exec_id = exec_details['valohai.execution-id']

        return f'{project_name}_{exec_id}'
