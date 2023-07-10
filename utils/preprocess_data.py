import os
import json
import tarfile

import valohai


class S3DataPreprocessor:
    def __init__(self, save_path):
        self.save_path = save_path
        self.out_path = valohai.outputs()

    def preprocess_data(self):
        files = os.listdir(self.save_path)
        print(f'Preprocess stated on {files}')
        for f in files:
            fp = os.path.join(self.save_path, f)
            with open(fp, 'rb') as file:
                data = bytearray(file.read())

            # Modify the byte at index 10
            data[10] = 0x55

            with open(fp, 'wb') as file:
                file.write(data)


    def update_existing_tar_from_inputs(self):
        path = valohai.inputs("production_dataset").path()

        with tarfile.open(path, "a") as tarhandle:
            for root, dirs, files in os.walk(self.save_path):
                for f in files:
                    tarhandle.add(os.path.join(root, f))



    def tar_directory(self, name, set_alias=False):
        out_path = self.out_path.path(name)
        print('start tar')
        with tarfile.open(out_path, "w") as tarhandle:
            for root, dirs, files in os.walk(self.save_path):
                for f in files:
                    tarhandle.add(os.path.join(root, f))

        if set_alias:
            self._save_metadata(name)

    def _save_metadata(self, tarname):
        metadata = {
            "valohai.alias": 'production'
        }
        metadata_path = self.out_path.path(f'{tarname}.metadata.json')

        with open(metadata_path, 'w') as outfile:
            json.dump(metadata, outfile)
