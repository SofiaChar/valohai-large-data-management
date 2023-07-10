import json
import os
import tarfile

import valohai


def tardirectory(path, name, set_alias = False):
    out_path = valohai.outputs().path(name)

    with tarfile.open(out_path, "w") as tarhandle:
        for root, dirs, files in os.walk(path):
            for f in files:
                tarhandle.add(os.path.join(root, f))

    if set_alias:
        save_metadata(name)


def save_metadata(tarname):
    metadata = {
        "valohai.alias": 'production'
    }
    metadata_path = valohai.outputs().path(f'{tarname}.metadata.json')

    with open(metadata_path, 'w') as outfile:
        json.dump(metadata, outfile)
