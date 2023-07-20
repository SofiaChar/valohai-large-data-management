import os
import tarfile

import valohai


def untar(target_dir=None):
    # Extract to target directory
    if target_dir:
        tar_path = valohai.inputs('dataset').path(process_archives=False)
        tar = tarfile.open(tar_path)
        tar.extractall(path=target_dir)
        out = os.path.abspath(target_dir)

    # Automatically extract to tmp directory
    else:
        files = valohai.inputs('dataset').path()
        out = os.path.dirname(files)

    print(f'--TAR is extracted to {out}')


if __name__ == '__main__':
    save_to = 'current_dataset'
    untar(save_to)
