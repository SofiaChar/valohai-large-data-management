import os
import tarfile

import valohai


def untar(target_dir):
    # Extract to target directory

    tar_paths = valohai.inputs('dataset').paths(
        process_archives=False)  # when process_archives = True, all archives are extracted automatically to tmp dir

    for tar_path in tar_paths:
        tar = tarfile.open(tar_path)
        tar.extractall(path=target_dir)
        out = os.path.abspath(target_dir)

    print(f'\n --TARs are extracted to {out}')


if __name__ == '__main__':
    save_to = 'current_dataset'
    untar(save_to)
