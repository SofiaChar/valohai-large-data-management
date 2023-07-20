import time

from utils.fetch_data import S3DataDownloader
from utils.preprocess_data import S3DataPreprocessor

second_data_configs = {
    'bucket_name': 'dd-sample-bucket',
    'folder_prefix': 'bigfolder/randomdata/',
    'data_path': '/valohai/repository/dataset',
    'set_production_alias': False,
    '200_samples': True
}


def update_dataset(configs):
    downloader = S3DataDownloader(bucket_name=configs['bucket_name'], prefix=configs['folder_prefix'],
                                  save_path=configs['data_path'])
    downloader.download_files(configs['200_samples'])

    preprocessor = S3DataPreprocessor(save_path=configs['data_path'])
    preprocessor.preprocess_data()

    preprocessor.update_existing_tar(new_tar_name=f'{time.strftime("%Y.%m.%d-%H.%M")}.tar')


if __name__ == '__main__':
    update_dataset(second_data_configs)
