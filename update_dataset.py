import time

from utils.fetch_data import S3DataDownloader
from utils.process_data import S3DataProcessor

second_data_configs = {
    'bucket_name': 'dd-sample-bucket',
    'folder_prefix': 'bigfolder/additional-randomdata/',
    'data_path': '/valohai/repository/dataset',
    'set_alias': 'extended_dataset',
}


def update_dataset(configs):
    downloader = S3DataDownloader(bucket_name=configs['bucket_name'], prefix=configs['folder_prefix'],
                                  save_path=configs['data_path'])
    downloader.download_files()

    preprocessor = S3DataProcessor(save_path=configs['data_path'])
    preprocessor.preprocess_data()

    # Choose either method save_updated_dataset_by_appending_tar
    # or save_updated_dataset_as_two_tars

    preprocessor.save_updated_dataset_as_two_tars(new_tar_name=f'{time.strftime("%Y.%m.%d-%H.%M")}.tar',
                                                  alias=configs['set_alias'])

    # preprocessor.save_updated_dataset_by_appending_tar(new_tar_name=f'{time.strftime("%Y.%m.%d-%H.%M")}.tar',
    #                                                    alias=configs['set_alias'])


if __name__ == '__main__':
    update_dataset(second_data_configs)
