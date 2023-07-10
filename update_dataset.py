import time

from utils.fetch_data import S3DataDownloader
from utils.preprocess_data import S3DataPreprocessor

second_data_configs = {
    'bucket_name': 'dd-sample-bucket',
    'folder_prefix': 'bigfolder/randomdata/',
    'data_path': '/valohai/repository/bucket_contents',
    'set_prod_alias': False,
    '200_samples': True
}


def update_dataset(configs):
    downloader = S3DataDownloader(bucket_name=configs['bucket_name'], prefix=configs['folder_prefix'],
                                  save_path=configs['data_path'])
    downloader.download_files(configs['200_samples'])

    preprocessor = S3DataPreprocessor(save_path=configs['data_path'])
    preprocessor.preprocess_data()

    # get prod dataset & join with new data


    # tar the results
    preprocessor.tar_directory(f'{time.strftime("%Y.%m.%d-%H:%M")}.tar', set_alias=configs['set_prod_alias'])


if __name__ == '__main__':
    update_dataset(second_data_configs)
