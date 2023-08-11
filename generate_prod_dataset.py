import time

from utils.fetch_data import S3DataDownloader
from utils.process_data import S3DataProcessor

prod_data_configs = {
    'bucket_name': 'dd-sample-bucket',
    'folder_prefix': 'bigfolder/randomdata/',
    'data_path': '/valohai/repository/dataset',
    'set_alias': 'production_dataset',
}


def generate_prod_dataset(configs):
    downloader = S3DataDownloader(bucket_name=configs['bucket_name'], prefix=configs['folder_prefix'],
                                  save_path=configs['data_path'])
    downloader.download_files()

    preprocessor = S3DataProcessor(save_path=configs['data_path'])
    preprocessor.preprocess_data()

    preprocessor.tar_directory(tarname=f'{time.strftime("%Y.%m.%d-%H.%M")}.tar', alias=configs['set_alias'])


if __name__ == '__main__':
    generate_prod_dataset(prod_data_configs)
