import shutil
import os
from google_drive_downloader import GoogleDriveDownloader as downloader
from pathlib import Path
import re
import logging
import sys

CITIES_DIRECTORY = 'cities'
INPUT_DIRECTORY = 'input'
JAR_FILE_NAME = 'hadoop-homework.jar'
RESULT_FILE = 'result.csv'
log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - %(funcName)s - %(message)s"


"""
    Задаёт настройки логгера и возвращает логгер
"""
def get_logger():
    custom_logger = logging.getLogger(__name__)
    custom_logger.setLevel("INFO")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    custom_logger.addHandler(stream_handler)
    return custom_logger


"""
    Функция очистки локальной рабочей директории
"""
def clear_local_working_directory():
    if Path(INPUT_DIRECTORY).exists():
        shutil.rmtree(INPUT_DIRECTORY)
        logger.info("previous {} directory was deleted locally".format(INPUT_DIRECTORY))
    if Path(CITIES_DIRECTORY).exists():
        shutil.rmtree(CITIES_DIRECTORY)
        logger.info("previous {} directory was deleted locally".format(CITIES_DIRECTORY))
    if RESULT_FILE in os.listdir("."):
        os.remove(RESULT_FILE)
        logger.info("previous {} file was deleted locally".format(RESULT_FILE))
    for part_file in os.listdir("."):
        if re.search("part-r-*", part_file):
            os.remove(part_file)
            logger.info("previous {} file was deleted locally", part_file)
    if JAR_FILE_NAME in os.listdir("."):
        os.remove(JAR_FILE_NAME)
        logger.info("previous {} file wad deleted locally".format(JAR_FILE_NAME))
    if "hw1 4.zip" in os.listdir("."):
        os.remove("hw1 4.zip")
        logger.info("previous hw1 4.zip file was deleted locally")


"""
    Функция очистки рабочей директории в HDFS
"""
def clear_hdfs_working_directory():
    logger.info("HDFS try remove files in directory {}".format(CITIES_DIRECTORY))
    os.system('hdfs dfs -rm /{}/city.en.txt'.format(CITIES_DIRECTORY))
    logger.info("HDFS try remove directory {}".format(CITIES_DIRECTORY))
    os.system('hdfs dfs -rm -r /{}'.format(CITIES_DIRECTORY))
    logger.info("HDFS try remove files in directory {}".format(CITIES_DIRECTORY))
    os.system('hdfs dfs -rm /{}/*'.format(INPUT_DIRECTORY))
    logger.info("HDFS try remove directory {}".format(CITIES_DIRECTORY))
    os.system('hdfs dfs -rm -r /{}'.format(INPUT_DIRECTORY))


"""
    Функция скачивания входных данных
    убирает символ табуляции из первой строки файла city.en.txt
    переименовывает папку hw1 4 в input,
    кладёт файл city.en.txt в папку cities
"""
def download_and_prepare_input_files():
    if Path(INPUT_DIRECTORY).exists():
        shutil.rmtree(INPUT_DIRECTORY)
    if Path(INPUT_DIRECTORY).exists():
        shutil.rmtree(INPUT_DIRECTORY)
    downloader.download_file_from_google_drive(file_id=input_archive_google_id,
                                               dest_path='./hw1 4.zip',
                                               unzip=True)
    shutil.rmtree('./__MACOSX')
    os.remove('./hw1 4.zip')
    old_city_file = open('./hw1 4/city.en.txt')
    first_line = old_city_file.readline()
    updated_first_line = first_line.replace(" ", "\t")
    new_city_file = open("./hw1 4/city.en.txt", "w")
    new_city_file.write(updated_first_line)
    shutil.copyfileobj(old_city_file, new_city_file)
    old_city_file.close()
    new_city_file.close()
    Path('./' + CITIES_DIRECTORY).mkdir(parents=True, exist_ok=True)
    shutil.move('./hw1 4/city.en.txt', './' + CITIES_DIRECTORY)
    os.rename('./hw1 4', INPUT_DIRECTORY)


"""
    Скачивает jar файл из google drive
"""
def download_jar_file():
    downloader.download_file_from_google_drive(file_id=jar_file_google_id,
                                               dest_path='./' + JAR_FILE_NAME)


"""
    Копирует входные файлы в рабочую директорию HDFS
"""
def copy_input_files_to_hdfs():
    logger.info("HDFS put {} files".format(INPUT_DIRECTORY))
    os.system('hdfs dfs -copyFromLocal ./{} /'.format(INPUT_DIRECTORY))
    logger.info("HDFS put {} file".format(CITIES_DIRECTORY))
    os.system('hdfs dfs -copyFromLocal ./{} /'.format(CITIES_DIRECTORY))

"""
    Запускает джобу
"""
def run_job():
    logger.info("run job on hadoop")
    os.system('yarn jar {} /{}/city.en.txt /{} /output'.format(JAR_FILE_NAME, CITIES_DIRECTORY, INPUT_DIRECTORY))


"""
    Скачивает все part-r-* файлы в локальную директорию из HDFS, объединяет результат в RESULT_FILE
"""
def get_and_format_result():
    os.system('hadoop fs -get /output/part-r-*')
    for part_file in os.listdir("."):
        if re.search("part-r-*", part_file):
            os.system('cat part-r-* >> {}'.format(RESULT_FILE))
            return 'success'
    os.system('echo "NO RESULT" >> {}'.format(RESULT_FILE))
    return 'error'


"""
    Главная функция
    argv[1] - id архива с входными данными из google drive,
    argv[2] - id jar-файла hadoop программы из google drive
"""
if __name__ == "__main__":
    logger = get_logger()
    input_archive_google_id = sys.argv[1]
    jar_file_google_id = sys.argv[2]
    clear_local_working_directory()
    clear_hdfs_working_directory()
    download_and_prepare_input_files()
    download_jar_file()
    copy_input_files_to_hdfs()
    run_job()
    logger.info("FINISHED: TASK STATUS: {}".format(get_and_format_result()))
