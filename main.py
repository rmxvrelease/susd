from astroid.typing import Callable
from ftplib import FTP
import os.path as path
import os
import sys

# Class responsible for defining, sharing and creating the paths used in the program
class ProjPaths:
    DOWNLOAD_DIR: str = ""
    BINARIES_DIR: str = ""
    INIT = False

    @staticmethod
    def init():
        if ProjPaths.INIT:
            return

        ProjPaths.define_paths()
        ProjPaths.create_paths()

        ProjPaths.INIT = True

    @staticmethod
    def init_guard(func: Callable):
        def wrapped_func():
            if not ProjPaths.INIT:
                raise ValueError("Project paths not initialized")
            return func()
        return wrapped_func

    @staticmethod
    def define_paths():
        ProjPaths.DOWNLOAD_DIR = path.join(ProjPaths.get_script_dir(), "download")
        ProjPaths.BINARIES_DIR = path.join(ProjPaths.get_script_dir(), "bin")

    @staticmethod
    def create_paths():
        ProjPaths.create_download_dir()
        ProjPaths.create_binaries_dir()

    @staticmethod
    def create_binaries_dir():
        if not os.path.exists(ProjPaths.BINARIES_DIR):
            os.makedirs(ProjPaths.BINARIES_DIR)

    @staticmethod
    def create_download_dir():
        if not os.path.exists(ProjPaths.DOWNLOAD_DIR):
            os.makedirs(ProjPaths.DOWNLOAD_DIR)

    @staticmethod
    @init_guard
    def get_download_dir():
        return ProjPaths.DOWNLOAD_DIR

    @staticmethod
    @init_guard
    def get_binaries_dir():
        return ProjPaths.BINARIES_DIR

    @staticmethod
    def get_script_dir() -> str:
        return path.split(path.join(os.getcwd(), sys.argv[0]))[0]


class ProjConfigs:
    N_OF_THREADS = 4


class Date:
    def __init__(self, month: int, year: int):
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12")
        if year < 50:
            year += 2000
        elif year < 99:
            year += 1900

        self.year = year
        self.month = month

    @staticmethod
    def from_string(date_str: str):
            month, year = date_str.split('-')
            return Date(int(month), int(year))

    def __lt__(self, other):
        if self.year == other.year:
            return self.month < other.month
        return self.year < other.year

    def __eq__(self, other):
        return self.year == other.year and self.month == other.month

    def __gt__(self, other):
        if self.year == other.year:
            return self.month > other.month
        return self.year > other.year

    def __str__(self):
        return f"{self.month:02d}-{self.year:04d}"


def download(files: list[str]):
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    for file in files:
        file_name = path.split(file)[-1]
        local_file_path = path.join(ProjPaths.get_download_dir(), file_name)

        def callback(data:bytes):
            with open(local_file_path, 'wb') as f:
                f.write(data)

        ftp.retrbinary(f"RETR {file}", callback)

    ftp.quit()


def find_files(sistema: str, estado: str, inicio: Date, fim: Date):
    SEARCH_RREFIXES = {
    'SIA': 'PA',
    'SIH': 'SP'
    }

    SEARCH_DIRS = {
        'SIA': ["/dissemin/publicos/SIASUS/199407_200712/Dados",
                "/dissemin/publicos/SIASUS/200801_/Dados"],
        'SIH': ["/dissemin/publicos/SIHSUS/199201_200712/Dados",
                "/dissemin/publicos/SIHSUS/200801_/Dados"]
    }

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    search_target = SEARCH_DIRS[sistema]
    search_prefix = SEARCH_RREFIXES[sistema]

    files: list[str] = []


    for dir in search_target:
        def append_to_file(file: str):
            file = file.split(' ')[-1]

            if file[0:2] != search_prefix:
                return

            if file[2:4] != estado:
                return

            try:
                date = Date.from_string(file[6:8] + "-" + file[4:6])
            except:
                return

            if date < inicio or fim < date:
                return

            files.append(path.join(dir, file))

        ftp.cwd(dir)
        ftp.retrlines("LIST", append_to_file)

    ftp.quit()

    return files


def main():
    ProjPaths.init()

    files = find_files("SIA", "SP", Date.from_string("01-2023"), Date.from_string("02-2023"))
    download(files)


main()
