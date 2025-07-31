from astroid.typing import Callable
from multiprocessing import Pool
from ftplib import FTP
import subprocess
import os.path as path
import pandas as pd
import shutil
import os
import sys

# Class responsible for defining, sharing and creating the directories used in the program
class ProjPaths:
    DOWNLOAD_DIR: str = ""
    BINARIES_DIR: str = ""
    SCRIPTS_DIR: str = ""
    DBFS_DIR: str = ""
    CSVS_DIR: str = ""
    UNITED_CSV_DIR: str = ""
    INIT = False

    @staticmethod
    def init():
        if ProjPaths.INIT:
            return
        ProjPaths.INIT = True
        ProjPaths.define_paths()
        ProjPaths.create_paths()
        ProjPaths.empty_dirs()

    @staticmethod
    def init_guard(func: Callable):
        def wrapped_func():
            if not ProjPaths.INIT:
                raise ValueError("Project paths not initialized")
            return func()
        return wrapped_func


    @staticmethod
    def define_paths():
        ProjPaths.SCRIPTS_DIR = path.split(path.join(os.getcwd(), sys.argv[0]))[0]
        ProjPaths.DOWNLOAD_DIR = path.join(ProjPaths.get_script_dir(), "download")
        ProjPaths.BINARIES_DIR = path.join(ProjPaths.get_script_dir(), "bin")
        ProjPaths.DBFS_DIR = path.join(ProjPaths.get_script_dir(), "dbf")
        ProjPaths.CSVS_DIR = path.join(ProjPaths.get_script_dir(), "csv")
        ProjPaths.UNITED_CSV_DIR = path.join(ProjPaths.get_script_dir(), "united_csv")

    @staticmethod
    def create_paths():
        ProjPaths.create_download_dir()
        ProjPaths.create_binaries_dir()
        ProjPaths.create_dbfs_dir()
        ProjPaths.create_csvs_dir()
        ProjPaths.create_united_csv_dir()

    @staticmethod
    def empty_dirs():
        ProjPaths.empty_download_dir()
        ProjPaths.empty_dbfs_dir()
        ProjPaths.empty_csvs_dir()

    @staticmethod
    def create_binaries_dir():
        if not os.path.exists(ProjPaths.BINARIES_DIR):
            os.makedirs(ProjPaths.BINARIES_DIR)

    @staticmethod
    def create_download_dir():
        if not os.path.exists(ProjPaths.DOWNLOAD_DIR):
            os.makedirs(ProjPaths.DOWNLOAD_DIR)

    @staticmethod
    def create_dbfs_dir():
        if not os.path.exists(ProjPaths.DBFS_DIR):
            os.makedirs(ProjPaths.DBFS_DIR)


    @staticmethod
    def create_csvs_dir():
        if not os.path.exists(ProjPaths.CSVS_DIR):
            os.makedirs(ProjPaths.CSVS_DIR)

    @staticmethod
    def create_united_csv_dir():
        if not os.path.exists(ProjPaths.UNITED_CSV_DIR):
            os.makedirs(ProjPaths.UNITED_CSV_DIR)

    @staticmethod
    @init_guard
    def get_download_dir():
        return ProjPaths.DOWNLOAD_DIR

    @staticmethod
    @init_guard
    def get_binaries_dir():
        return ProjPaths.BINARIES_DIR

    @staticmethod
    @init_guard
    def get_script_dir() -> str:
        return ProjPaths.SCRIPTS_DIR

    @staticmethod
    @init_guard
    def get_csvs_dir() -> str:
        return ProjPaths.CSVS_DIR

    @staticmethod
    @init_guard
    def get_dbfs_dir() -> str:
        return ProjPaths.DBFS_DIR

    @staticmethod
    @init_guard
    def get_united_csv_dir() -> str:
        return ProjPaths.UNITED_CSV_DIR

    @staticmethod
    def empty_download_dir():
        files = os.listdir(ProjPaths.DOWNLOAD_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.DOWNLOAD_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the download dir:\n {str(e)}")

    @staticmethod
    def empty_dbfs_dir():
        files = os.listdir(ProjPaths.DBFS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.DBFS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the dbf dir:\n {str(e)}")

    @staticmethod
    def empty_csvs_dir():
        files = os.listdir(ProjPaths.CSVS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.CSVS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the csv dir:\n {str(e)}")


class Binaries:
    DBF2CSV_PATH: str = ""
    BLAST_DBF_PATH: str = ""
    INIT: bool = False


    @staticmethod
    @ProjPaths.init_guard
    def init():
        Binaries.define_paths()

        if not os.path.exists(Binaries.DBF2CSV_PATH):
            Binaries.create_dbf2csv()

        if not os.path.exists(Binaries.BLAST_DBF_PATH):
            Binaries.create_blast_dbf()

    @staticmethod
    def define_paths():
        Binaries.DBF2CSV_PATH = path.join(ProjPaths.get_binaries_dir(), "DBF2CSV")
        Binaries.BLAST_DBF_PATH = path.join(ProjPaths.get_binaries_dir(), "BLAST_DBF")

    @staticmethod
    def init_guard(func: Callable):
        def wrapped_func():
            if not ProjPaths.INIT:
                raise ValueError("Project paths not initialized")
            return func()
        return wrapped_func

    @staticmethod
    @init_guard
    def get_dbf2csv_path():
        return Binaries.DBF2CSV_PATH

    @staticmethod
    def create_blast_dbf():
        os.chdir(ProjPaths.get_binaries_dir())
        subprocess.run(["git", "clone", "https://github.com/eaglebh/blast-dbf.git"],
            capture_output=True,
            text=True,
            check=True
        )


        os.chdir("blast-dbf")
        subprocess.run(["make"],
            capture_output=True,
            text=True,
            check=True
        )

        shutil.move("blast-dbf", path.join(ProjPaths.get_binaries_dir(), "BLAST_DBF"))

        os.chdir(ProjPaths.get_binaries_dir())

        shutil.rmtree("blast-dbf")

        os.chdir(ProjPaths.get_script_dir())

    @staticmethod
    def create_dbf2csv():
        os.chdir(ProjPaths.get_binaries_dir())
        subprocess.run(["git", "clone", "https://github.com/rmxvrelease/dbc2csv.git"],
            capture_output=True,
            text=True,
            check=True
        )

        source_file = path.join("dbc2csv", "DBF2CSV.c")

        subprocess.run(["gcc", "-o", "DBF2CSV", source_file],
            capture_output=True,
            text=True,
            check=True
        )

        shutil.rmtree("dbc2csv")

        os.chdir(ProjPaths.get_script_dir())


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


class ProjParams:
    CNES: str = "0000000"
    STATE: str = "AC"
    SYSTEM:str = "SIA"
    START: Date = Date(1, 2023)
    END: Date = Date(12, 2023)
    INIT = False

    @staticmethod
    def init():
        ProjParams.CNES = sys.argv[1]
        ProjParams.STATE = sys.argv[2]
        ProjParams.SYSTEM = sys.argv[3]
        ProjParams.START = Date.from_string(sys.argv[4])
        ProjParams.END = Date.from_string(sys.argv[5])
        ProjParams.INIT = True

    @staticmethod
    def init_guard(func: Callable):
        def wrapped_func():
            if not ProjParams.INIT:
                raise ValueError("Project parameters not initialized")
            return func()
        return wrapped_func

    @staticmethod
    @init_guard
    def get_start_date():
        return ProjParams.START

    @staticmethod
    @init_guard
    def get_end_date():
        return ProjParams.END

    @staticmethod
    @init_guard
    def get_cnes():
        return ProjParams.CNES

    @staticmethod
    @init_guard
    def get_state():
        return ProjParams.STATE

    @staticmethod
    @init_guard
    def get_system():
        return ProjParams.SYSTEM

def download_file(file: str):
    dowload_dir_path = ProjPaths.get_download_dir()
    file_name = path.split(file)[-1]
    local_file_path = path.join(dowload_dir_path, file_name)


    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    with open(local_file_path, 'wb') as f:
        ftp.retrbinary(f"RETR {file}", f.write)

    print(f"Downloaded {file_name}")
    ftp.quit()


def download(files: list[str]):
    with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
        p.map(download_file, files)


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


def convert_files():
    files = os.listdir(ProjPaths.DOWNLOAD_DIR)
    path_to_files = list(map(lambda file: path.join(ProjPaths.DOWNLOAD_DIR, file), files))

    with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
        p.map(convert_file_to_csv, path_to_files)


def convert_file_to_csv(file: str):
    PREFIX_SISTEM = {
    'PA': 'SIA',
    'SP': 'SIH'
    }
    cnes = ProjParams.get_cnes()

    filename = path.split(file)[-1]
    dbf_file_name = filename.replace(".dbc", ".dbf")
    csv_file_name = filename.replace(".dbc", ".csv")

    dbf_file_path = path.join(ProjPaths.get_dbfs_dir(), dbf_file_name)
    csv_file_path = path.join(ProjPaths.get_csvs_dir(), csv_file_name)
    sistema = PREFIX_SISTEM[filename[0:2]]

    subprocess.run([Binaries.BLAST_DBF_PATH, file, dbf_file_path])
    subprocess.run([Binaries.DBF2CSV_PATH, dbf_file_path, csv_file_path, cnes, sistema])


def unite_files():
    csv_files = os.listdir(ProjPaths.get_csvs_dir())
    csv_files = list(map(lambda file: path.join(ProjPaths.get_csvs_dir(), file), csv_files))


    data_frames: list[pd.DataFrame] = []
    found_filled_data_frame = False
    for file in csv_files:
        df = pd.read_csv(file)
        if df.empty:
            continue
        data_frames.append(df)
        found_filled_data_frame = True

    if not found_filled_data_frame:
        print("No entries were found the specified period.")
        return

    union = pd.concat(data_frames, ignore_index=True)

    union.to_csv(path.join(ProjPaths.get_united_csv_dir(), f'{ProjParams.SYSTEM}.csv'), index=False)


def main():
    ProjPaths.init()
    Binaries.init()
    ProjParams.init()
    files = find_files(
        ProjParams.get_system(),
        ProjParams.get_state(),
        ProjParams.get_start_date(),
        ProjParams.get_end_date())

    print("will download the follwing files:")
    for file in files:
        print(file)

    download(files)
    print("downloads finished")

    convert_files()
    print("conversions finished")
    unite_files()
    print("files united")

main()
