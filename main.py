from multiprocessing import Pool
from ftplib import FTP
import subprocess
import os.path as path
from numpy._core.multiarray import ITEM_HASOBJECT
import pandas as pd
import shutil
from tables.interest_rate_before_01_2022 import INTEREST_BEFORE_01_2022
import numpy as np
import datetime
import os
import sys

from template_docs import ivr_file_template
from template_docs.ivr_template import ivr_tex_str

SIA_RELEVANT_FIELDS = np.array(['PA_CMP', 'PA_PROC_ID', 'PA_QTDAPR', 'PA_VALAPR'])
SIH_RELEVANT_FIELDS = np.array(['SP_AA', 'SP_MM','SP_ATOPROF', 'SP_QTD_ATO', 'SP_VALATO'])

# Class responsible for defining, sharing and creating the directories used in the program
class ProjPaths:
    DOWNLOAD_DIR: str = ""
    BINARIES_DIR: str = ""
    SCRIPTS_DIR: str = ""
    SIA_DBFS_DIR: str = ""
    SIH_DBFS_DIR: str = ""
    SIA_CSVS_DIR: str = ""
    SIH_CSVS_DIR: str = ""
    UNITED_CSV_DIR: str = ""
    TABLES_DIR: str = ""
    SELIC_TABLE_PATH: str = ""
    DBF2CSV_PATH: str = ""
    BLAST_DBF_PATH: str = ""
    RESULTS_DIR: str = ""
    TOTAL_REPORT_PATH: str = ""
    MONTH_REPORT_PATH: str = ""
    YEAR_REPORT_PATH: str = ""
    LATEX_DIR: str = ""
    LATEX_FILE_PATH: str = ""

    @staticmethod
    def init():
        ProjPaths.define_paths()
        ProjPaths.create_paths()
        ProjPaths.empty_dirs()

    @staticmethod
    def define_paths():
        ProjPaths.SCRIPTS_DIR = path.split(path.join(os.getcwd(), sys.argv[0]))[0]
        ProjPaths.DOWNLOAD_DIR = path.join(ProjPaths.SCRIPTS_DIR, "download")
        ProjPaths.BINARIES_DIR = path.join(ProjPaths.SCRIPTS_DIR, "bin")
        ProjPaths.SIA_DBFS_DIR = path.join(ProjPaths.SCRIPTS_DIR, "sia_dbf")
        ProjPaths.SIH_DBFS_DIR = path.join(ProjPaths.SCRIPTS_DIR, "sih_dbf")
        ProjPaths.SIA_CSVS_DIR = path.join(ProjPaths.SCRIPTS_DIR, "sia_csv")
        ProjPaths.SIH_CSVS_DIR = path.join(ProjPaths.SCRIPTS_DIR, "sih_csv")
        ProjPaths.RESULTS_DIR = path.join(ProjPaths.SCRIPTS_DIR, "results")
        ProjPaths.UNITED_CSV_DIR = path.join(ProjPaths.SCRIPTS_DIR, "united_csv")
        ProjPaths.TABLES_DIR = path.join(ProjPaths.SCRIPTS_DIR, "tables")
        ProjPaths.SELIC_TABLE_PATH = path.join(ProjPaths.TABLES_DIR, "selic.csv")
        ProjPaths.DBF2CSV_PATH = path.join(ProjPaths.BINARIES_DIR, "DBF2CSV")
        ProjPaths.BLAST_DBF_PATH = path.join(ProjPaths.BINARIES_DIR, "BLAST_DBF")
        ProjPaths.MONTH_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "month.csv")
        ProjPaths.YEAR_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "year.csv")
        ProjPaths.TOTAL_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "total.csv")
        ProjPaths.LATEX_DIR = path.join(ProjPaths.SCRIPTS_DIR, "latex")
        ProjPaths.LATEX_FILE_PATH = path.join(ProjPaths.LATEX_DIR, "laudo.tex")

    @staticmethod
    def create_paths():
        ProjPaths.create_download_dir()
        ProjPaths.create_binaries_dir()
        ProjPaths.create_dbfs_dir()
        ProjPaths.create_csvs_dir()
        ProjPaths.create_united_csv_dir()
        ProjPaths.create_tables_dir()
        ProjPaths.create_results_dir()
        ProjPaths.create_dbf2csv()
        ProjPaths.create_blast_dbf()
        ProjPaths.create_latex_dir()

    @staticmethod
    def empty_dirs():
        ProjPaths.empty_download_dir()
        ProjPaths.empty_dbfs_dir()
        ProjPaths.empty_csvs_dir()
        ProjPaths.empty_results_dir()
        ProjPaths.empty_latex_dir()

    @staticmethod
    def create_tables_dir():
        if not os.path.exists(ProjPaths.TABLES_DIR):
            os.makedirs(ProjPaths.TABLES_DIR)

    @staticmethod
    def create_results_dir():
        if not os.path.exists(ProjPaths.RESULTS_DIR):
            os.makedirs(ProjPaths.RESULTS_DIR)

    @staticmethod
    def create_binaries_dir():
        if not os.path.exists(ProjPaths.BINARIES_DIR):
            os.makedirs(ProjPaths.BINARIES_DIR)

    @staticmethod
    def create_latex_dir():
        if not os.path.exists(ProjPaths.LATEX_DIR):
            os.makedirs(ProjPaths.LATEX_DIR)

    @staticmethod
    def create_download_dir():
        if not os.path.exists(ProjPaths.DOWNLOAD_DIR):
            os.makedirs(ProjPaths.DOWNLOAD_DIR)

    @staticmethod
    def create_dbfs_dir():
        if not os.path.exists(ProjPaths.SIA_DBFS_DIR):
            os.makedirs(ProjPaths.SIA_DBFS_DIR)

        if not os.path.exists(ProjPaths.SIH_DBFS_DIR):
            os.makedirs(ProjPaths.SIH_DBFS_DIR)


    @staticmethod
    def create_csvs_dir():
        if not os.path.exists(ProjPaths.SIA_CSVS_DIR):
            os.makedirs(ProjPaths.SIA_CSVS_DIR)

        if not os.path.exists(ProjPaths.SIH_CSVS_DIR):
            os.makedirs(ProjPaths.SIH_CSVS_DIR)

    @staticmethod
    def create_united_csv_dir():
        if not os.path.exists(ProjPaths.UNITED_CSV_DIR):
            os.makedirs(ProjPaths.UNITED_CSV_DIR)


    @staticmethod
    def create_blast_dbf():
        if not os.path.exists(ProjPaths.BLAST_DBF_PATH):
            os.chdir(ProjPaths.BINARIES_DIR)
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


            shutil.move("blast-dbf", path.join(ProjPaths.BINARIES_DIR, "BLAST_DBF"))

            os.chdir(ProjPaths.BINARIES_DIR)

            shutil.rmtree("blast-dbf")

            os.chdir(ProjPaths.SCRIPTS_DIR)


    @staticmethod
    def create_dbf2csv():
        if not os.path.exists(ProjPaths.BLAST_DBF_PATH):
            os.chdir(ProjPaths.BINARIES_DIR)
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

            os.chdir(ProjPaths.SCRIPTS_DIR)


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
    def empty_latex_dir():
        files = os.listdir(ProjPaths.LATEX_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.LATEX_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the latex dir:\n {str(e)}")


    @staticmethod
    def empty_dbfs_dir():
        files = os.listdir(ProjPaths.SIA_DBFS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIA_DBFS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the dbf dir:\n {str(e)}")

        files = os.listdir(ProjPaths.SIH_DBFS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIH_DBFS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the dbf dir:\n {str(e)}")

    @staticmethod
    def empty_csvs_dir():
        files = os.listdir(ProjPaths.SIA_CSVS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIA_CSVS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the csv dir:\n {str(e)}")

        files = os.listdir(ProjPaths.SIH_CSVS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIH_CSVS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the csv dir:\n {str(e)}")

    @staticmethod
    def empty_results_dir():
        files = os.listdir(ProjPaths.RESULTS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.RESULTS_DIR, file))
            except Exception as e:
                print(f"could not delete {file} from the results dir:\n {str(e)}")


class ProjConfigs:
    N_OF_THREADS = 1


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

    @staticmethod
    def first_day_of_previous_month() -> str:
        today = datetime.date.today()
        if (today.month == 1):
            return f'01/12/{today.year-1}'
        else:
            return f'01/{today.month-1}/{today.year}'

    @staticmethod
    def from_sus_file_name(file_name: str):
        f_name_no_path = path.split(file_name)[-1]
        year = int(f_name_no_path[4:6])
        month = int(f_name_no_path[6:8])
        return Date(month, year)


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
    END_INTEREST: Date = Date(4, 2025)

    @staticmethod
    def init():
        ProjParams.CNES = sys.argv[1]
        ProjParams.STATE = sys.argv[2]
        ProjParams.SYSTEM = sys.argv[3]
        ProjParams.START = Date.from_string(sys.argv[4])
        ProjParams.END = Date.from_string(sys.argv[5])
        ProjParams.END_INTEREST = Date.from_string(sys.argv[6])

    @staticmethod
    def get_start_date():
        return ProjParams.START

    @staticmethod
    def get_end_date():
        return ProjParams.END

    @staticmethod
    def get_cnes():
        return ProjParams.CNES

    @staticmethod
    def get_state():
        return ProjParams.STATE

    @staticmethod
    def get_system():
        return ProjParams.SYSTEM




class InterestRate:
    SELIC: np.ndarray

    @staticmethod
    def load_selic():
        end_time_str = Date.first_day_of_previous_month()
        print(end_time_str)
        try: selic = pd.read_csv(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=csv&dataInicial=01/12/2021&dataFinal={end_time_str}", sep=";")
        except: selic = pd.read_csv(ProjPaths.SELIC_TABLE_PATH)

        selic['valor'] = (selic['valor'].astype(str).str.replace(",", ".").astype(float) / 100)
        InterestRate.SELIC = selic['valor'].__array__()
        selic.to_csv(ProjPaths.SELIC_TABLE_PATH, index=False)


    @staticmethod
    def cumulative_selic(s: Date, e: Date) -> float:
        '''WARNING: JUROS SIMPLES'''
        if (s < Date.from_string('01-2022')):
            s = Date.from_string('01-2022')

        s_months_since_12_2021 = (s.year - 2021)*12 + s.month - 12
        e_months_since_12_2021 = (e.year - 2021)*12 + e.month - 12

        cumulative_rate = InterestRate.SELIC[s_months_since_12_2021-1:e_months_since_12_2021].sum()

        return cumulative_rate


    @staticmethod
    def rate_until_01_2022(s: Date) -> float:
        '''Calcula o juros da data "s" até a 01-2022'''
        s_month_until_01_2021 = (s.year - 2022)*12 + s.month - 1

        if (s_month_until_01_2021 >= 0):
            return(1.0)

        return INTEREST_BEFORE_01_2022[s_month_until_01_2021]

    @staticmethod
    def complete_rate(s: Date, e: Date) -> float:
        rate_before_01_2022 = InterestRate.rate_until_01_2022(s)
        rate_after_01_2022 = InterestRate.cumulative_selic(s, e)
        return rate_before_01_2022 * (1.0 + rate_after_01_2022)



class Downloads:
    # download a file from the ftp data sus given it's path inside the server
    @staticmethod
    def download_file(file: str):
        dowload_dir_path = ProjPaths.DOWNLOAD_DIR
        file_name = path.split(file)[-1]
        local_file_path = path.join(dowload_dir_path, file_name)


        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()

        with open(local_file_path, 'wb') as f:
            ftp.retrbinary(f"RETR {file}", f.write)

        print(f"Downloaded {file_name}")
        ftp.quit()

    @staticmethod
    def download(files: list[str]):
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            p.map(Downloads.download_file, files)

    @staticmethod
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


class Conversions:
    @staticmethod
    def convert_files():
        files = os.listdir(ProjPaths.DOWNLOAD_DIR)
        path_to_files = [path.join(ProjPaths.DOWNLOAD_DIR, file) for file in files]

        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            p.map(Conversions.convert_file_to_csv, path_to_files)

    @staticmethod
    def convert_file_to_csv(file: str):
        PREFIX_SYSTEM = {
        'PA': 'SIA',
        'SP': 'SIH'
        }
        PREFIX_CSV_DIR = {
        'PA': ProjPaths.SIA_CSVS_DIR,
        'SP': ProjPaths.SIH_CSVS_DIR
        }
        PREFIX_DBF_DIR = {
        'PA': ProjPaths.SIA_DBFS_DIR,
        'SP': ProjPaths.SIH_DBFS_DIR,
        }

        cnes = ProjParams.get_cnes()

        filename = path.split(file)[-1]
        dbf_file_name = filename.replace(".dbc", ".dbf")
        csv_file_name = filename.replace(".dbc", ".csv")

        csv_dir = PREFIX_CSV_DIR[filename[0:2]]
        dbf_dir = PREFIX_DBF_DIR[filename[0:2]]


        dbf_file_path = path.join(dbf_dir, dbf_file_name)
        csv_file_path = path.join(csv_dir, csv_file_name)
        sistema = PREFIX_SYSTEM[filename[0:2]]

        subprocess.run([ProjPaths.BLAST_DBF_PATH, file, dbf_file_path])
        subprocess.run([ProjPaths.DBF2CSV_PATH, dbf_file_path, csv_file_path, cnes, sistema])

    @staticmethod
    def unite_files(system: str):
        PREFIX_CSV_DIR = {
        'SIA': ProjPaths.SIA_CSVS_DIR,
        'SIH': ProjPaths.SIH_CSVS_DIR
        }

        csv_dir = PREFIX_CSV_DIR[system]

        csv_files = os.listdir(csv_dir)
        csv_files = [path.join(csv_dir, file) for file in csv_files]


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

        union.to_csv(path.join(ProjPaths.UNITED_CSV_DIR, f'{ProjParams.SYSTEM}.csv'), index=False)


class IvrMonthInfo:
    def __init__(self, sia_brt_sum: float, sih_brt_sum: float, i_rate: float, source: str, when: Date):
        '''Parameters: brute sum, interest rate'''
        self.i_rate = i_rate
        self.src = source
        self.when = when

        self.brt_sum = sia_brt_sum + sih_brt_sum
        self.adj_sum = self.brt_sum * i_rate
        self.brt_ivr = self.brt_sum * 1.5
        self.adj_ivr = self.brt_ivr * i_rate

        self.sia_brt_sum = sia_brt_sum
        self.sia_adj_sum = self.sia_brt_sum * i_rate
        self.sia_brt_ivr = sia_brt_sum * 1.5
        self.sia_adj_ivr = self.sia_brt_ivr * i_rate

        self.sih_brt_sum = sih_brt_sum
        self.sih_adj_sum = self.sih_brt_sum * i_rate
        self.sih_brt_ivr = sih_brt_sum * 1.5
        self.sih_adj_ivr = self.sih_brt_ivr * i_rate

    @classmethod
    def from_sia(cls, sia_brt_sum: float, i_rate: float, when: Date):
        return cls(sia_brt_sum, 0.0, i_rate, 'SIA', when)

    @classmethod
    def from_sih(cls, sih_brt_sum: float, i_rate: float, when: Date):
        return cls(0.0, sih_brt_sum, i_rate, 'SIH', when)

    @classmethod
    def empty(cls, i_rate: float, when: Date):
        return cls(0.0, 0.0, i_rate, 'BOTH', when)

    def add_sia(self, sia_brt_sum):
        self.sia_brt_sum = sia_brt_sum
        self.sia_brt_ivr = sia_brt_sum * 1.5
        self.sia_adj_sum = self.sia_brt_sum * self.i_rate
        self.sia_adj_ivr = self.sia_brt_ivr * self.i_rate

        self.brt_sum += sia_brt_sum
        self.brt_ivr = self.brt_sum * 1.5
        self.adj_ivr = self.brt_ivr * self.i_rate
        self.adj_sum = self.brt_sum * self.i_rate

    def add_sih(self, sih_brt_sum):
        self.sih_brt_sum = sih_brt_sum
        self.sih_brt_ivr = sih_brt_sum * 1.5
        self.sih_adj_sum = self.sih_brt_sum * self.i_rate
        self.sih_adj_ivr = self.sih_brt_ivr * self.i_rate

        self.brt_sum += sih_brt_sum
        self.brt_ivr = self.brt_sum * 1.5
        self.adj_ivr = self.brt_ivr * self.i_rate
        self.adj_sum = self.brt_sum * self.i_rate


class IvrYearInfo:
    def __init__(self, year: int):
        self.when = year
        self.brute_ivr_diff = 0.0
        self.val_correcao = 0.0
        self.adj_ivr_diff = 0.0

    def add_month(self, m: IvrMonthInfo):
        self.brute_ivr_diff += m.brt_ivr/3
        self.val_correcao += m.adj_ivr/3 - m.brt_ivr/3
        self.adj_ivr_diff += m.adj_ivr/3


class IvrTotalInfo:
    def __init__(self) -> None:
        self.brute_ivr_diff = 0.0
        self.val_correcao = 0.0
        self.adj_ivr_diff = 0.0

    def add_month(self, m: IvrMonthInfo):
        self.brute_ivr_diff += m.brt_ivr/3
        self.val_correcao += m.adj_ivr/3 - m.brt_ivr/3
        self.adj_ivr_diff += m.adj_ivr/3


class ProcessingIVR:
    @staticmethod
    def adjusted_month_SIA(file_path:str) -> IvrMonthInfo:
        df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS)
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate(when, Date(4, 2025))
        brute_sum = df["PA_VALAPR"].sum()

        return IvrMonthInfo.from_sia(brute_sum, rate, when)


    @staticmethod
    def adjusted_month_SIH(file_path: str) -> IvrMonthInfo:
        df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS)
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate(when, Date(4, 2025))
        brute_sum = df["SP_VALATO"].sum()

        return IvrMonthInfo.from_sih(brute_sum, rate, when)


    @staticmethod
    def brute_sum_SIA(file_path: str) -> tuple[float, Date]:
        df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS)
        when = Date.from_sus_file_name(file_path)
        sum = df["PA_VALAPR"].sum()
        return sum, when


    @staticmethod
    def brute_sum_SIH(file_path: str) -> tuple[float, Date]:
        df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS)
        when = Date.from_sus_file_name(file_path)
        sum = df["SP_VALATO"].sum()
        return sum, when


    @staticmethod
    def adjusted_SIH(files: list[str]) -> list[IvrMonthInfo]:
        res: list[IvrMonthInfo] = []
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            res = p.map(ProcessingIVR.adjusted_month_SIH, files)
        return res


    @staticmethod
    def adjusted_SIA(files: list[str]):
        res: list[IvrMonthInfo] = []
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            res = p.map(ProcessingIVR.adjusted_month_SIA, files)
        return res

    @staticmethod
    def brute_SIA(files: list[str]):
        res: list[tuple[float, Date]] = []
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            res = p.map(ProcessingIVR.brute_sum_SIA, files)
        return res

    @staticmethod
    def brute_SIH(files: list[str]):
        res: list[tuple[float, Date]] = []
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            res = p.map(ProcessingIVR.brute_sum_SIH, files)
        return res

    @staticmethod
    def adjusted_BOTH(sia_files: list[str], sih_files: list[str]) -> list[IvrMonthInfo]:
        brute_months_sia = ProcessingIVR.brute_SIA(sia_files)
        brute_months_sih = ProcessingIVR.brute_SIH(sih_files)

        months_info: dict[str,IvrMonthInfo] = {}

        for sia_f in brute_months_sia:
            str_date = str(sia_f[1])
            if not str_date in months_info:
                i_rate = InterestRate.complete_rate(sia_f[1], ProjParams.END_INTEREST)
                months_info[str_date] = IvrMonthInfo.empty(i_rate, sia_f[1])

            months_info[str_date].add_sia(sia_f[0])


        for sih_f in brute_months_sih:
            str_date = str(sih_f[1])
            if not str_date in months_info:
                i_rate = InterestRate.complete_rate(sih_f[1], ProjParams.END_INTEREST)
                months_info[str_date] = IvrMonthInfo.empty(i_rate, sih_f[1])

            months_info[str_date].add_sih(sih_f[0])

        lst = [i[1] for i in list(months_info.items())]
        lst.sort(key=lambda x: x.when)
        return lst


    @staticmethod
    def year_results(months_res: list[IvrMonthInfo]) -> list[IvrYearInfo]:
        years_table: dict[int, IvrYearInfo] = {}

        for month in months_res:
            year = month.when.year
            if not year in years_table:
                years_table[year] = IvrYearInfo(year)

            years_table[year].add_month(month)

        lst = [i[1] for i in list(years_table.items())]
        lst.sort(key=lambda x: x.when)
        return lst
    
    @staticmethod
    def total_result(months_res: list[IvrMonthInfo]) -> IvrTotalInfo:
        result = IvrTotalInfo()
        for month in months_res:
            result.add_month(month)
        return result


class IvrCsvBuilder:
    @staticmethod
    def build_month_report(months: list[IvrMonthInfo]):
        df = pd.DataFrame()

        for month in months:
            new_row = pd.DataFrame({
                'MES': [str(month.when)],
                'TOTAL_DEVIDO': [month.adj_ivr/3],
                'CORRECAO': [month.i_rate],
                'PAGO_BRUTO_TOT': [month.brt_sum],
                'Diferença IVR': [month.adj_ivr]})

            df = pd.concat([df, new_row], ignore_index=True)

        df.to_csv(ProjPaths.MONTH_REPORT_PATH, index=False, sep=';', float_format="%.2f")

    @staticmethod
    def build_year_report(years: list[IvrYearInfo]):
        df = pd.DataFrame()

        for year in years:
            new_row = pd.DataFrame({'ANO': [str(year.when)],
                                    'DIF_IVR_BRUTO': [str(year.brute_ivr_diff)],
                                    'VAL_CORRECAO': [str(year.val_correcao)],
                                    'DIF_IVR_CORRIGIDO': [str(year.adj_ivr_diff)]})
            df = pd.concat([df, new_row], ignore_index=True)

        df.to_csv(ProjPaths.YEAR_REPORT_PATH, index=False, sep=';', float_format="%.2f")

    @staticmethod
    def build_total_report(report: IvrTotalInfo):
            df = pd.DataFrame({'DIF_IVR_BRUTO': [str(report.brute_ivr_diff)],
                                    'VAL_CORRECAO': [str(report.val_correcao)],
                                    'DIF_IVR_CORRIGIDO': [str(report.adj_ivr_diff)]})

            df.to_csv(ProjPaths.TOTAL_REPORT_PATH, index=False, sep=';', float_format="%.2f")

class LatexBuilder:
    import template_docs.ivr_file_template

    @staticmethod
    def build_IVR_latex_file(months: list[IvrMonthInfo], years: list[IvrYearInfo], report: IvrTotalInfo):
        result = ivr_file_template.FILE_HEADER

        result += ivr_file_template.CONCLUSAO

        result += LatexBuilder.build_total_latex_table(report)

        result += LatexBuilder.build_year_latex_table(years)
        
        result += LatexBuilder.build_month_latex_table(months)

        result += ivr_file_template.FILE_FOOTER

        f = open(ProjPaths.LATEX_FILE_PATH, 'w')
        f.write(result)
        f.close()


    @staticmethod
    def build_month_latex_table(months: list[IvrMonthInfo]) -> str:
        table_body = ivr_file_template.MONTH_HEADER
        for m in months:
            table_body += f"{m.when} & {m.brt_sum:.2f} & {m.brt_ivr/3:.2f} & {(m.i_rate*100)-100:.4f}\\% & {m.adj_ivr/3:.2f}"
            table_body += '\\\\ \\hline'
        return table_body + ivr_file_template.MONTH_FOOTER


    @staticmethod
    def build_year_latex_table(years: list[IvrYearInfo]) -> str:
        table_body = ivr_file_template.YEAR_HEADER
        for y in years:
            table_body += f"{y.when} & {y.brute_ivr_diff:.2f} & {y.val_correcao:.2f} & {y.adj_ivr_diff:.2f}"
            table_body += '\\\\ \\hline'
        return table_body + ivr_file_template.YEAR_FOOTER


    @staticmethod
    def build_total_latex_table(report: IvrTotalInfo) -> str:
        table_body = ivr_file_template.TOTAL_HEADER
        table_body += f"{report.brute_ivr_diff:.2f} & {report.val_correcao:.2f} & {report.adj_ivr_diff:.2f}"
        table_body += '\\\\ \\hline'
        return table_body +ivr_file_template.TOTAL_FOOTER

class PdfBuilder:
    @staticmethod
    def write_pdf(dst: str):
        os.chdir(ProjPaths.LATEX_DIR)
        result = subprocess.run(["xelatex", path.split(ProjPaths.LATEX_FILE_PATH)[-1]], timeout=15*60)
        result.check_returncode() #raise error in fail condition
        shutil.move("laudo.pdf", dst)
        os.chdir(ProjPaths.SCRIPTS_DIR)


def getSIA():
    files = Downloads.find_files(
        "SIA",
        ProjParams.get_state(),
        ProjParams.get_start_date(),
        ProjParams.get_end_date())

    print("will download the follwing files:")
    for file in files:
        print(file)

    Downloads.download(files)
    print("downloads finished")

    Conversions.convert_files()
    print("conversions finished")

    Conversions.unite_files("SIA")
    print("files united")


def getSIH():
    files = Downloads.find_files(
        "SIH",
        ProjParams.get_state(),
        ProjParams.get_start_date(),
        ProjParams.get_end_date())

    print("will download the follwing files:")
    for file in files:
        print(file)

    Downloads.download(files)
    print("downloads finished")

    Conversions.convert_files()
    print("conversions finished")

    Conversions.unite_files("SIH")
    print("files united")


def main():
    ProjPaths.init()
    ProjParams.init()
    InterestRate.load_selic()

    # download e conversão para csv
    if ProjParams.SYSTEM == "SIA":
        getSIA()
    elif ProjParams.SYSTEM == "SIH":
        getSIH()
    elif ProjParams.SYSTEM == "BOTH":
        getSIA()
        ProjPaths.empty_download_dir()
        getSIH()

    #processamento mensal dos csvs (IVR)
    sih_files = [path.join(ProjPaths.SIH_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIH_CSVS_DIR)]
    sia_files = [path.join(ProjPaths.SIA_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIA_CSVS_DIR)]
    result = ProcessingIVR.adjusted_BOTH(sia_files, sih_files)
    IvrCsvBuilder.build_month_report(result)


#main()

ProjParams.init()
ProjPaths.define_paths()
ProjPaths.create_latex_dir()
InterestRate.load_selic()

sia_files = [path.join(ProjPaths.SIA_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIA_CSVS_DIR)]
sih_files = [path.join(ProjPaths.SIH_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIH_CSVS_DIR)]

months_results = ProcessingIVR.adjusted_BOTH(sia_files, sih_files)
year_results = ProcessingIVR.year_results(months_results)
total_results = ProcessingIVR.total_result(months_results)


IvrCsvBuilder.build_month_report(months_results)
IvrCsvBuilder.build_year_report(year_results)
IvrCsvBuilder.build_total_report(total_results)

LatexBuilder.build_IVR_latex_file(months_results, year_results, total_results)
PdfBuilder.write_pdf(dst=ProjPaths.RESULTS_DIR)
