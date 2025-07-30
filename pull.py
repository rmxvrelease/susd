# flake8: noqa

import ftplib as ftp
import os
import sys
import time as t
from multiprocessing.dummy import Pool
from pathlib import Path

import arquivos_pa_e_sp
import laudo_final
import pandas as pd
import processar_dados_sia
import processar_dados_sih
import sigtap_procedimento
from tempo import Tdata

# python3 pull.py BOTH RS 01-24 01-24 2248328 Ambos
# TODO: descobir como exportar pdf para o front end
# TODO: reorganizar o codigo para carregar os arquivos sigtap uma vez ao mês, fazer o mesmo para o arquivo da selic

searchDirs = {
    'SIA': ["/dissemin/publicos/SIASUS/199407_200712/Dados", "/dissemin/publicos/SIASUS/200801_/Dados"],
    'SIH': ["/dissemin/publicos/SIHSUS/199201_200712/Dados", "/dissemin/publicos/SIHSUS/200801_/Dados"]
}

search_prefix = {
    'SIA': 'PA',
    'SIH': 'SP'
}

# padrão de chamada do programa:
# python pull.py <SIA/SIH> <estado>TabelaUnificada_202503_v2503101901.zip <data-inicio> <data-fim> <CNES>

def main():
    try:
        verify_dependencies()
        python_file = sys.argv[0]
        python_file_dir = os.path.dirname(python_file)
        # os.chdir(python_file_dir)

        CarregaSelic()

        args = sys.argv[1:]
        if not validate_args(args): return
        print(args)

        sistema = args[0]
        estado = args[1]
        data_inicio = Tdata.str_to_data(args[2])
        data_fim = Tdata.str_to_data(args[3])
        cnes = args[4]
        tipo_valor = args[5]

        subdirectory_name = create_subdirectory(cnes, estado)

        if not sigtap(Tdata.data_atual_aaaamm()):
            print("AVISO: Não foi possível carregar arquivos SIGTAP")
        get_and_process_data(estado, data_inicio, data_fim, sistema, cnes, subdirectory_name, tipo_valor)
        unite_files(subdirectory_name)

    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}", file=sys.stderr)
        sys.exit(1)


def get_day():
    date = t.localtime()
    mes_str = ""
    if (date.tm_mon >= 10):
        mes_str = str(date.tm_mon)
    else:
        mes_str = f"0{date.tm_mon}"

    if (date.tm_mday >= 10):
        day_str = str(date.tm_mday)
    else:
        day_str = f"0{date.tm_mday}"

    return f"{day_str}/{mes_str}/{date.tm_year}"


def CarregaSelic():
    today_str = get_day()
    print(today_str)
    url_bcb = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=csv&dataInicial=01/12/2021&dataFinal={today_str}"
    try: selic = pd.read_csv(url_bcb, sep=";")
    except: return
    selic['valor'] = selic['valor'].astype(str).str.replace(",", ".").astype(float)
    selic['valor'] = (selic['valor']/100) + 1
    lista_selic = selic['valor'].__array__()
    file_descriptor = open(get_path(DADOS_DIR, 'selic.txt'), "w")
    file_descriptor.write(str(lista_selic.tolist()))
    file_descriptor.close;

def getSelic() -> list[float]:
    file_descriptor = open(get_path(DADOS_DIR, 'selic.txt'), "r")
    lista = [float(x) for x in (file_descriptor.readline()[1:-1]).split(',')]
    file_descriptor.close()
    return lista

# Configuração absoluta de caminhos
def get_base_dir():
    """Retorna o diretório base absoluto do projeto"""
    try:
        return Path(__file__).parent.parent
    except NameError:
        return Path.cwd() / 'scripts' / 'susprocessing'

BASE_DIR = get_base_dir()

def get_path(*parts):
    """Constrói caminhos absolutos de forma confiável"""
    return str(BASE_DIR.joinpath(*parts))

# Configuração de diretórios
EXES_DIR = get_path('exes')
DADOS_DIR = get_path('dados')
SCRIPTS_DIR = get_path('scripts')

# Verificação de dependências
def verify_dependencies():
    required = {
        'blast-dbf': os.path.join(EXES_DIR, 'blast-dbf'),
        'DBF2CSV': os.path.join(EXES_DIR, 'DBF2CSV'),
        'unzip': os.path.join(EXES_DIR, 'unzip')
    }

    missing = [name for name, path in required.items() if not os.path.exists(path)]
    if missing:
        raise FileNotFoundError(
            f"Ferramentas necessárias não encontradas: {', '.join(missing)}\n"
            f"Por favor, instale em {EXES_DIR}"
        )

def create_subdirectory(cnes: str, estado: str):
    subdirectory_name = f'H{cnes}{estado}'
    subdirectory_path = get_path()
    print("diretório antes da criação\n", os.listdir(subdirectory_path))
    os.makedirs(subdirectory_path, exist_ok=True)
    os.makedirs(get_path(subdirectory_name, 'downloads'), exist_ok=True)
    os.makedirs(get_path(subdirectory_name, 'dbfs'), exist_ok=True)
    os.makedirs(get_path(subdirectory_name, 'csvs'), exist_ok=True)
    os.makedirs(get_path(subdirectory_name, 'finalcsvs'), exist_ok=True)
    os.makedirs(get_path(subdirectory_name, 'laudos'), exist_ok=True)
    print("diretório depois da criação\n", os.listdir(subdirectory_path))
    return subdirectory_name

def validate_args(args: list[str]) -> bool:
    if len(args) != 6:
        print("Número de argumentos fornecidos é inválido")
        return False


    if args[0] not in ['SIA', 'SIH', 'BOTH']:
        print("sistema inválido:", args[0])
        return False


    #essa condição não impede a execução
    if len(args[4]) != 7:
        print(f"WARNING: É possível que o cnes {args[4]} seja inválido")

    try:
        data_inicio = Tdata.str_to_data(args[2])
    except:
        print(f"data de início em um formato inválido: {args[2]}")

    try:
        data_fim = Tdata.str_to_data(args[3])
    except:
        print(f"data de final em um formato inválido: {args[2]}")


    if data_fim < data_inicio:
        print("Data de início maior que data de fim")
        return False

    if args[5] not in ['IVR', 'TUNEP', 'Ambos']:
        print("sistema inválido:", args[5])
        return False

    return True

def find_files_of_interest(estado: str, data_inicio: Tdata, data_fim: Tdata, sih_sia: str) -> list[str]:
    files = []
    search_dirs = searchDirs[sih_sia]
    ftp_client = ftp.FTP("ftp.datasus.gov.br")

    try: ftp_client.login()
    except:
        print("não foi possível fazer login no ftp do sus")

    for dir in search_dirs:
        print(f"{dir} <---- vasculhando diretório")
        def append_to_file(file: str):
            file = file.split(' ')[-1]
            dateString =  file[6:8] + "-" + file[4:6]
            try: date = Tdata.str_to_data(dateString)
            except: return

            if file[0:2] != search_prefix[sih_sia] or estado != file[2:4] or date < data_inicio or data_fim < date:
                return
            files.append(dir + "/" + file)

        ftp_client.cwd(dir)
        ftp_client.retrlines("LIST", append_to_file)
    ftp_client.quit()
    return files

def get_and_process_data(estado: str, data_inicio: Tdata, data_fim: Tdata, sia_sih: str, cnes: str, subdirectory_name: str, tipo_valor: str):
    if (sia_sih == 'BOTH'):  # caso especial no qual os dois sistemas são selecionados
        get_and_process_data(estado, data_inicio, data_fim, 'SIA', cnes, subdirectory_name, tipo_valor)
        get_and_process_data(estado, data_inicio, data_fim, 'SIH', cnes, subdirectory_name, tipo_valor)
        return

    print(f"processando {sia_sih}:")

    files_of_interest = find_files_of_interest(estado, data_inicio, data_fim, sia_sih)
    print(f"Arquivos a serem baixados:\n{files_of_interest}")


    with Pool(10) as p:
        print([[file, cnes, sia_sih, subdirectory_name, tipo_valor] for file in files_of_interest])
        p.map(dowload_e_processamento, [[file, cnes, sia_sih, subdirectory_name, tipo_valor] for file in files_of_interest])


def unite_files(subdirectory_name: str):
    print("Unindo arquivos para gerar laudo final...")
    try:
        csv_dir = get_path(subdirectory_name, 'csvs')
        csv_final_dir = get_path(subdirectory_name, 'finalcsvs')
        laudos_dir = get_path(subdirectory_name, 'laudos')
        if not os.path.exists(csv_final_dir) or not os.path.exists(csv_dir):
            raise FileNotFoundError(f"Diretório não encontrado")

        laudo_final.main(csv_final_dir, laudos_dir)
        arquivos_pa_e_sp.main(csv_dir, laudos_dir)
    except Exception as e:
        print(f"Erro ao unir arquivos: {str(e)}")
        raise


def file_was_already_downloaded(file_path: str) -> bool:
    return os.path.exists(file_path)


def dowload_from_ftp(ftp_server: str, remote_path: str, local_dir: str):
    try:
        print(f"Iniciando download de {remote_path}")
        remote_dir, remote_file = os.path.split(remote_path)

        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        local_file = os.path.join(local_dir, remote_file)

        ftp_client = ftp.FTP(ftp_server)
        ftp_client.login()
        ftp_client.cwd(remote_dir)

        with open(local_file, 'wb') as file:
            ftp_client.retrbinary(f'RETR {remote_file}', file.write)
        ftp_client.quit()
        print(f"Download de {remote_file} concluído com sucesso.")
        return
    except Exception as e:
        print(f"Falha no download de {remote_path}: {str(e)}")
        print("É provável que o servidor do sus não esteja funcionando como esperado")
        return


def sigtap(data: str):
    try:
        print("Carregando arquivos SIGTAP...")
        arquivo_mais_recente = sigtap_procedimento.arquivos_procedimentos_ftp(data)

        if not arquivo_mais_recente:
            print("Nenhum arquivo SIGTAP encontrado")
            return False

        zip_path = get_path('dados', arquivo_mais_recente)

        print(f"Extraindo {arquivo_mais_recente}...")
        err = os.system(f"{get_path('exes', 'unzip')} {zip_path} {DADOS_DIR}")
        if(err != 0):
            print(f'erro ao descompactar {arquivo_mais_recente}')
            return False

        sigtap_procedimento.descricao_procedimento(
            get_path('dados', 'tb_procedimento.txt'),
            get_path('dados', 'desc_procedimento.csv')
        )

        sigtap_procedimento.origem_sia_sih(
            get_path('dados', 'rl_procedimento_sia_sih.txt'),
            get_path('dados', 'origem_sia_sih.csv')
        )

        print("Processamento SIGTAP concluído")
        return True

    except Exception as e:
        print(f"Erro no processamento SIGTAP: {str(e)}")
        return False


def dowload_e_processamento(file_and_cnes: list[str]):
    file = file_and_cnes[0]
    cnes = file_and_cnes[1]
    sih_sia = file_and_cnes[2]
    subdirectory_name = file_and_cnes[3]
    tipo_valor = file_and_cnes[4]
    fileName = os.path.split(file)[1]

    try:
        start_time = Tdata.str_to_data(f"{fileName[6:8]}-{fileName[4:6]}")
    except:
        print(f"a data do arquivo {file} parece não estar em conformidade com o padrão esperado")
        return


    download_path = get_path(subdirectory_name, 'downloads', fileName)
    dbf_path = get_path(subdirectory_name, 'dbfs', f"{fileName[:-4]}.dbf")
    csv_path = get_path(subdirectory_name, 'csvs', f"{fileName[:-4]}.csv")
    final_csv_path = get_path(subdirectory_name, 'finalcsvs', f"{fileName[:-4]}.csv")

    if not file_was_already_downloaded(download_path):
        print(f"Download de {file}...")
        dowload_from_ftp("ftp.datasus.gov.br", file, os.path.dirname(download_path))

    print("Conversão para dbf...")
    os.system(f"{get_path('exes', 'blast-dbf')} {download_path} {dbf_path}")

    print("Conversão para csv...")
    os.system(f"{get_path('exes', 'DBF2CSV')} {dbf_path} {csv_path} {cnes} {sih_sia}")

    print("Processando dados do csv por cnes...")

    if (sih_sia == 'SIA'):
        processar_dados_sia.processar_dados_csv(csv_path, final_csv_path, start_time, Tdata.current_data(), tipo_valor)
    else:
        processar_dados_sih.processar_dados_csv(csv_path, final_csv_path, start_time, Tdata.current_data(), tipo_valor)

    print(f"removendo ../{subdirectory_name}/downloads/{fileName}")
    os.remove(download_path)

    print(f"removendo ../{subdirectory_name}/dbfs/{fileName[:-4]}.dbf")
    os.remove(dbf_path)


main()
