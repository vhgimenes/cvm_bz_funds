"""
Projeto: CVM_FUNDS

Módulo responsável pelo:
    (i)  Download dos informes diários e Informações cadastrais do site da CVM em formato csv e; 
    (ii) Armazenamento desses arquivos na pasta raw (para posteriormente serem armazarnados na base de dados)
"""
#%%
#Ignoras warnings desnecessários
import warnings
warnings.filterwarnings("ignore")

#1: Importando libs
import pandas as pd
from urllib.request import HTTPError
from datetime import datetime
import time
import requests
import zipfile
import os
import dateutil.relativedelta
from datetime import datetime
import workdays as wd
import time

#2: Importando módulo com meu logger customizado
import sys
sys.path.insert(0,r'T:\GESTAO\MACRO\DEV\AUXILIARES')
from my_logging import get_logger
from feriadosAnbima import holidays

# Instanciando o local que o logg desse módulo será salvo e o seu nome
global logg_path
global logger_name
logg_path = r'T:\GESTAO\MACRO\DEV\LOGGS\MAIN.txt'
logger_name = 'CVM_FUNDS_DOWNLOAD'

def load_cadastro_cvm():
    """
    Função criada para buscar as informações cadastrais de dentro do site da CVM. 

    Returns:
        pandas dataframe: informação cadastral dos fundos na CVM.
    """
    file_name = 'cad_fi.csv'
    path = os.path.join('./raw',file_name)
    try:
        url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
        cadastro = pd.read_csv(url, sep=';', encoding='ISO-8859-1')
        cadastro.to_csv(path, sep=';', index=False)
    except Exception as e:
        print(f'Error in {file_name}: {e}')
        raise

def load_informes_cvm(year: int, mth: int, db_cvm_reference_year: int):
    # sourcery skip: extract-duplicate-method, extract-method
    """
    Função principal criada para buscar os informes diários de dentro do site da CVM.

    Args:
        year (int): ano de referência.
        mth (int):  mês de referência.
        db_cvm_reference_year (int): ano em que o código deve começar a procurar os zips de históricos dentro do site.

    Returns:
        pd.DataFrame: informes dos fundos.
    """
    # Criando o o destino e nome dos arquivos
    mth = f"{mth:02d}"
    year_ref = str(year)
    file_name = f'inf_diario_fi_{year_ref}{mth}.csv'
    path = os.path.join('./raw',file_name)
    # A depender do ano de referência iremos escolher um método para acessar o csv
    if year >= db_cvm_reference_year:
        # Os informes mais recentes são disponibilizados (espermos até aqui que em até ano-1) em zips dividos pelo mês e ano de referência contendo apenas um csv
        try:
            #creates url using the parameters provided to the function
            url = f'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year_ref}' + mth + '.zip'
            #reads the csv inside the zip link
            cotas = pd.read_csv(url, delimiter=';', encoding='ISO-8859-1', dtype='str', compression='zip')
            cotas.to_csv(path, sep=';', index=False)
            return True
        except Exception as e:
            print(f'Error in {file_name}: {e}.\n')
            raise
    if year < db_cvm_reference_year:
        # O históricos de informes são disponibilizados dentro de zips contendo vários csv's dividos por datas
        try:
            url = f'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year_ref}.zip'
            #sends request to the url
            r = requests.get(url, stream=True, allow_redirects=True)
            #writes the .zip file downloaded
            zip_name = f'informe{year}.zip'
            with open(zip_name, 'wb') as fd: 
                fd.write(r.content)
            zip_inf = zipfile.ZipFile(zip_name)
            # procura o arquivo csv dentro do arquivo zip
            cotas = pd.read_csv(zip_inf.open(file_name), sep=";", encoding='ISO-8859-1', dtype='str') 
            cotas.to_csv(path, sep=';', index=False)            
            return True
        except Exception as e:
            print(f'Error in {zip_name}: {e}.\n')
            raise 
        finally: #This clause is executed no matter what, and is generally used to release external resources.
            if zip_inf is not None:
                zip_inf.close() #fecha o arquivo zip
                os.remove(f'informe{year}.zip')
    if year < 2005:
        raise ValueError('Erro: theres no report for this date!.\n')

def main(recalc: int = 0):
    #Instanciando meu logger
    logger = get_logger(logger_name,logg_path)
    logger.info(msg="Iniciando rotina de extração CVM...")
    t = time.time()
    #Criando as datas para buscar das informações
    today = datetime.now()
    # identificando qual data exata devemos pegar (D-2)
    end_date = wd.workday(today.date(),-2, holidays())
    #Os informes são atualizados diariamente com informações de até mês-11
    if recalc ==0: # pegaremos somente os meses passíveis de revisão e updates
        initial_date = datetime(2005,1,1).date() 
    else: #reiniciaremos a base
        initial_date = end_date - dateutil.relativedelta.relativedelta(months=11)
    #A data de mudança de busca das informações de dentro do site da CVM #TODO: REVISAR COM O TEMPO COMO O SITE SE REAJUSTA O HISTÓRICO DOS DADOS
    db_cvm_reference_year = end_date.year - 1
    logger.info(msg="Iniciando a extração e armazenamento dos informes...")
    while initial_date <= end_date:
        mth = initial_date.month
        year = initial_date.year
        try:
            logger.debug(msg=f"Extraindo o informe: {mth:02d}/{year}.")
            load_informes_cvm(year, mth, db_cvm_reference_year)
            logger.debug(msg="Extração realizada com sucesso!")
        except Exception as e:
            logger.error(msg=f"Falha na extração do informe: {mth:02d}/{year}.",exc_info=True)
            logger.critical(msg="Erro crítico: stopando a rotina.")
            raise
        initial_date = initial_date + dateutil.relativedelta.relativedelta(months=1)
    logger.info(msg="Extração e armazenamento dos informes realizados com sucesso!")

    #Iniciando a extração e armazenamento da informação cadastral 
    logger.info(msg="Iniciando rotina de extração das informações cadastrais.")
    try:
        load_cadastro_cvm()
        logger.debug(msg="Extração realizada com sucesso!")
    except Exception as e:
        logger.error(msg=f"Falha na extração do informação cadastral: {e}.",exc_info=True)
        logger.critical(msg="Erro crítico: stopando a rotina.")
        raise
    t = time.time() - t
    logger.info(msg="Rotina finalizada com sucesso!")
    logger.info(msg=f"Tempo de execução: {t} seconds")
    
if __name__ == '__main__':
    main(1)