import copy
import random
import os
from time import time as seconds
from collections import Counter

import pandas as pd
import numpy as np
from datetime import datetime

from data import *
from location import *
from scheduling import *
from classes_folder.conexao import AbrirConexao
from classes_folder.menu import menu_insert

import traceback

def location(cod_estudo, cod_modulo, cod_empresa):

    print('Lendo dados de entrada...', flush=True)
    data = Data('strategic')
    
    try:
        data.read(cod_modulo, cod_estudo,cod_empresa) # leitura dos dados
        try:
            print('Executando módulo de posicionamento de bases...', flush=True)
            location = Location(data,cod_modulo,cod_estudo,cod_empresa)

            status=location.run()
        except Exception as erro:
            print(erro)
            msg_status = 'Erro ao executar o módulo estratégico'
            status = 4
            traceback.print_exc()
    except Exception as erro:
        print(erro)
        msg_status = 'Erro na leitura dos dados'
        status = 4
    
    print('\nEscrevendo resultados...', flush=True)
    conexao_oracle=AbrirConexao(cod_empresa)
    
    try:
        if status == 0:
            msg_status = 'Erro de simulação'
        elif status == 4:
            date = datetime.now()
            date = date.strftime('%y/%m/%d %H:%M:%S')
            status_output = pd.DataFrame({'IND_SITUACAO_MODULO_ETTGC': [3], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg_status],
                                   'DTH_RETORNO_MODULO_ESTRATEGICO': [date], 'IDT_CONTROLE_PRCSM_ETUDO': [int(cod_estudo)]})
        elif status == 1:
            print('Escrevendo resultados...', flush=True)
            msg_status = 'Processado'
            menu_insert(conexao_oracle,location.saida_atendimento,'at04')
            menu_insert(conexao_oracle,location.bases_configuration,'at05')
            menu_insert(conexao_oracle,location.electricians_data,'at06')
            menu_insert(conexao_oracle,location.electricians_locality,'at07')
            menu_insert(conexao_oracle,location.general_results,'at08')
    except Exception as erro:
        print(erro)
        status = 4;
        msg_status = 'Erro na escrita do resultados'
        date = datetime.now()
        date = date.strftime('%y/%m/%d %H:%M:%S')
        status_output = pd.DataFrame({'IND_SITUACAO_MODULO_ETTGC': [3], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg_status],
                                   'DTH_RETORNO_MODULO_ESTRATEGICO': [date], 'IDT_CONTROLE_PRCSM_ETUDO': [int(cod_estudo)]})
    finally:
        if status != 1 and status != 0:
            menu_insert(conexao_oracle,status_output,'at14')
        conexao_oracle.con.close()
    print(msg_status)
    print('\nDone!')    
    
    if status == 1:
        location.general_results.to_csv('output/SAIDA_ESTRATEGICO.csv',sep=';',index=False)
        location.saida_atendimento.to_csv('output/SAIDA_ESTRATEGICO_ATENDIMENTO.csv',sep=';',index=False)
        location.bases_configuration.to_csv('output/SAIDA_ESTRATEGICO_BASES.csv',sep=';',index=False)
        location.electricians_data.to_csv('output/SAIDA_ESTRATEGICO_ELETRICISTAS.csv',sep=';',index=False)
        location.electricians_locality.to_csv('output/SAIDA_ESTRATEGICO_ELETRICISTAS_POR_LOCALIDADE.csv',sep=';',index=False)

def sizing(cod_estudo, cod_modulo, cod_empresa):

    print('Lendo dados de entrada para modulo tático...', flush=True)
    data = Data('tatic')
    
    try:
        data.read(cod_modulo, cod_estudo, cod_empresa) # leitura dos dados        
        try:
            print('Executando módulo de dimensionamento...', end='', flush=True)
            sched = Scheduling(data, cod_modulo, cod_estudo)
            status, msg_status, trash = sched.run()
        except:
            msg_status = 'Erro ao executar o módulo tático'
            status = 4
    except Exception as e:
        print(e)
        msg_status = 'Erro na leitura dos dados'
        status = 4
            
    print('\nEscrevendo resultados...', flush=True)
    conexao_oracle=AbrirConexao(cod_empresa)
    
    try:
        if isinstance(status,int) and status == 3:
            status_output = sched.status_output
            status_output.loc[0, 'IND_SITUACAO_PROCESSAMENTO'] = status
            status_output.loc[0, 'DSC_MENSAGEM_SITUACAO_PRCSM'] = msg_status           
        elif isinstance(status,int) and status == 4:
            date = datetime.now()
            date = date.strftime('%y/%m/%d %H:%M:%S')
            status_output = pd.DataFrame(columns=['IND_SITUACAO_PROCESSAMENTO','DSC_MENSAGEM_SITUACAO_PRCSM','DTH_INCLUSAO_RETORNO', 'CODIGO_DO_ESTUDO'])
            status_output.loc[0] = [3, msg_status, date, int(cod_estudo)]
        else:
            msg_status = 'Processado'
            status_output = sched.status_output
            print('Escrevendo SAIDA_TATICO_ESCALAS', flush=True)
            menu_insert(conexao_oracle,sched.elect_by_sched,'at09')
            print('Escrevendo SAIDA_TATICO_GERAL', flush=True)
            menu_insert(conexao_oracle,sched.gen_output,'at10')
            print('Escrevendo SAIDA_TATICO_POR_GARAGEM', flush=True)
            menu_insert(conexao_oracle,sched.base_output,'at11')
            print('Escrevendo RETORNO_GRAFICO_TATICO', flush=True)
            menu_insert(conexao_oracle,sched.plots_output,'at17')
    except Exception as erro:
        print(erro)
        msg_status = 'Erro na escrita do resultados'
        date = datetime.now()
        date = date.strftime('%y/%m/%d %H:%M:%S')
        status_output = pd.DataFrame(columns=['IND_SITUACAO_PROCESSAMENTO','DSC_MENSAGEM_SITUACAO_PRCSM','DTH_INCLUSAO_RETORNO', 'CODIGO_DO_ESTUDO'])
        status_output.loc[0] = [3, msg_status, date, int(cod_estudo)]       
    finally:  
        print('Escrevendo PARAMETRO_CONTROLE_PRCSM_ETUDO', flush=True)
        menu_insert(conexao_oracle,status_output,'at15')
        conexao_oracle.con.close()
    print(msg_status)
    print('\nDone!') 
    
    # print('Escrevendo resultados em csv...', flush=True)
    # sched.elect_by_sched.to_csv('output/SAIDA_TATICO_ESCALAS.csv', sep=';', encoding='latin-1', index=False)
    # sched.gen_output.to_csv('output/SAIDA_TATICO_GERAL.csv', sep=';', encoding='latin-1', index=False)
    # sched.base_output.to_csv('output/SAIDA_TATICO_POR_GARAGEM.csv', sep=';', encoding='latin-1', index=False)
    # sched.plots_output.to_csv('output/SAIDA_TATICO_PLOTS.csv', sep=';', encoding='latin-1', index=False)
    # sched.status_output.to_csv('output/PARAMETRO_CONTROLE_PRCSM_ETUDO.csv', sep=';', encoding='latin-1', index=False)

def scheduling(cod_estudo, cod_modulo, cod_empresa):

    print('Lendo dados de entrada para módulo operacional...', flush=True)
    data = Data('operational')
    
    try:
        data.read(cod_modulo, cod_estudo, cod_empresa) # leitura dos dados
        try:
            print('Executando módulo de definição de escalas...', end='', flush=True)
            sched = Scheduling(data, cod_modulo, cod_estudo)
            status, msg_status, trash = sched.run(sizing=False)
        except:
            msg_status = 'Erro ao executar o modulo operacional'
            status = 4
    except:
        msg_status = 'Erro na leitura dos dados'
        status = 4
        
    print('\nEscrevendo resultados...', flush=True)
    conexao_oracle=AbrirConexao(cod_empresa)
    
    try:
        if status == 3:
            status_output = sched.status_output
            status_output.loc[0, 'IND_SITUACAO_PROCESSAMENTO'] = status
            status_output.loc[0, 'DSC_MENSAGEM_SITUACAO_PRCSM'] = msg_status           
        elif status == 4:
            date = datetime.now()
            date = date.strftime('%y/%m/%d %H:%M:%S')
            status_output = pd.DataFrame(columns=['IND_SITUACAO_PROCESSAMENTO','DSC_MENSAGEM_SITUACAO_PRCSM','DTH_INCLUSAO_RETORNO', 'CODIGO_DO_ESTUDO'])
            status_output.loc[0] = [3, msg_status, date, int(cod_estudo)]       
        else:
            msg_status = 'Processado'
            status_output = sched.status_output
            print('Escrevendo SAIDA_OPERACIONAL_ESCALAS', flush=True)
            menu_insert(conexao_oracle,sched.elect_by_sched,'at12')
            print('Escrevendo RETORNO_GRAFICO_OPERACIONAL', flush=True)            
            menu_insert(conexao_oracle,sched.plots_output,'at18') 
            sched.plots_output.to_csv('RETORNO_GRAFICO_OPERACIONAL.csv',sep=';')   # Remover - RL
            
    except:
        msg_status = 'Erro na escrita do resultados'
        date = datetime.now()
        date = date.strftime('%y/%m/%d %H:%M:%S')
        status_output = pd.DataFrame(columns=['IND_SITUACAO_PROCESSAMENTO','DSC_MENSAGEM_SITUACAO_PRCSM','DTH_INCLUSAO_RETORNO', 'CODIGO_DO_ESTUDO'])
        status_output.loc[0] = [3, msg_status, date, int(cod_estudo)]       
    finally:
        print('Escrevendo PARAMETRO_CONTROLE_PRCSM_ETUDO', flush=True)
        menu_insert(conexao_oracle,status_output,'at16')
        conexao_oracle.con.close()
    print(msg_status)
    print('\nDone!')    
    
    # print('Escrevendo resultados em csv...', flush=True)
    # sched.elect_by_sched.to_csv('output/SAIDA_OPERACIONAL_ESCALAS.csv', sep=';', encoding='latin-1', index=False)
    # sched.plots_output.to_csv('output/SAIDA_OPERACIONAL_PLOTS.csv', sep=';', encoding='latin-1', index=False)
    # sched.status_output.to_csv('output/PARAMETRO_CONTROLE_PRCSM_ETUDO.csv', sep=';', encoding='latin-1', index=False)
    #cputime = sched.runtime # tempo total de execução do módulo
