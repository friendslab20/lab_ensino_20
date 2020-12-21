import pandas as pd
import os
import math

from classes_folder.receber_e_separar_queries import ReceberESeparar
from classes_folder.conexao import AbrirConexao
from classes_folder.menu import menu_select

# Data armazena todos os dataframes utilizados pelo módulo
class Data:

    def __init__(self,modulo=None):
        self.modulo = modulo
        
    # Função de leitura dos dados de entrada
    def read(self,cod_modulo,cod_estudo,cod_empresa):
        self.cod_modulo = cod_modulo
        self.cod_estudo = cod_estudo

        queries = ReceberESeparar('query_list/select/queries.txt')        
        conexao_oracle = AbrirConexao(cod_empresa) # abre conexao com banco de dados

        if self.modulo =='forecast': #        
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Lendo dados para simulacao' where IDT_CONTROLE_PRCSM_ETUDO=" +str(cod_estudo)
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            print("\nLendo dados para simulação da previsão...\n Código do estudo: ",cod_estudo)   # Status - RL
            #self.os_input_forecast=pd.read_csv('input/OS_ENTRADA_SIDEC.csv', sep=";", encoding='latin-1')
            #self.services = pd.read_csv('input/SERVICOS_PREVISAO.csv', sep=";", index_col='IDT_BLOCO_SERVICO', encoding='latin-1')
            #CS01 OK
            os_input_forecast=menu_select(conexao_oracle, "CS01", cod_estudo)[0]
            print("\ndata.py, linha 32, CS01: ",os_input_forecast)   # Remover - RL
            print("\ndata.py, linha 33, CS01, dtype: ",os_input_forecast.dtypes)
            print("\ndata.py, linha 34, CS01, columns: ",os_input_forecast.columns)
            self.os_input_forecast=os_input_forecast
            self.os_input_forecast.to_csv('CS01.csv',sep=';')          # Comentar - RL
            
            #CS02 OK
            services_raw = menu_select(conexao_oracle,'CS02',cod_estudo) # obtem tabela bruta do banco de dados
            # services_raw = services_raw[0].drop('COD_SERVICO', 1)            
            # services_raw = services_raw.drop('COD_PERFIL_EQUIPE', 1)
            # services_raw = services_raw.drop_duplicates()        
            # services_raw = services_raw.rename(columns={"IDT_BLOCO_SERVICO": "COD_SERVICO", "IDT_DEPARTAMENTO_EQUIPE": "DEPARTAMENTO", "IND_TIPO_OCORRENCIA" : "TIPO_OCORRENCIA",
            #                             "IND_TMD_EXCECAO" : "TMD_INTERNO_EXCECAO", "VLR_TME_BLOCO_SERVICO" : "TME", "VLR_TMP_BLOCO_SERVICO" : "TMP" })
            # services_raw = services_raw.set_index('COD_SERVICO')
            self.services = services_raw[0]
            self.services = self.services.set_index('IDT_BLOCO_SERVICO')
            print("\ndata.py, linha 46, cs02: ",self.services)    # Remover - RL
            print("\ndata.py, linha 47, cs02, dtype: ",self.services.dtypes)
            print("\ndata.py, linha 48, cs02, columns: ",self.services.columns)
            self.services.to_csv('CS02.csv',sep=';')            # Serviços.csv Comentar - RL
            
            #CS03 OK    
            locs_bases_forecast=menu_select(conexao_oracle, "CS03", cod_estudo)[0] 
            locs_bases_forecast=locs_bases_forecast.set_index('COD_LOCALIDADE')
            self.locs_bases_forecast=locs_bases_forecast
            #self.locs_bases_forecast.to_csv('bases.csv',sep=';')
            
        elif self.modulo =='strategic':
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Lendo dados para simulacao' where IDT_CONTROLE_PRCSM_ETUDO=" +str(cod_estudo)
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            #self.os_volume = pd.read_csv('input/PREVISAO_DEMANDA_VOLUME.csv', sep=";", encoding='latin-1')
            #self.services = pd.read_csv('input/SERVICOS.csv', sep=";", index_col='COD_SERVICO', encoding='latin-1')
            #self.locs_bases = pd.read_csv('input/LOCALIDADES.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            #self.bases_profiles = pd.read_csv('input/PERFIL_BASE.csv', sep=";", encoding='latin-1')
            #self.electrician_profiles = pd.read_csv('input/PERFIL_ELETRICISTA.csv', sep=";")
            #self.crew_profiles = pd.read_csv('input/PERFIL_EQUIPE.csv', sep=";", index_col='COD_PERFIL_EQUIPE', encoding='latin-1')
            #self.crew_formation = pd.read_csv('input/EQUIPE_FORMACAO.csv', sep=";", encoding='latin-1')
            #self.crew_service = pd.read_csv('input/EQUIPE_SERVICO.csv', sep=";", encoding='latin-1')
            #self.electrician_per_loc = pd.read_csv('input/ELETRICISTAS_POR_LOCALIDADE.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            #self.loc_distances = pd.read_csv('input/DISTANCIA_ENTRE_LOCALIDADES.csv', sep=";",  index_col='COD_LOCALIDADE')
            #self.loc_times = pd.read_csv('input/TEMPO_ENTRE_LOCALIDADES.csv', sep=";", index_col='COD_LOCALIDADE')
            #self.electricians_costs = pd.read_csv('input/CUSTO_ELETRICISTA.csv', sep=";", index_col='COD_PERFIL_ELETRICISTA', encoding='latin-1')
            #self.os_hour = pd.read_csv('input/PREVISAO_DEMANDA_POR_HORA.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            #self.schedule = pd.read_csv('input/ESCALAS.csv', sep=";", encoding='latin-1')
            #self.vehicles = pd.read_csv('input/PERFIL_VEICULO.csv', sep=";", index_col='COD_PERFIL_VEICULO', encoding='latin-1')
            #self.misc_input = pd.read_csv('input/OUTRAS_ENTRADAS.csv', sep=";", encoding='latin-1', index_col='PARAMETRO')
            #self.depot = pd.read_csv('input/ATENDIMENTO_BASE_LOCALIDADE.csv', sep=';')
            #self.clients = pd.read_csv('input/CLIENTES.csv', sep=";", encoding='latin-1')
            #self.os_emergency = pd.read_csv('input/PREVISAO_DEMANDA_EMERGENCIAL.csv', sep=";", encoding='latin-1')
            #self.veicle_km_costs = pd.read_csv('input/CUSTO_KM.csv', sep=";", index_col='COD_PERFIL_VEICULO', encoding='latin-1')
            #self.hotel_cost = pd.read_csv('input/CUSTO_HOTEL.csv', sep=";", encoding='latin-1')['DIARIA'].iat[0]
            #self.bases_costs = pd.read_csv('input/CUSTO_BASE.csv', index_col='COD_PERFIL_BASE', sep=";", encoding='latin-1')
        
            #CS02 OK
            services_raw = menu_select(conexao_oracle,'CS02',cod_estudo) # obtem tabela bruta do banco de dados
            services_raw = services_raw[0].drop('COD_SERVICO', 1)            
            services_raw = services_raw.drop('COD_PERFIL_EQUIPE', 1)
            services_raw = services_raw.drop_duplicates()        
            services_raw = services_raw.rename(columns={"IDT_BLOCO_SERVICO": "COD_SERVICO", "IDT_DEPARTAMENTO_EQUIPE": "DEPARTAMENTO", "IND_TIPO_OCORRENCIA" : "TIPO_OCORRENCIA",
                                        "IND_TMD_EXCECAO" : "TMD_INTERNO_EXCECAO", "VLR_TME_BLOCO_SERVICO" : "TME", "VLR_TMP_BLOCO_SERVICO" : "TMP" })
            services_raw = services_raw.set_index('COD_SERVICO')
            self.services = services_raw
            #print("\nData.py, linha 89, self.services TME antes da conversão: \n",self.services['TME'])   # Remover - RL
            #print("\nself.services TMP antes da conversão: \n",self.services['TMP'])   # Remover - RL
            self.services['TME'] = self.services['TME']/60   
            self.services['TMP'] = self.services['TMP']/60
            #print("\nData.py, linha 93, self.services TME após conversão: \n",self.services['TME'])   # Remover - RL
            #print("\nself.services TMP após conversão: \n",self.services['TMP'])   # Remover - RL
            self.services.to_csv('SERVICOS.csv',sep=';')

            #CS15 OK
            locs_bases=menu_select(conexao_oracle, "CS15", cod_estudo)[0]            
            locs_bases=locs_bases.rename(columns={'VLR_TMD_INTERNO':'TEMPO_MEDIO_ENTRE_OS','VLR_TMD_EXCECAO':'TEMPO_MEDIO_ENTRE_OS_EXCESSAO','IND_BASE_FIXA':'BASE_FIXA'})        
            self.locs_bases=locs_bases.set_index('COD_LOCALIDADE')       
            self.locs_bases.to_csv('LOCALIDADES.csv',sep=';')
            
            #CS32 OK 
            os_volume_raw=menu_select(conexao_oracle, "CS32", cod_estudo)[0]
            os_volume_cols = ['COD_LOCALIDADE','ANO','MES']
            for s in self.services.index.values:
                os_volume_cols.append('SERVICO '+str(s))
            self.os_volume = pd.DataFrame(columns=os_volume_cols)
            self.os_volume = self.os_volume.set_index(['COD_LOCALIDADE', 'ANO', 'MES'])
            for index, row in os_volume_raw.iterrows():
                loc, year = row['COD_LOCALIDADE_VIRTUAL'], row['ANO_PREVISAO_DEMANDA']
                month, serv = row['MES_PREVISAO_DEMANDA'], row['IDT_BLOCO_SERVICO']
                vol = row['QTD_OS_DEMANDA']
                if self.os_volume.index.isin([(loc, year, month)]).any():
                    self.os_volume.loc[(loc, year, month), 'SERVICO '+str(serv)] = vol
                else:
                    new_row =  [0 for s in self.services.index.values]
                    for (i,s) in enumerate(self.services.index.values):
                        if s == serv:
                            new_row[i] = vol
                    self.os_volume.loc[(loc, year, month)] = new_row
            self.os_volume = self.os_volume.reset_index()       
            self.os_volume.to_csv('PREVISAO_DEMANDA_VOLUME.csv',sep=';')

            #CS16 OK  
            bases_profiles=menu_select(conexao_oracle, "CS16", cod_estudo)[0]
            self.bases_profiles=bases_profiles
            self.bases_profiles.to_csv('PERFIL_BASE.csv',sep=';')
            
            # CS07 OK 
            electrician_profiles_raw= menu_select(conexao_oracle, "CS07", cod_estudo)[0]   
            electrician_profiles_raw=electrician_profiles_raw.rename(columns={'DSC_PERFIL':'DESC_PERFIL_ELETRICISTA','VLR_CUSTO_CONTRATACAO':'CUSTO_CONTRATACAO','VLR_CUSTO_DEMISSAO':'CUSTO_DEMISSAO','VLR_CUSTO_TRANSFERENCIA':'CUSTO_TRANSFERENCIA',
                'VLR_CUSTO_MANUTENCAO':'CUSTO_MANUTENCAO','VLR_CUSTO_HH':'CUSTO_HH','VLR_CUSTO_HH_NOTURNO':'CUSTO_HH_NOTURNO','VLR_CUSTO_HH_FIM_SEMANA':'CUSTO_HH_FDS','VLR_CUSTO_HH_FIM_SEMANA_NOTUR':'CUSTO_HH_FDS_NOTURNO'})
            self.electrician_profiles=electrician_profiles_raw[['COD_PERFIL_ELETRICISTA', 'DESC_PERFIL_ELETRICISTA']]
            self.electrician_profiles.to_csv('PERFIL_ELETRICISTA.csv',sep=';')
            self.electricians_costs=electrician_profiles_raw[['COD_PERFIL_ELETRICISTA','CUSTO_CONTRATACAO','CUSTO_DEMISSAO',
                'CUSTO_TRANSFERENCIA', 'CUSTO_MANUTENCAO','CUSTO_HH','CUSTO_HH_NOTURNO','CUSTO_HH_FDS','CUSTO_HH_FDS_NOTURNO']]
            self.electricians_costs = self.electricians_costs.set_index('COD_PERFIL_ELETRICISTA')
            self.electricians_costs.to_csv('CUSTO_ELETRICISTA.csv',sep=';')

            # CS17 OK
            crew_profiles=menu_select(conexao_oracle, "CS17", cod_estudo)[0]        
            crew_profiles=crew_profiles.rename(columns={'QTD_HH_DIARIO':'HH_TOTAL','VLR_MIP':'MIP','QTD_DIA_TRABALHADO_ANO':'QTD_DIAS_TRABALHO_ANO'})        
            self.crew_profiles=crew_profiles
            self.crew_profiles = self.crew_profiles.set_index('COD_PERFIL_EQUIPE')
            self.crew_profiles.to_csv('PERFIL_EQUIPE.csv',sep=';')

            # CS12 OK
            crew_formation=menu_select(conexao_oracle,  "CS12", cod_estudo)[0]
            #crew_formation=crew_formation.rename(columns={'SUM(PPEBE.QTD_ELETRICISTA)':'QTD_ELETRICISTAS'})
            crew_formation=crew_formation.rename(columns={'QTD_ELETRICISTA':'QTD_ELETRICISTAS'})
            self.crew_formation=crew_formation
            self.crew_formation.to_csv('EQUIPE_FORMACAO.csv',sep=';')
            
            # CS13 OK
            crew_service_strategic=menu_select(conexao_oracle, "CS13", cod_estudo)[0]
            self.crew_service =crew_service_strategic.rename(columns={'IDT_BLOCO_SERVICO':'COD_SERVICO'})
            self.crew_service.to_csv('EQUIPE_SERVICO.csv',sep=';')
            
            # CS11  OK
            electrician_per_loc=menu_select(conexao_oracle, "CS11", cod_estudo)[0]
            electrician_per_loc=electrician_per_loc.rename(columns={'COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE','SUM(PPEBE.QTD_ELETRICISTA)':'QTD_ELETRICISTAS'})
            self.electrician_per_loc=electrician_per_loc.set_index('COD_LOCALIDADE')
            self.electrician_per_loc.to_csv('ELETRICISTAS_POR_LOCALIDADE.csv',sep=';')
            
            # CS10 OK 
            loc_distances_raw=menu_select(conexao_oracle, "CS10", cod_estudo)[0]
            L_ = sorted(self.locs_bases.index.values)
            drows = []
            for i in L_:
                drow = [i]
                for j in L_:
                    vlr = loc_distances_raw.loc[(loc_distances_raw['COD_LOCALIDADE_ORIGEM']==i) &
                                                (loc_distances_raw['COD_LOCALIDADE_DESTINO']==j), 
                                                'VLR_DISTANCIA'].values
                    if not vlr or math.isnan(vlr):
                        vlr = loc_distances_raw.loc[(loc_distances_raw['COD_LOCALIDADE_ORIGEM']==j) &
                                                (loc_distances_raw['COD_LOCALIDADE_DESTINO']==i), 
                                                'VLR_DISTANCIA'].values
                        if not vlr or math.isnan(vlr):
                            drow.append(0.0)
                        else:
                            drow.append(float(vlr))
                    else:
                        drow.append(float(vlr))
                drows.append(drow)
            dist_cols = ['COD_LOCALIDADE']
            for l in L_:
                dist_cols.append(str(l))
            self.loc_distances = pd.DataFrame(drows, columns=dist_cols)
            self.loc_distances = self.loc_distances.set_index('COD_LOCALIDADE')
            self.loc_distances.to_csv('DISTANCIA_ENTRE_LOCALIDADES.csv',sep=';')
            
            # CS18 OK
            loc_times_raw=menu_select(conexao_oracle, "CS18", cod_estudo)[0]                  
            drows = []
            for i in L_:
                drow = [i]
                for j in L_:
                    vlr = loc_times_raw.loc[(loc_times_raw['COD_LOCALIDADE_ORIGEM']==i) &
                                                (loc_times_raw['COD_LOCALIDADE_DESTINO']==j), 
                                                'TEM_CALCULADO'].values
                    if not vlr or math.isnan(vlr):
                        vlr = loc_times_raw.loc[(loc_times_raw['COD_LOCALIDADE_ORIGEM']==j) &
                                                (loc_times_raw['COD_LOCALIDADE_DESTINO']==i), 
                                                'TEM_CALCULADO'].values
                        if not vlr or math.isnan(vlr):
                            drow.append(0.0)
                        else:
                            drow.append(float(vlr))
                    else:
                        drow.append(float(vlr))
                drows.append(drow)
            dist_cols = ['COD_LOCALIDADE']
            for l in L_:
                dist_cols.append(str(l))
            self.loc_times = pd.DataFrame(drows, columns=dist_cols)
            self.loc_times = self.loc_times.set_index('COD_LOCALIDADE')
            self.loc_times.to_csv('TEMPO_ENTRE_LOCALIDADES.csv',sep=';')                        
            
            # CS04 OK
            misc_input_raw=menu_select(conexao_oracle, "CS04", cod_estudo)[0]             
            misc_input_raw=misc_input_raw.rename(columns={'PCT_ATENDIMENTO_OS':'PERCENTUAL_BACKLOG',"TO_CHAR(DTH_INICIO_HORARIO_COMERCIAL,'HH24:MI:SS')":'INICIO_HORA_COMERCIAL',
                "TO_CHAR(DTH_FIM_HORARIO_COMERCIAL,'HH24:MI:SS')":'FIM_HORA_COMERCIAL','IND_MES_BASE':'MES_DEMANDA'})
            self.misc_input = pd.DataFrame(columns=['PARAMETRO', 'VALOR'])
            self.misc_input.loc[0] = ['PERCENTUAL_BACKLOG', misc_input_raw.iloc[0]['PERCENTUAL_BACKLOG']]
            inic_hc = int(misc_input_raw.iloc[0]['INICIO_HORA_COMERCIAL'].split(':')[0])
            fim_hc = int(misc_input_raw.iloc[0]['FIM_HORA_COMERCIAL'].split(':')[0])-1
            print("\nData.py, linha 226, Início hora comercial: ",inic_hc," / fim hora comercial: ",fim_hc)   # Remover - RL
            self.misc_input.loc[2] = ['INICIO_HORA_COMERCIAL', inic_hc]
            self.misc_input.loc[3] = ['FIM_HORA_COMERCIAL', fim_hc]
            self.misc_input.loc[4] = ['MES_DEMANDA', misc_input_raw.iloc[0]['MES_DEMANDA']]
            if misc_input_raw.iloc[0]['IND_TIPO_EXECUCAO'] == 2:
                self.misc_input.loc[5] = ['MONO_OBJ_EXEC', 'S']
                self.misc_input.loc[6] = ['MONO_OBJ_EXEC_BASES_LIVRE', 'S']
                self.misc_input.loc[7] = ['MONO_OBJ_VARIACAO_BASES', 0]
                self.misc_input.loc[8]= ['EXEC_COMPLETA_BASES',1]
            elif misc_input_raw.iloc[0]['IND_TIPO_EXECUCAO'] == 3:
                self.misc_input.loc[5] = ['MONO_OBJ_EXEC', 'S']
                self.misc_input.loc[6] = ['MONO_OBJ_EXEC_BASES_LIVRE', 'N']                
                delta = misc_input_raw.iloc[0]['QTD_BASE_ALOCADA']
                if delta == 0 or math.isnan(delta):
                    delta = -misc_input_raw.iloc[0]['QTD_BASE_DESLIGADA']
                self.misc_input.loc[7] = ['MONO_OBJ_VARIACAO_BASES', delta]
                self.misc_input.loc[8]= ['EXEC_COMPLETA_BASES',1]
            else: # multiobj
                self.misc_input.loc[5] = ['MONO_OBJ_EXEC', 'N']
                self.misc_input.loc[6] = ['MONO_OBJ_EXEC_BASES_LIVRE', 'N']
                self.misc_input.loc[7] = ['MONO_OBJ_VARIACAO_BASES', 0]
                self.misc_input.loc[8]=['EXEC_COMPLETA_BASES',1]
            # self.misc_input.loc[8] = ['CARREGA_PRECALC', misc_input_raw.iloc[0]['XXXXX']]
            self.misc_input = self.misc_input.set_index('PARAMETRO')            
            self.misc_input.to_csv('OUTRAS_ENTRADAS.csv',sep=';')
            
            # CS14 OK
            schedule_raw=menu_select(conexao_oracle, "CS14", cod_estudo)[0]
            schedule_raw=schedule_raw.rename(columns={'ESCALA':'COD_ESCALA','DSC_DIA_SEMANA':'DIA_SEMANA','H0':'0','H1':'1','H2':'2','H3':'3',
                'H4':'4','H5':'5','H6':'6','H7':'7','H8':'8','H9':'9','H10':'10','H11':'11','H12':'12','H13':'13','H14':'14','H15':'15','H16':'16','H17':'17',
                'H18':'18','H19':'19','H20':'20','H21':'21','H22':'22','H23':'23'})
            self.schedule=schedule_raw
            #### Solicitar alteracoes abaixo na base para evitar substituicao
            self.schedule = self.schedule.replace('N', '-')
            self.schedule = self.schedule.replace('S', 'X')
            self.schedule = self.schedule.replace('I', 'P')
            self.schedule = self.schedule.replace('E', 'T')
            self.schedule = self.schedule.replace('SEGUNDA', 'SEG')
            self.schedule = self.schedule.replace('TERÇA', 'TER')
            self.schedule = self.schedule.replace('QUARTA', 'QUA')
            self.schedule = self.schedule.replace('QUINTA', 'QUI')
            self.schedule = self.schedule.replace('SEXTA', 'SEX')
            self.schedule = self.schedule.replace('SABADO', 'SAB')
            self.schedule = self.schedule.replace('DOMINGO', 'DOM')
            self.schedule.to_csv('ESCALAS.csv',sep=';')
            
            # CS31 OK             
            os_hour_raw=menu_select(conexao_oracle, "CS31", cod_estudo)[0]  
            os_hour_raw=os_hour_raw.rename(columns={'COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE',
                                            'ANO_PREVISAO_DEMANDA':'ANO','MES_PREVISAO_DEMANDA':'MES'
                                            ,'DIA_PREVISAO_DEMANDA':'DIA',
                                            'IND_DIA_SEMANA':'DIA_DA_SEMANA',
                                            'HOR_PREVISAO_DEMANDA':'HORA'})
            self.os_hour = os_hour_raw.set_index('COD_LOCALIDADE')
            self.os_hour.to_csv('PREVISAO_DEMANDA_POR_HORA.csv',sep=';')
                     
            # CS08 OK
            vehicles_raw=menu_select(conexao_oracle, "CS08", cod_estudo)[0]
            vehicles_raw=vehicles_raw.rename(columns={'VLR_CUSTO_KM':'CUSTO_KM'})
            self.vehicles=vehicles_raw[['COD_PERFIL_VEICULO','DSC_TIPO_VEICULO']]
            self.vehicles = self.vehicles.set_index('COD_PERFIL_VEICULO')
            self.vehicles.to_csv('PERFIL_VEICULO.csv',sep=';', encoding='latin-1')
            self.veicle_km_costs = vehicles_raw[['COD_PERFIL_VEICULO','CUSTO_KM']]
            self.veicle_km_costs = self.veicle_km_costs.set_index('COD_PERFIL_VEICULO')
            self.veicle_km_costs.to_csv('CUSTO_KM.csv',sep=';')            
            
            # CS35 Ok
            depot_raw=menu_select(conexao_oracle, "CS35", cod_estudo)[0]
            self.depot=depot_raw.rename(columns={'NUM_CENARIO':'ID_SOLUCAO','IDT_PERFIL_BASE_OPERACIONAL':'COD_PERFIL_BASE'})
            self.depot = self.depot[['COD_LOCALIDADE_CLIENTE', 'COD_LOCALIDADE_BASE']]
            self.depot.to_csv('ATENDIMENTO_BASE_LOCALIDADE.csv',sep=';')
            
            # CS05 OK
            clients_raw=menu_select(conexao_oracle, "CS05", cod_estudo)[0]
            clients_raw=clients_raw.rename(columns={"NUM_COORDENADA_LATITUDE":"COORD_X",'NUM_COORDENADA_LONGITUDE':'COORD_Y','COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE',
                   'COD_GRUPO_FORNECIMENTO':'GRUPO_FORNECIMENTO','QTD_DMIC':'LIMITE_DMIC'})            
            self.clients=clients_raw
            self.clients.to_csv('CLIENTES.csv',sep=';')
            #self.clients = pd.read_csv('CLIENTES.csv', sep=";", encoding='latin-1')
            
            # CS30 OK
            os_emergency_raw=menu_select(conexao_oracle, "CS30", cod_estudo)[0]
            self.os_emergency=os_emergency_raw.rename(columns={'NUM_OS_VIRTUAL_PREVISAO':'NUM_OS','COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE','IDT_BLOCO_SERVICO':'COD_SERVICO','DSC_CONJUNTO_UC_PREVISAO':'COD_UC'})
            self.os_emergency.to_csv('PREVISAO_DEMANDA_EMERGENCIAL.csv',sep=';')
            
            #CS09 OK
            hotel_cost_raw=menu_select(conexao_oracle, "CS09", cod_estudo)[0]
            hotel_cost_raw.to_csv('HOTEL.csv',sep=';')
            diaria = 0.0
            for index, row in hotel_cost_raw.iterrows():
                if hotel_cost_raw.loc[index, 'DSC_TIPO_CUSTO_DIVERSO'] == 'HOSPEDAGEM':
                    diaria = hotel_cost_raw.loc[index, 'VLR_CUSTO_DIVERSO']
                    break            
            self.hotel_cost=diaria

            #CS06 OK 
            bases_costs=menu_select(conexao_oracle, "CS06", cod_estudo)[0]
            bases_costs=bases_costs.rename(columns={'IDT_PERFIL_BASE_OPERACIONAL':'COD_PERFIL_BASE','VLR_CUSTO_MANUTENCAO':'CUSTO_MANUTENCAO','VLR_CUSTO_INSTALACAO':'CUSTO_INSTALACAO','VLR_CUSTO_FECHAMENTO':'CUSTO_FECHAMENTO'})
            self.bases_costs=bases_costs.set_index('COD_PERFIL_BASE')
            self.bases_costs = self.bases_costs.drop(columns=['IND_TIPO_PERFIL_BASE_OPERL'])
            self.bases_costs.to_csv('CUSTO_BASE.csv',sep=';')
            
        elif self.modulo =='tatic' or self.modulo == 'operational':
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Lendo dados para simulacao' where IDT_CONTROLE_PRCSM_ETUDO=" +str(cod_estudo)
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            '''self.os_volume = pd.read_csv('input/PREVISAO_DEMANDA_VOLUME.csv', sep=";", encoding='latin-1')
            self.services = pd.read_csv('input/SERVICOS.csv', sep=";", index_col='COD_SERVICO', encoding='latin-1')
            self.locs_bases = pd.read_csv('input/LOCALIDADES.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            self.electrician_profiles = pd.read_csv('input/PERFIL_ELETRICISTA.csv', sep=";")
            self.crew_profiles = pd.read_csv('input/PERFIL_EQUIPE.csv', sep=";", index_col='COD_PERFIL_EQUIPE', encoding='latin-1')
            self.crew_formation = pd.read_csv('input/EQUIPE_FORMACAO.csv', sep=";", encoding='latin-1')
            self.crew_service = pd.read_csv('input/EQUIPE_SERVICO.csv', sep=";", encoding='latin-1')
            self.electrician_per_loc = pd.read_csv('input/ELETRICISTAS_POR_LOCALIDADE.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            self.loc_distances = pd.read_csv('input/DISTANCIA_ENTRE_LOCALIDADES.csv', sep=";",  index_col='COD_LOCALIDADE')
            self.loc_times = pd.read_csv('input/TEMPO_ENTRE_LOCALIDADES.csv', sep=";", index_col='COD_LOCALIDADE')
            self.electricians_costs = pd.read_csv('input/CUSTO_ELETRICISTA.csv', sep=";", index_col='COD_PERFIL_ELETRICISTA', encoding='latin-1')
            self.os_hour = pd.read_csv('input/PREVISAO_DEMANDA_POR_HORA.csv', sep=";", index_col='COD_LOCALIDADE', encoding='latin-1')
            self.schedule = pd.read_csv('input/ESCALAS.csv', sep=";", encoding='latin-1')
            self.vehicles = pd.read_csv('input/PERFIL_VEICULO.csv', sep=";", index_col='COD_PERFIL_VEICULO', encoding='latin-1')
            self.misc_input = pd.read_csv('input/OUTRAS_ENTRADAS.csv', sep=";", encoding='latin-1', index_col='PARAMETRO')
            self.bases_profiles = pd.read_csv('PERFIL_BASE.csv', sep=";", encoding='latin-1')
            self.depot = pd.read_csv('ATENDIMENTO_BASE_LOCALIDADE.csv', sep=';')'''
            
            #CS02 OK
            services_raw = menu_select(conexao_oracle,'CS02',cod_estudo) # obtem tabela bruta do banco de dados
            services_raw = services_raw[0].drop('COD_SERVICO', 1)            
            services_raw = services_raw.drop('COD_PERFIL_EQUIPE', 1)
            services_raw = services_raw.drop_duplicates()        
            services_raw = services_raw.rename(columns={"IDT_BLOCO_SERVICO": "COD_SERVICO", "IDT_DEPARTAMENTO_EQUIPE": "DEPARTAMENTO", "IND_TIPO_OCORRENCIA" : "TIPO_OCORRENCIA",
                                        "IND_TMD_EXCECAO" : "TMD_INTERNO_EXCECAO", "VLR_TME_BLOCO_SERVICO" : "TME", "VLR_TMP_BLOCO_SERVICO" : "TMP" })
            services_raw = services_raw.set_index('COD_SERVICO')
            self.services = services_raw
            #print("\nData.py, linha 89, self.services TME antes da conversão: \n",self.services['TME'])   # Remover - RL
            #print("\nself.services TMP antes da conversão: \n",self.services['TMP'])   # Remover - RL
            self.services['TME'] = self.services['TME']/60     
            self.services['TMP'] = self.services['TMP']/60
            #print("\nData.py, linha 93, self.services TME após conversão: \n",self.services['TME'])   # Remover - RL
            #print("\nself.services TMP após conversão: \n",self.services['TMP'])   # Remover - RL
            self.services.to_csv('SERVICOS_TTC.csv',sep=';')
            
            #CS25 OK
            locs_bases=menu_select(conexao_oracle, "CS25", cod_estudo)[0]            
            locs_bases=locs_bases.rename(columns={'VLR_TMD_INTERNO':'TEMPO_MEDIO_ENTRE_OS','VLR_TMD_EXCECAO':'TEMPO_MEDIO_ENTRE_OS_EXCESSAO','IND_BASE_FIXA':'BASE_FIXA'})        
            self.locs_bases=locs_bases.set_index('COD_LOCALIDADE')
            self.locs_bases.to_csv('LOCALIDADES_TTC.csv',sep=';')
            
            #CS32 OK 
            os_volume_raw=menu_select(conexao_oracle, "CS32", cod_estudo)[0]
            os_volume_cols = ['COD_LOCALIDADE','ANO','MES']
            for s in self.services.index.values:
                os_volume_cols.append('SERVICO '+str(s))
            self.os_volume = pd.DataFrame(columns=os_volume_cols)
            self.os_volume = self.os_volume.set_index(['COD_LOCALIDADE', 'ANO', 'MES'])
            for index, row in os_volume_raw.iterrows():
                loc, year = row['COD_LOCALIDADE_VIRTUAL'], row['ANO_PREVISAO_DEMANDA']
                month, serv = row['MES_PREVISAO_DEMANDA'], row['IDT_BLOCO_SERVICO']
                vol = row['QTD_OS_DEMANDA']
                if self.os_volume.index.isin([(loc, year, month)]).any():
                    self.os_volume.loc[(loc, year, month), 'SERVICO '+str(serv)] = vol
                else:
                    new_row =  [0 for s in self.services.index.values]
                    for (i,s) in enumerate(self.services.index.values):
                        if s == serv:
                            new_row[i] = vol
                    self.os_volume.loc[(loc, year, month)] = new_row
            self.os_volume = self.os_volume.reset_index()
            self.os_volume.to_csv('PREVISAO_DEMANDA_VOLUME_TTC.csv',sep=';')
                       
            # CS20 OK 
            electrician_profiles_raw= menu_select(conexao_oracle, "CS20", cod_estudo)[0]   
            electrician_profiles_raw=electrician_profiles_raw.rename(columns={'DSC_PERFIL':'DESC_PERFIL_ELETRICISTA','VLR_CUSTO_CONTRATACAO':'CUSTO_CONTRATACAO','VLR_CUSTO_DEMISSAO':'CUSTO_DEMISSAO','VLR_CUSTO_TRANSFERENCIA':'CUSTO_TRANSFERENCIA',
                'VLR_CUSTO_MANUTENCAO':'CUSTO_MANUTENCAO','VLR_CUSTO_HH':'CUSTO_HH','VLR_CUSTO_HH_NOTURNO':'CUSTO_HH_NOTURNO','VLR_CUSTO_HH_FIM_SEMANA':'CUSTO_HH_FDS','VLR_CUSTO_HH_FIM_SEMANA_NOTUR':'CUSTO_HH_FDS_NOTURNO'})
            self.electrician_profiles=electrician_profiles_raw[['COD_PERFIL_ELETRICISTA', 'DESC_PERFIL_ELETRICISTA']]
            self.electrician_profiles.to_csv('PERFIL_ELETRICISTA_TTC.csv',sep=';')
            self.electricians_costs=electrician_profiles_raw[['COD_PERFIL_ELETRICISTA','CUSTO_CONTRATACAO','CUSTO_DEMISSAO',
                'CUSTO_TRANSFERENCIA', 'CUSTO_MANUTENCAO','CUSTO_HH','CUSTO_HH_NOTURNO','CUSTO_HH_FDS','CUSTO_HH_FDS_NOTURNO']]
            self.electricians_costs = self.electricians_costs.set_index('COD_PERFIL_ELETRICISTA')
            self.electricians_costs.to_csv('CUSTO_ELETRICISTA_TTC.csv',sep=';')
            
            # CS27 OK
            crew_profiles=menu_select(conexao_oracle, "CS27", cod_estudo)[0]        
            crew_profiles=crew_profiles.rename(columns={'QTD_HH_DIARIO':'HH_TOTAL','VLR_MIP':'MIP','QTD_DIA_TRABALHADO_ANO':'QTD_DIAS_TRABALHO_ANO'})        
            self.crew_profiles=crew_profiles
            self.crew_profiles = self.crew_profiles.set_index('COD_PERFIL_EQUIPE')
            self.crew_profiles.to_csv('PERFIL_EQUIPE_TTC.csv',sep=';')
            
            # CS23 OK
            crew_formation=menu_select(conexao_oracle,  "CS23", cod_estudo)[0]
            #crew_formation=crew_formation.rename(columns={'SUM(PPEBE.QTD_ELETRICISTA)':'QTD_ELETRICISTAS'})
            crew_formation=crew_formation.rename(columns={'QTD_ELETRICISTA':'QTD_ELETRICISTAS'})
            self.crew_formation=crew_formation
            self.crew_formation.to_csv('EQUIPE_FORMACAO_TTC.csv',sep=';')
            
            # CS24 OK
            crew_service_raw=menu_select(conexao_oracle, "CS24", cod_estudo)[0]
            self.crew_service =crew_service_raw.rename(columns={'IDT_BLOCO_SERVICO':'COD_SERVICO'})
            self.crew_service.to_csv('EQUIPE_SERVIÇO_TTC.csv',sep=';')#REN
            
            # CS22  OK
            electrician_per_loc=menu_select(conexao_oracle, "CS22", cod_estudo)[0]
            electrician_per_loc=electrician_per_loc.rename(columns={'COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE','SUM(PPEBE.QTD_ELETRICISTA)':'QTD_ELETRICISTAS'})
            self.electrician_per_loc=electrician_per_loc.set_index('COD_LOCALIDADE')
            self.electrician_per_loc.to_csv('ELETRICISTAS_POR_LOCALIDADE_TTC.csv',sep=';')
            
            # CS21 OK 
            loc_distances_raw=menu_select(conexao_oracle, "CS21", cod_estudo)[0]
            L_ = sorted(self.locs_bases.index.values)
            drows = []
            for i in L_:
                drow = [i]
                for j in L_:
                    vlr = loc_distances_raw.loc[(loc_distances_raw['COD_LOCALIDADE_ORIGEM']==i) &
                                                (loc_distances_raw['COD_LOCALIDADE_DESTINO']==j), 
                                                'VLR_DISTANCIA'].values
                    if not vlr or math.isnan(vlr):
                        vlr = loc_distances_raw.loc[(loc_distances_raw['COD_LOCALIDADE_ORIGEM']==j) &
                                                (loc_distances_raw['COD_LOCALIDADE_DESTINO']==i), 
                                                'VLR_DISTANCIA'].values
                        if not vlr or math.isnan(vlr):
                            drow.append(0.0)
                        else:
                            drow.append(float(vlr))
                    else:
                        drow.append(float(vlr))
                drows.append(drow)
            dist_cols = ['COD_LOCALIDADE']
            for l in L_:
                dist_cols.append(str(l))
            self.loc_distances = pd.DataFrame(drows, columns=dist_cols)
            self.loc_distances = self.loc_distances.set_index('COD_LOCALIDADE')
            self.loc_distances.to_csv('DISTANCIA_ENTRE_LOCALIDADES_TTC.csv',sep=';')          
            
            # CS29 OK
            loc_times_raw=menu_select(conexao_oracle, "CS29", cod_estudo)[0]
            drows = []
            for i in L_:
                drow = [i]
                for j in L_:
                    vlr = loc_times_raw.loc[(loc_times_raw['COD_LOCALIDADE_ORIGEM']==i) &
                                                (loc_times_raw['COD_LOCALIDADE_DESTINO']==j), 
                                                'TEM_CALCULADO'].values
                    if not vlr or math.isnan(vlr):
                        vlr = loc_times_raw.loc[(loc_times_raw['COD_LOCALIDADE_ORIGEM']==j) &
                                                (loc_times_raw['COD_LOCALIDADE_DESTINO']==i), 
                                                'TEM_CALCULADO'].values
                        if not vlr or math.isnan(vlr):
                            drow.append(0.0)
                        else:
                            drow.append(float(vlr))
                    else:
                        drow.append(float(vlr))
                drows.append(drow)
            dist_cols = ['COD_LOCALIDADE']
            for l in L_:
                dist_cols.append(str(l))
            self.loc_times = pd.DataFrame(drows, columns=dist_cols)
            self.loc_times = self.loc_times.set_index('COD_LOCALIDADE')
            self.loc_times.to_csv('TEMPO_ENTRE_LOCALIDADES_TTC.csv',sep=';')
            
            # CS31 OK 
            os_hour_raw=menu_select(conexao_oracle, "CS31", cod_estudo)[0]
            os_hour_raw=os_hour_raw.rename(columns={'COD_LOCALIDADE_VIRTUAL':'COD_LOCALIDADE',
                                            'ANO_PREVISAO_DEMANDA':'ANO','MES_PREVISAO_DEMANDA':'MES'
                                            ,'DIA_PREVISAO_DEMANDA':'DIA',
                                            'IND_DIA_SEMANA':'DIA_DA_SEMANA',
                                            'HOR_PREVISAO_DEMANDA':'HORA'})
            self.os_hour = os_hour_raw.set_index('COD_LOCALIDADE')
            self.os_hour.to_csv('PREVISAO_DEMANDA_POR_HORA_TTC.csv',sep=';')
            
            # CS19 OK
            schedule_raw=menu_select(conexao_oracle, "CS19", cod_estudo)[0]
            schedule_raw=schedule_raw.rename(columns={'ESCALA':'COD_ESCALA','DSC_DIA_SEMANA':'DIA_SEMANA','H0':'0','H1':'1','H2':'2','H3':'3',
                'H4':'4','H5':'5','H6':'6','H7':'7','H8':'8','H9':'9','H10':'10','H11':'11','H12':'12','H13':'13','H14':'14','H15':'15','H16':'16','H17':'17',
                'H18':'18','H19':'19','H20':'20','H21':'21','H22':'22','H23':'23'})
            self.schedule=schedule_raw
            #### Solicitar alteracoes abaixo na base para evitar substituicao
            self.schedule = self.schedule.replace('N', '-')
            self.schedule = self.schedule.replace('S', 'X')
            self.schedule = self.schedule.replace('I', 'P')
            self.schedule = self.schedule.replace('E', 'T')
            self.schedule = self.schedule.replace('SEGUNDA', 'SEG')
            self.schedule = self.schedule.replace('TERÇA', 'TER')
            self.schedule = self.schedule.replace('QUARTA', 'QUA')
            self.schedule = self.schedule.replace('QUINTA', 'QUI')
            self.schedule = self.schedule.replace('SEXTA', 'SEX')
            self.schedule = self.schedule.replace('SABADO', 'SAB')
            self.schedule = self.schedule.replace('DOMINGO', 'DOM')
            self.schedule.to_csv('ESCALAS_TTC.csv',sep=';')           
            
            # CS28 OK
            vehicles_raw=menu_select(conexao_oracle, "CS28", cod_estudo)[0]
            vehicles_raw=vehicles_raw.rename(columns={'VLR_CUSTO_KM':'CUSTO_KM'})
            self.vehicles=vehicles_raw[['COD_PERFIL_VEICULO','DSC_TIPO_VEICULO']]
            self.vehicles = self.vehicles.set_index('COD_PERFIL_VEICULO')
            self.vehicles.to_csv('PERFIL_VEICULO_TTC.csv',sep=';') 
            
            # CS26 OK
            misc_input_raw=menu_select(conexao_oracle, "CS26", cod_estudo)[0]                     
            misc_input_raw=misc_input_raw.rename(columns={'PCT_ATENDIMENTO_OS':'PERCENTUAL_BACKLOG',"TO_CHAR(DTH_INICIO_HORARIO_COMERCIAL,'HH24:MI:SS')":'INICIO_HORA_COMERCIAL',
                "TO_CHAR(DTH_FIM_HORARIO_COMERCIAL,'HH24:MI:SS')":'FIM_HORA_COMERCIAL','IND_MES_BASE':'MES_DEMANDA'})
            self.misc_input = pd.DataFrame(columns=['PARAMETRO', 'VALOR'])
            self.misc_input.loc[0] = ['PERCENTUAL_BACKLOG', misc_input_raw.iloc[0]['PERCENTUAL_BACKLOG']]
            inic_hc = int(misc_input_raw.iloc[0]['INICIO_HORA_COMERCIAL'].split(':')[0])
            fim_hc = int(misc_input_raw.iloc[0]['FIM_HORA_COMERCIAL'].split(':')[0])-1 #.split(':')[0]
            self.misc_input.loc[2] = ['INICIO_HORA_COMERCIAL', inic_hc]
            self.misc_input.loc[3] = ['FIM_HORA_COMERCIAL', fim_hc]
            self.misc_input.loc[4] = ['MES_DEMANDA', misc_input_raw.iloc[0]['MES_DEMANDA']]
            #self.misc_input.loc[5] = ['EXEC_COMPLETA_BASES', misc_input_raw.iloc[0]['EXEC_COMPLETA_BASES']]
            #self.misc_input.loc[8] = ['CARREGA_PRECALC', misc_input_raw.iloc[0]['XXXXX']]
            self.misc_input = self.misc_input.set_index('PARAMETRO')
            self.misc_input.to_csv('OUTRAS_ENTRADAS_TTC.csv',sep=';')
            
            # CS35 Ok
            depot_raw=menu_select(conexao_oracle, "CS35", cod_estudo)[0]
            self.depot=depot_raw.rename(columns={'NUM_CENARIO':'ID_SOLUCAO','IDT_PERFIL_BASE_OPERACIONAL':'COD_PERFIL_BASE'})
            self.depot = self.depot[['COD_LOCALIDADE_CLIENTE', 'COD_LOCALIDADE_BASE']]
            self.depot.to_csv('ATENDIMENTO_BASE_LOCALIDADE_TTC.csv',sep=';')
            
            #CS36 OK  
            bases_profiles=menu_select(conexao_oracle, "CS36", cod_estudo)[0]
            self.bases_profiles=bases_profiles
            self.bases_profiles.to_csv('PERFIL_BASE_TTC.csv',sep=';')
            
        conexao_oracle.con.close() # fecha conexao com banco de dados                
