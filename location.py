from pulp import *
import precalc
import copy
import numpy as np
from geopy import distance
import random
import pandas as pd
from scheduling import *
from classes_folder.menu import menu_insert
import datetime
from classes_folder.conexao import AbrirConexao
import os
import sys
from data import Data

class Location:

    def __init__(self, data,cod_modulo,cod_estudo,cod_empresa):
        self.data = data 
        self.runtime = 0.0
        self.general_results=None
        self.saida_atendimento=None
        self.bases_configuration=None
        self.electricians_data=None
        self.electricians_locality=None
        self.cod_modulo=cod_modulo
        self.cod_estudo=cod_estudo
        self.cod_empresa=cod_empresa

    def run(self):
        
        # Calculo de variaveis comuns a ambos os módulos
        S = self.data.services.index.values # codigos de servico OK        
        E = self.data.crew_profiles.index.values # indices de perfis de equipe OK        
        P = self.data.electrician_profiles['COD_PERFIL_ELETRICISTA'].unique() # indices de perfis de eletricista OK        
        L = self.data.locs_bases.index.values # indices de localidades OK        
        l = get_qtd_elet_eq(self.data.crew_formation) # qtd elet. na equipe, acesso l[p,e] ou l.get((p,e),0) que retorna 0 se n tiver OK        
        T = {cod_proc: row['CUSTO_CONTRATACAO'] for cod_proc, row in self.data.electricians_costs.iterrows()} # custo contratação (treinamento) OK        
        D = {cod_proc: row['CUSTO_DEMISSAO'] for cod_proc, row in self.data.electricians_costs.iterrows()} # custo demissão  OK        
        B = self.data.bases_profiles['COD_PERFIL_BASE'].unique() # indices de perfis de base OK        
        q = get_loc_volume(self.data.os_volume) # volume de um serviço por localidade (q[s][i]) OK
        print("\nVolume de serviço por localidade q[s][i]: ",q)   # Remover - RL
        
        tmd = {i: {j: self.data.loc_times.at[i, str(j)] for j in L} for i in L} # tempo entre localidades        
        tme = {s: row['TME'] for s, row in self.data.services.iterrows()} # tempo de execução OK        
        tmp = {s: row['TMP'] for s, row in self.data.services.iterrows()} # tempo de preparação OK        
        
        # Calculando algumas outras constantes
        c = get_km_cost(self.data.veicle_km_costs, self.data.services)  # acesso c[s], s eh codigo servico OK        
        f = get_custos_bases(self.data.bases_costs)  # acesso f[cod_perf_base][i] i=0,1,2 (manut.,instal.,fechamento) OK        
        a = get_a(self.data.locs_bases, self.data.bases_profiles, L) # a[j][b] indica se existe base perfil b na localidade j OK   
        
        d = {i: {j: self.data.loc_distances.at[i, str(j)] for j in L} for i in L} # distancia entre localidades         
        r = get_qtd_eletricistas(self.data.electrician_per_loc, L) # acesso r[j].get(p, 0), local. j e perfil p OK
        fixed_bases=list(self.data.locs_bases[self.data.locs_bases['BASE_FIXA']=='S'].index) #bases que n podem ser desinstaladas

        print("\nPrecalculando constantes para execução do módulo estratégico...", end=" ", flush=True)

        #try:
        #    self.data.misc_input.loc['CARREGA_PRECALC','VALOR'] == 'S'
        #    print("\nExecutando location.py try linha 59:")   # Remover - RL
        #    path_precalc = 'precalc/PRECALCULO_'+str(hash((int(self.data.cod_modulo),int(self.data.cod_estudo))))+'.csv'
        #    print('\nCarregando constantes já calculadas em '+path_precalc+'...', end=' ', flush=True)
        #    if os.path.exists(path_precalc):
        #        m, h, g, beta, gama, H = precalc.load(path_precalc, E, L, B, self.data.services, strategic=True) # carrega valores já existentes (muito mais rápido!)
        #    else:
        #        print('\nCaminho de precalculo '+path_precalc+' não existe! Calculando do zero...')
        #        m, h, g, beta, gama, H = precalc.strategic_consts(self.data, q, L, tmd, tmp, tme, self.data.hotel_cost, E, P, l)
        #except: 
        try:
            print("\nExecutando location.py except linha 68:")   # Remover - RL
            m, h, g, beta, gama, H = precalc.strategic_consts(self.data, q, L, tmd, tmp, tme, self.data.hotel_cost, E, P, l)
        except Exception as erro:
            print(erro)
        print("\nLocation.py, linha 72")#, dicionário H: ", H)   # Remover - RL
        Lat_min, Lat_Max, Long_Min, Long_Max=get_lat_long(self.data.locs_bases)        
        """Verifica o tipo de solução escolhido pelo usuário. 0 = Mono com ou sem variação de bases 
        1 = multi-objetivo"""
        #soluc_type = 0;   # Novo - RL
        
        if self.data.misc_input.loc['MONO_OBJ_EXEC','VALOR'] == 'N': #EXEC MULTI
            #soluc_type = 1;   # Novo - RL
            bases_positioning = LocationModel(L=L, B=B, S=S, d=d, c=c, q=q, beta=beta, m=m, f=f, a=a, g=g, P=P, T=T,
                                              D=D, E=E, l=l, r=r, h=h, hotel_link=H, fixed_bases=[])                                             
            try:
                nb = self.data.locs_bases[self.data.locs_bases['POSSUI_BASE'] == 'S'].shape[0]  # num. bases existentes                
                nmin = int(nb / 2)
                nmax = int(1.5 * nb) + 1
                print("\nNúmero de bases existentes: ",nb,"\nnmin e nmax: ",nmin," e ",nmax)   # Remover - RL
                if nmin == 0:  # nmin n pode ser 0 bases
                    nmin = 1
                    nmax = 2
                solutions_cod = []  # lista p salvar solucoes
                for n in range(nmin,nmax):                    
                    custo_total, bases = bases_positioning.solve(forced_nb_bases=n)  # instala n bases
                    solutions_cod.append(bases)
                AE = ATUALIZA_ELETRICISTAS(P=P, r=r, f=f, d=d, T=T, D=D, h=h, m=m, g=g, S=S, c=c, beta=beta, q=q, H=H,
                                           B=B, data=self.data, cod_modulo=self.cod_modulo, cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa)
                input_payback, output_interface = AE.electrician_sizing(solutions_cod, B[0])
                payback = CALCULA_PAYBACK(P=P, r=r, f=f, d=d, T=T, D=D, h=h, m=m, g=g, S=S, c=c, beta=beta, q=q, H=H,
                                          B=B, data=self.data, cod_modulo=self.cod_modulo, cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa)
                payback.get_payback(input_payback, output_interface)
                df = get_dataframe(status=1, cod_estudo=self.cod_estudo, modulo='strategic', id_situacao=2)
                conexao_oracle=AbrirConexao(self.cod_empresa)
                menu_insert(conexao_oracle,df,'at14')
                conexao_oracle.con.close()
            except Exception as erro:
                print("location.py, linha 108, Multi Objetivo erro: ",erro)   # Tratamento de erro - RL
                df=get_dataframe(status=1,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3)
                conexao_oracle=AbrirConexao(self.cod_empresa)
                menu_insert(conexao_oracle,df,'at14')
                conexao_oracle.con.close()
                return 0
        else: 
            bases_positioning = LocationModel(L=L, B=B, S=S, d=d, c=c, q=q, beta=beta, m=m, f=f, a=a, g=g, P=P, T=T, D=D, E=E, l=l, r=r, h=h, hotel_link=H, fixed_bases=fixed_bases)
            if self.data.misc_input.loc['MONO_OBJ_EXEC_BASES_LIVRE','VALOR'] == 'S': #EXEC MONO BASES LIVRES
                try:                    
                    result= bases_positioning.solve()
                    if result[0]!=-1: #-1= caso infeasible
                        custos,bases=result[0],result[1]
                        if bases: # se bases nao voltar vazio                               
                            AE=ATUALIZA_ELETRICISTAS(P=P,r=r,f=f,d=d,T=T,D=D,h=h,m=m,g=g,S=S,c=c,beta=beta,q=q,H=H,B=B,data=self.data,cod_modulo=self.cod_modulo,cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa)                    
                            input_payback, output_interface=AE.electrician_sizing([bases],B[0])
                            if len(input_payback)>0: #se payback não voltar vazio                                
                                payback=CALCULA_PAYBACK(P=P,r=r,f=f,d=d,T=T,D=D,h=h,m=m,g=g,S=S,c=c,beta=beta,q=q,H=H,B=B,data=self.data,cod_modulo=self.cod_modulo,cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa,payback=True)                                                                
                                payback.get_payback(input_payback,output_interface)                                
                                df=get_dataframe(status=1,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=2)                                
                                conexao_oracle=AbrirConexao(self.cod_empresa)
                                menu_insert(conexao_oracle,df,'at14')
                                conexao_oracle.con.close()
                            else:
                                msg='Erro no dimensionamento de eletricistas (módulo estratégico)'
                                conexao_oracle=AbrirConexao(self.cod_empresa)
                                df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                                menu_insert(conexao_oracle,df,'at14')
                                conexao_oracle.con.close()                                
                                return 0
                        else:
                            msg='Simulação de bases não retornou nenhuma base. Verificar coerência/ausência de algum dado de entrada. Certifique-se de que todos os parâmetros e dados estejam preenchidos corretamente'
                            conexao_oracle=AbrirConexao(self.cod_empresa)
                            df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                            menu_insert(conexao_oracle,df,'at14')
                            conexao_oracle.con.close()
                            return 0
                    else:
                        msg='Não foi possível resolver o módulo estratégico. Verificar se a simulação desejada está coerente'
                        conexao_oracle=AbrirConexao(self.cod_empresa)
                        df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                        menu_insert(conexao_oracle,df,'at14')
                        conexao_oracle.con.close()  
                        return 0
                except Exception as erro:
                    print("location.py, linha 153, Mono Objetivo Bases livres erro: ",erro)   # Tratamento de erro - RL
                    df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3)
                    conexao_oracle=AbrirConexao(self.cod_empresa)
                    menu_insert(conexao_oracle,df,'at14')
                    conexao_oracle.con.close()
                    return 0
            else: #EXEC MONO INSTALA/DESINSTALA BASES
                try:
                    delta = int(self.data.misc_input.loc['MONO_OBJ_VARIACAO_BASES','VALOR'])
                    result = bases_positioning.solve(delta_bases=delta) # instala novas bases                    
                    if result[0]!=-1: #-1= caso infeasible
                        custos,bases=result[0],result[1]
                        if bases:
                            custo_total, bases = result[0],result[1]                            
                            AE=ATUALIZA_ELETRICISTAS(P=P,r=r,f=f,d=d,T=T,D=D,h=h,m=m,g=g,S=S,c=c,beta=beta,q=q,H=H,B=B,data=self.data,cod_modulo=self.cod_modulo,cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa)                            
                            input_payback, output_interface=AE.electrician_sizing([bases],B[0])                            
                            if len(input_payback)>0:
                                payback=CALCULA_PAYBACK(P=P,r=r,f=f,d=d,T=T,D=D,h=h,m=m,g=g,S=S,c=c,beta=beta,q=q,H=H,B=B,data=self.data,cod_modulo=self.cod_modulo,cod_estudo=self.cod_estudo,cod_empresa=self.cod_empresa,payback=True)                               
                                payback.get_payback(input_payback,output_interface)
                                df=get_dataframe(status=1,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=2)                                
                                conexao_oracle=AbrirConexao(self.cod_empresa)
                                menu_insert(conexao_oracle,df,'at14')
                                conexao_oracle.con.close()
                            else:
                                msg='Erro no dimensionamento de eletricistas (módulo tático)'
                                conexao_oracle=AbrirConexao(self.cod_empresa)
                                df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                                menu_insert(conexao_oracle,df,'at14')
                                conexao_oracle.con.close()  
                                return 0                                            
                        else:
                            msg='Simulação de bases não retornou nenhuma base. Verificar coerência/ausência de algum dado de entrada'
                            conexao_oracle=AbrirConexao(self.cod_empresa)
                            df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                            menu_insert(conexao_oracle,df,'at14')
                            conexao_oracle.con.close()                            
                            return 0
                    else:
                        msg='Não foi possível resolver o módulo estratégico. Verificar se a simulação desejada está coerente'
                        conexao_oracle=AbrirConexao(self.cod_empresa)
                        df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                        menu_insert(conexao_oracle,df,'at14')
                        conexao_oracle.con.close()  
                        return 0                        
                except Exception as erro:
                    print("location.py, linha 198, Mono Objetivo Instala/Desinstala bases erro: ",erro)   # Tratamento de erro - RL
                    msg=str(sys.exc_info()[1])
                    df=get_dataframe(status=2,cod_estudo=self.cod_estudo,modulo='strategic',id_situacao=3,msg=msg)
                    conexao_oracle=AbrirConexao(self.cod_empresa)
                    menu_insert(conexao_oracle,df,'at14')
                    conexao_oracle.con.close()
                    return 0

        self.general_results = get_general_results(output_interface,self.cod_modulo)
        self.saida_atendimento=get_saida_atendimento(self.data.locs_bases,self.data.loc_times,output_interface,B,self.cod_modulo)
        self.bases_configuration=get_bases_configuration(output_interface, B,self.cod_modulo)
        self.electricians_data=get_electricians_out(output_interface,self.cod_modulo)
        self.electricians_locality=get_electricians_locality(output_interface,r,L,self.cod_modulo)

        return 1

class mopso:

    def __init__(self, Lat_min, Lat_Max, Long_Min, Long_Max,B,data,NumIteracoes=250, Numparticulas=50):
        self.NumIteracoes = NumIteracoes
        self.Numparticulas = Numparticulas
        self.Lat_min = Lat_min
        self.Lat_Max = Lat_Max
        self.Long_Min = Long_Min
        self.Long_Max = Long_Max
        self.latlong_localities = {i: np.array([data.locs_bases.loc[i]['COORD_X'], data.locs_bases.loc[i]['COORD_Y']]) for i in data.locs_bases.index}
        self.existing_bases=np.array(data.locs_bases [data.locs_bases ['POSSUI_BASE']=='S'][['COORD_X','COORD_Y','COD_PERFIL_BASE']])
        self.existing_bases_cod=list(data.locs_bases[data.locs_bases ['POSSUI_BASE']=='S'].index)
        self.B=B
        self.cod_prohibited=list(data.locs_bases[data.locs_bases['BASE_FIXA']=='S'].index) # bases q não podem ser fechadas

    def MOPSO(self,func_obj):
        FO = func_obj  # FUNCAO OBJETIVO
        BASES_EXISTENTES=self.existing_bases
        COD_BASES_EXISTENTES=self.existing_bases_cod
        LOCALIDADES=self.latlong_localities
        nBases = len(LOCALIDADES)  # numero de localidades para instalacao de bases
        nBases_existentes = len(BASES_EXISTENTES)  # número de bases existentes
        prohibited=self.cod_prohibited

        # PARAMETROS MOPSO
        MinX = self.Lat_min
        MaxX = self.Lat_Max
        MinY = self.Long_Min
        MaxY = self.Long_Max

        nVar = 2 * nBases  #Num variaveis
        MaxIt = self.NumIteracoes  # Número de iterações
        nPop = self.Numparticulas  # Número da população
        c1, c2 = 1.497, 1.497  # constante de aceleração individual e social

        empty_particle = Particle()  # cria instancia particula
        pop = []  # lista para salvar particulas  
        Custos_aux=[]
        Pos=[]
        Tipo_base=[]
        # Inicialização das partículas
        for i in range(nPop):

            pop.append(copy.deepcopy(empty_particle))
            pop[i].position = np.zeros(nVar)
            pop[i].base_profile=np.zeros(nVar)
            if i == 0:  # primeira partícula pega apenas bases existentes
                for k in range(nBases):
                    if (k < nBases_existentes):
                        pop[i].position[k] = BASES_EXISTENTES[k][0]
                        pop[i].position[k + nBases] = BASES_EXISTENTES[k][1]
                        pop[i].base_profile[k]=BASES_EXISTENTES[k][2] #perfil da base
                        pop[i].base_profile[k+nBases]=BASES_EXISTENTES[k][2] #perfil da base
                    else:
                        pop[i].position[k] = BASES_EXISTENTES[0][0]
                        pop[i].position[k + nBases] = BASES_EXISTENTES[0][1]
                        pop[i].base_profile[k]=self.B[0] #variar este parâmetro posteriormente 
                        pop[i].base_profile[k+nBases]=self.B[0] #variar este parâmetro posteriormente 

            elif i == 1:  # segunda particula poe uma base em cada localidade
                k = 0
                for loc in LOCALIDADES:
                    pop[i].position[k] = LOCALIDADES[loc][0]  # LAT DA LOCALIDADE LOC
                    pop[i].position[k + nBases] = LOCALIDADES[loc][1]  # LONG DA LOCALIDADE LOC
                    pop[i].base_profile[k]=self.B[0] #variar este parâmetro posteriormente 
                    pop[i].base_profile[k+nBases]=self.B[0] #variar este parâmetro posteriormente
                    k += 1                
            else:  # Demais partículas recebem solucoes em torno das bases existentes
                for k in range(nBases):
                    if k < nBases_existentes:
                        pop[i].position[k] = BASES_EXISTENTES[k][0]
                        pop[i].position[k + nBases] = BASES_EXISTENTES[k][1]                        
                        pop[i].base_profile[k]=BASES_EXISTENTES[k][2] #perfil da base
                        pop[i].base_profile[k+nBases]=BASES_EXISTENTES[k][2] #perfil da base
                    else:
                        pop[i].position[k] = np.random.uniform(MinX, MaxX)  # sorteia lat
                        pop[i].position[k + nBases] = np.random.uniform(MinY, MaxY)  # sorteia long
                        pop[i].base_profile[k]=self.B[0] #variar este parâmetro posteriormente 
                        pop[i].base_profile[k+nBases]=self.B[0] #variar este parâmetro posteriormente

            Tipo_base.append(pop[i].base_profile)    
            pop[i].velocity = np.zeros(nVar)
            BASES_PSO = get_bases_location(pop[i].position, LOCALIDADES,prohibited)  # arruma vetor particula
            COD_BASES_PSO = coord_to_cod(BASES_PSO, LOCALIDADES) #pega codigo das localidades c/ base                        
            pop[i].cost = FO(COD_BASES_PSO, COD_BASES_EXISTENTES)  # calcula custo p/ a particula i
            Custos_aux.append(pop[i].cost)            
            Pos.append(pop[i].position)
            pop[i].best_position = pop[i].position.copy()  #Update Personal Best
            pop[i].best_cost = pop[i].cost

        Custos = Custos_aux.copy()  #inicia respositorio com primeiros custos encontrados        
        lider_pos = SelectLeader(Custos, Pos)  #selecioina o lider
        for it in range(MaxIt):  #Main Loop
            #print('Iteration {0} '.format(it + 1))
            w = random.random()
            Pos_aux = []
            Custos_aux = []    
            Tipo_base_aux=[]        
            for i in range(nPop):
                pop[i].velocity = w * pop[i].velocity + \
                                  c1 * np.random.rand(nVar) * (pop[i].best_position - pop[i].position) + \
                                  c2 * np.random.rand(nVar) * (lider_pos - pop[i].position)  # atualiza velocidade
                pop[i].position = pop[i].position + pop[i].velocity  # atualiza posicao
                # checa limites
                pop[i].position[0:nBases] = np.minimum(pop[i].position[0:nBases], MaxX)
                pop[i].position[0:nBases] = np.maximum(pop[i].position[0:nBases], MinX)
                pop[i].position[nBases:] = np.minimum(pop[i].position[nBases:], MaxY)
                pop[i].position[nBases:] = np.maximum(pop[i].position[nBases:], MinY)

                Tipo_base_aux.append(pop[i].base_profile)
                Pos_aux.append(pop[i].position)  # salva posicao nova
                BASES_PSO = get_bases_location(pop[i].position, LOCALIDADES,prohibited)  # arruma vetor particula
                COD_BASES_PSO = coord_to_cod(BASES_PSO, LOCALIDADES)                
                pop[i].cost = FO(COD_BASES_PSO, COD_BASES_EXISTENTES)  # calcula custo p/ a particula i (custos,compensacao)
                Custos_aux.append(pop[i].cost)  # atualiza novos custos

            Pos = np.append(Pos, Pos_aux, axis=0)  #acrescenta novas posicoes na memoria
            Custos = np.append(Custos, Custos_aux, axis=0)  # acrescenta novos custos calculados ao repositorio  
            Tipo_base=np.append(Tipo_base,Tipo_base_aux,axis=0) #salva tipos de base  
            Custos, Pos= pareto(Custos, Pos)  #recebe C e Pos atualizadas (particulas n dominadas)
            lider_pos = SelectLeader(Custos, Pos)  #seleciona novo lider

            #print('Number of Rep Members = {0}'.format(len(Custos)))

        solutions_cod=[]    
        for p in Pos:
            BASES_PSO = get_bases_location(p, LOCALIDADES,prohibited)
            COD_BASES_PSO = coord_to_cod(BASES_PSO, LOCALIDADES)
            solutions_cod.append(COD_BASES_PSO)

        return Custos,solutions_cod
class FUNCAO_OBJETIVO:

    def __init__(self,P,r,f,d,T,D,h,m,g,S,c,beta,q,H,B,data,cod_modulo,cod_estudo,cod_empresa,payback=False):
        self.latlong_localities = {i: np.array([data.locs_bases.loc[i]['COORD_X'], data.locs_bases.loc[i]['COORD_Y']]) for i
            in data.locs_bases.index}  # lat e long de todas as localidades
        self.eletricistas_por_equipe=ELETRICISTAS_POR_EQUIPE(data.crew_formation)
        self.loc_distances=data.loc_distances
        self.loc_distances.columns=data.loc_distances.columns.astype(int)
        self.tempos = data.loc_times
        self.tempos.columns=data.loc_times.columns.astype(int)
        self.M={cod_proc: row['CUSTO_MANUTENCAO'] for cod_proc, row in data.electricians_costs.iterrows()}
        self.electrician_per_loc=data.electrician_per_loc
        self.existing_bases_cod=list(data.locs_bases[data.locs_bases ['POSSUI_BASE']=='S'].index)        
        self.loc_times=data.loc_times   
        self.locs_bases=data.locs_bases
        self.P=P
        self.q=q
        self.beta=beta
        self.c=c
        self.S=S
        self.g=g
        self.m=m
        self.h=h
        self.T=T
        self.D=D
        self.d=d
        self.f=f
        self.r=r   
        self.H=H   
        self.B=B
        self.data=data
        self.cod_modulo=cod_modulo
        self.cod_estudo=cod_estudo
        self.cod_empresa=cod_empresa
        self.payback=payback

    def FO(self,COD_BASES_PSO,existing_bases_cod):     
        #DEFINIR COMO VAI FICAR PERFIL DA BASE   
        custo_desloc, atendimento_loc_dic = self._CUSTO_DESLOC(self.d,self.loc_distances, self.latlong_localities,COD_BASES_PSO, self.q, self.beta, self.c,self.S,self.H,tempos=self.tempos,cod_perf_base=self.B[0]) #ta pegando unico perfil de base
        install, manutencao, desinstal, qtd_instaladas, qtd_desinstaladas = self._CUSTO_BASE(COD_BASES_PSO,existing_bases_cod,self.f,cod_perf_base=self.B[0])#ta pegando unico perfil de base
        custoequipes = self._CUSTO_EQUIPES(self.g, atendimento_loc_dic, cod_perf_base=self.B[0])#ta pegando unico perfil de base
        custocompensacoes = self._CUSTO_COMPENSACOES(self.m, atendimento_loc_dic,self.S)
        custoeletricista, soma_eletricistas = self._CUSTO_ELETRICISTA(self.h, atendimento_loc_dic,self.S, self.r,self.eletricistas_por_equipe, self.T, self.D,cod_perf_base=self.B[0])#ta pegando unico perfil de base
        OPEX = custo_desloc + manutencao  + custoequipes + custoeletricista+ custocompensacoes # soma de custos OPEX
        CAPEX = install+ desinstal #custo CAPEX

        return CAPEX,OPEX

    def _CUSTO_DESLOC(self,d,TABELA_DISTANCIAS, LOCALIDADES, COD_BASES_PSO, q, BETA, c, S,H,tempos,cod_perf_base):
        atendimento_loc_dic = {}  #esse dic salva quais localidades cada base atende. É utiliado nas funções CUSTO_EQUIPES e CUSTO_COMPENSACOES
        custo_total = 0  #var aux        
        for cod in LOCALIDADES:  #varre os códigos das localidades
            cod_loc_atendente = tempos.loc[cod, COD_BASES_PSO].idxmin()  # pega cod da localidade q atendeu a localidade 'cod'
            atendimento_loc_dic[cod] = cod_loc_atendente
            for s in S: #calcula custo de deslocamento pra cada serviço
                if H[s][cod][cod_perf_base][cod_loc_atendente]:
                    beta = BETA[s][cod][cod_loc_atendente]
                    dii=TABELA_DISTANCIAS.loc[cod,cod] 
                    custo_total += 2 * dii * c[s] * q.get(s,{cod:0}).get(cod,0) / beta
                else:       
                    beta = BETA[s][cod][cod_loc_atendente]
                    custo_total += (d[cod][cod_loc_atendente]+d[cod_loc_atendente][cod])* c[s] * q.get(s,{cod:0}).get(cod,0) / beta  # calcula custo para atender demanda da localidade i (considera ida e volta)                
        return custo_total, atendimento_loc_dic

    def _CUSTO_BASE(self,COD_BASES_PSO, existing_bases_cod,f,cod_perf_base):  # custos de instalação+manutenção+desinstalação das bases
        #i = 0, 1, 2(manut., instal., fechamento)
        custo_manutencao = f[cod_perf_base][0]  # pega custo de manutencao
        custo_instal = f[cod_perf_base][1]  # pega custo de instalacao
        custo_desinstal = f[cod_perf_base][2]  # pega custo de desinstalacao

        bases_existentes = set(existing_bases_cod)  # TRANSFORMA A LISTA COM BASES EXISTENTES EM SET

        bases_sugeridas = set(COD_BASES_PSO)  # TRANSFORMA A LISTA COM BASES SUGERIDAS EM SET

        bases_alocadas = bases_sugeridas.difference(bases_existentes)  # PEGA QUAIS BASES FORAM ALOCADAS

        qtd_bases_alocadas = len(bases_alocadas)  # PEGA A QUANTIDADE DE BASES ALOCADAS

        bases_desinstaladas = bases_existentes.difference(COD_BASES_PSO)  # PEGA QUAIS BASES FORAM DESALOCADAS

        qtd_bases_desinstaladas = len(bases_desinstaladas)  # PEGA A QUANTIDADE DE BASES ALOCADAS

        qtd_bases_manutencao = len(bases_sugeridas)  # PEGA A QUANTIDADE DE BASES SUGERIDAS

        custo_instalacao_base = qtd_bases_alocadas * custo_instal  # CALCULA O CUSTO COM INSTALACAO DE BASES

        custo_manutencao_base = qtd_bases_manutencao * custo_manutencao  # CALCULA O CUSTO COM MANUTENCAO DE BASES

        custo_desinstalacao_base = qtd_bases_desinstaladas * custo_desinstal  # CALCULA O CUSTO COM DESINSTALACAO DE BASES

        return custo_instalacao_base, custo_manutencao_base, custo_desinstalacao_base, qtd_bases_alocadas, qtd_bases_desinstaladas

    def _CUSTO_COMPENSACOES(self,mij, atendimento_loc_dic,S):  # custo relativo às compensações
        custo_compensacoes = 0
        for s in S:
            for cod_loc in atendimento_loc_dic:
                custo_compensacoes += mij[s][cod_loc][atendimento_loc_dic[cod_loc]]
            return custo_compensacoes

    def _CUSTO_ELETRICISTA(self,h, atendimento_loc_dic, S,eletricistas_por_localidade, eletricistas_por_equipe, T,D,cod_perf_base):  # custos de contratação+demissão de eletricista
        contratacao_eletricistas = 0  # var aux
        demissao_eletricistas = 0  # var aux
        soma_eletricistas = 0  # var aux
        eq_por_loc = {}  # qtd de equipes por localidade
        total_elet_existente = {}
        qtd_ele = {}
        for cod_loc in atendimento_loc_dic: #criando dic p/ salvar equipes por localidade
            eq_por_loc[atendimento_loc_dic[cod_loc]] = {}
        #hsibje
        for s in S:  # Percorre os servicos
            for cod_loc in atendimento_loc_dic:  # Percorre as localidades atendidas por bases

                equipes = h[s][cod_loc][cod_perf_base][atendimento_loc_dic[cod_loc]]  # Pega o tamamnho da lista contendo os eletricistas
                #hsibje
                for tipo_equipe in equipes:  # Percorre a lista contendo os eletricistas

                    if (h[s][cod_loc][cod_perf_base][atendimento_loc_dic[cod_loc]][tipo_equipe] != 0):  # Testa se houve alguma equipe que atendeu a OS

                        qtt_equipes = h[s][cod_loc][cod_perf_base][atendimento_loc_dic[cod_loc]][tipo_equipe]  # pega quantidade de equipes to tipo 'tipo_equipe'

                        if tipo_equipe not in eq_por_loc[atendimento_loc_dic[cod_loc]]:

                            eq_por_loc[atendimento_loc_dic[cod_loc]][tipo_equipe] = qtt_equipes  # salva qtd. de eqp. do tipo 'tipo_equipe'
                        else:
                            eq_por_loc[atendimento_loc_dic[cod_loc]][tipo_equipe] += qtt_equipes  # acrescenta qtd. de eqp. do tipo 'tipo_equipe'

        #salva qtd de eletricistas estimados                   
        for cod_loc_atendente in eq_por_loc:
            for tipo_equipe in eq_por_loc[cod_loc_atendente]:  # varre tipos de eq. necessarias na localidade 'cod_loc_atendente'
                tipo_ele = eletricistas_por_equipe[tipo_equipe][0]  # tipo de ele. da equipe 'tipo_equipe'
                if tipo_ele not in qtd_ele:
                    qtd_ele[tipo_ele] = eq_por_loc[cod_loc_atendente][tipo_equipe] * \
                                        eletricistas_por_equipe[tipo_equipe][1]  # qtd. ele. do tipo 'tipo_ele' contratados
                elif tipo_ele in qtd_ele:
                    qtd_ele[tipo_ele] += eq_por_loc[cod_loc_atendente][tipo_equipe] * \
                                         eletricistas_por_equipe[tipo_equipe][1]  # qtd. ele. do tipo 'tipo_ele' contratados

        #pega qtd de eletricistas existentes por localidade                                        	
        for cod_loc in eletricistas_por_localidade:
            for tipo_ele in eletricistas_por_localidade[cod_loc]:
                if tipo_ele not in total_elet_existente:
                    total_elet_existente[tipo_ele] = eletricistas_por_localidade[cod_loc][tipo_ele]
                else:
                    total_elet_existente[tipo_ele] += eletricistas_por_localidade[cod_loc][tipo_ele]

        #calcula custos relativos a contratacao e demissao
        for tipo_ele in total_elet_existente:
            aux = np.ceil(qtd_ele.get(tipo_ele, 0)) - total_elet_existente[tipo_ele]  # calcula dif. entre sugeridos e existentes
            if qtd_ele.get(tipo_ele, 0) != 0:  # caso o eletricista do tipo 'tipo_ele' não seja contratado
                soma_eletricistas += qtd_ele[tipo_ele]
            if aux > 0:
                contratacao_eletricistas += aux * T[tipo_ele]
            else:
                demissao_eletricistas += aux * D[tipo_ele]

        soma_eletricistas = np.ceil(soma_eletricistas)
        custo_eletricistas = contratacao_eletricistas - demissao_eletricistas
        return custo_eletricistas, soma_eletricistas 

    def _CUSTO_EQUIPES(self,g,atendimento_loc_dic,cod_perf_base):  # custo de manutenção das equipes ao longo do período de execução
        # Função que calcula o custo com equipes para atendimento das OS’s do tipo o
        # na localidade i por uma base do tipo b na localidade j
        custo_equipes = 0
        for cod_loc in atendimento_loc_dic:                               
                custo_equipes += g[cod_loc][cod_perf_base][atendimento_loc_dic[cod_loc]]
        return custo_equipes  # custo de manutenção das equipes ao longo do período de execução
class ATUALIZA_ELETRICISTAS(FUNCAO_OBJETIVO):

    def _FO_SAIDA(self,COD_BASES_PSO,existing_bases_cod):
        custo_desloc,atendimento_loc_dic = self._CUSTO_DESLOC(self.d, self.loc_distances,self.latlong_localities,COD_BASES_PSO, self.q,self.beta, self.c,self.S,H=self.H,tempos=self.tempos,cod_perf_base=self.B[0])
        custocompensacoes = self._CUSTO_COMPENSACOES(self.m, atendimento_loc_dic,self.S)
        install, manutencao, desinstall, bases_alocadas,bases_desinstaladas,bases_mantidas=self._CUSTO_BASE_SAIDA(COD_BASES_PSO, existing_bases_cod,self.f,self.B[0])
        ATENDIMENTO_LOCALIDADE=get_atendimento_base(self.locs_bases,self.loc_times,COD_BASES_PSO)
        try:
            electricians_costs,qtd_perfil,electricians_bases= self._CUSTO_ELETRICISTA_SAIDA(ATENDIMENTO_LOCALIDADE,COD_BASES_PSO)
            return install, manutencao, desinstall, bases_alocadas, bases_desinstaladas, bases_mantidas, electricians_costs, qtd_perfil, custocompensacoes, custo_desloc, atendimento_loc_dic, electricians_bases
        except Exception as erro:            
            print(erro, '_FO_SAIDA')             
            return None

    def _CUSTO_ELETRICISTA_SAIDA(self,ATENDIMENTO_LOCALIDADE,COD_BASES_PSO):
        #data=Data('tatic')
        #data.read(self.cod_modulo,self.cod_estudo,self.cod_empresa)             
        if self.payback==False:            
            data=copy.deepcopy(self.data) #self n pode ser modificado            
            data.depot=ATENDIMENTO_LOCALIDADE                         
            data.locs_bases=update_bases(COD_BASES_PSO,self.B[0],data.locs_bases)                  
            sched= Scheduling(data,self.cod_modulo,self.cod_estudo)#instancia do modelo de dimensionamento de eletricistas                 
        else:                    
            self.data.depot=ATENDIMENTO_LOCALIDADE
            sched= Scheduling(self.data,self.cod_modulo,self.cod_estudo)#instancia do modelo de dimensionamento de eletricistas         
        try:
            resume_all,resume_all_quant,resume_all_costs=sched.run() #roda modelo de dimensionamento de eletricistas
            #print("roda modelo de dimensionamento de eletricistas OK")
            electricians_costs,qtd_perfil=get_electricians_data(resume_all,self.P,self.M) #retorna qtd e custos por perfil de eletricista
            #print("retorna qtd e custos por perfil de eletricista OK")
            electricians_bases=get_electricians_per_base(resume_all_quant, self.P)
            #print("electricians_bases OK?")
            return electricians_costs, qtd_perfil, electricians_bases
            #print("return problema?")
        except Exception as erro:            
            print(erro, '_CUSTO_ELETRICISTA_SAIDA')                      
            return None

    def _CUSTO_BASE_SAIDA(self,COD_BASES_PSO, existing_bases_cod,f,cod_perf_base):
        custo_manutencao = f[cod_perf_base][0]  # pega custo de manutencao
        custo_instal = f[cod_perf_base][1]  # pega custo de instalacao
        custo_desinstal = f[cod_perf_base][2]  # pega custo de desinstalacao
        bases_existentes = set(existing_bases_cod)  # TRANSFORMA A LISTA COM BASES EXISTENTES EM SET        
        bases_sugeridas = set(COD_BASES_PSO)  # TRANSFORMA A LISTA COM BASES SUGERIDAS EM SET        
        bases_alocadas = bases_sugeridas.difference(bases_existentes)  # PEGA QUAIS BASES FORAM ALOCADAS
        bases_mantidas=bases_existentes.intersection(bases_sugeridas) #pega bases mantidas
        qtd_bases_alocadas = len(bases_alocadas)  # PEGA A QUANTIDADE DE BASES ALOCADAS
        bases_desinstaladas = bases_existentes.difference(COD_BASES_PSO)  # PEGA QUAIS BASES FORAM DESALOCADAS
        qtd_bases_desinstaladas = len(bases_desinstaladas)  # PEGA A QUANTIDADE DE BASES ALOCADAS
        qtd_bases_final = len(bases_sugeridas)  # PEGA A QUANTIDADE DE BASES SUGERIDAS
        custo_instalacao_base = qtd_bases_alocadas * custo_instal  # CALCULA O CUSTO COM INSTALACAO DE BASES
        custo_manutencao_base = qtd_bases_final * custo_manutencao  # CALCULA O CUSTO COM MANUTENCAO DE BASES
        custo_desinstalacao_base = qtd_bases_desinstaladas * custo_desinstal  # CALCULA O CUSTO COM DESINSTALACAO DE BASES
        return custo_instalacao_base, custo_manutencao_base, custo_desinstalacao_base, bases_alocadas,bases_desinstaladas,bases_mantidas

    def electrician_sizing(self, bases_per_solution,cod_perf_base):
        custos_atualizados = []  # frente de pareto atualizada
        input_payback = []  # custos para utilizacao no calculo de payback
        output_interface = []  # dados q serao exibidos ao usuario

        for i in range(len(bases_per_solution)):
            COD_BASES_PSO = bases_per_solution[i]
            #ATUALIZA SOLUCAO C/ ELETRICISTAS DO TATICO
            try:
                install, manutencao, desinstall, bases_alocadas, bases_desinstaladas, bases_mantidas, electricians_costs, qtd_perfil, custocompensacoes, custo_desloc, atendimento_loc_dic, electricians_bases = self._FO_SAIDA(
                    COD_BASES_PSO, self.existing_bases_cod)
                # CALCULA CUSTOS ANTIGOS ELETRICISTAS
                CAP, OP = self.FO(COD_BASES_PSO, self.existing_bases_cod)
                custoequipes = self._CUSTO_EQUIPES(self.g, atendimento_loc_dic,cod_perf_base=cod_perf_base)
                custoeletricista, soma_eletricistas = self._CUSTO_ELETRICISTA(self.h, atendimento_loc_dic, self.S, self.r,
                                                                              self.eletricistas_por_equipe, self.T, self.D,
                                                                              cod_perf_base=cod_perf_base)
                # DESCONTA CUSTOS ANTIGOS
                OP -= (custoeletricista + custoequipes)  # retira custos antigos
                # ACRESCENTA CUSTOS ATUALIZADOS
                new_costs = 0
                for cod_processo in self.P:
                    new_costs += electricians_costs[cod_processo][0]  # contratacao
                    new_costs += electricians_costs[cod_processo][1]  # demissao
                    new_costs += electricians_costs[cod_processo][2]  # transferencia
                    new_costs += electricians_costs[cod_processo][3]  # manutencao
                OP += new_costs  # acrescenta custos atualizados

                # SALVA SOLUCOES ATUALIZADAS
                aux = [install, manutencao, desinstall, bases_alocadas, bases_desinstaladas, bases_mantidas,
                       custocompensacoes, custo_desloc, electricians_costs, electricians_bases, qtd_perfil,COD_BASES_PSO]

                sol = solutions(aux)
                output_interface.append(sol)
                custos_atualizados.append([CAP, OP])
                input_payback.append([install, manutencao, desinstall, electricians_costs, custo_desloc,custocompensacoes])
            except Exception as erro:
                print(erro, 'electrician_sizing')
                return [],[]

        output_interface = np.array(output_interface)
        custos_atualizados = np.array(custos_atualizados)
        input_payback = np.array(input_payback)

        #atualiza frente de pareto (retira solucoes dominadas, se houver)
        #custos_atualizados, input_payback, output_interface = pareto_update(custos_atualizados, input_payback,output_interface)

        return input_payback, output_interface  
class solutions:
    def __init__(self, sol):
        self.base_install_costs = None
        self.base_maintenance_costs = None
        self.base_closing_costs = None
        self.installed_bases = None
        self.closed_bases = None
        self.maintained_bases = None
        self.sugested_bases=None
        self.compensation_cost = None
        self.displace_cost = None
        self.transfer_costs = None
        self.hiring_costs = None
        self.firing_costs = None
        self.crew_costs = None
        self.investment = None  # lista [capex,opex]
        self.elec_by_base = None
        self.elec_qtd=None #qtd de eletricistas contratados, demitidos, transferidos
        self.payback=None # esse parametro sera preenchido por outra classe 
        self.electricians_costs=None         
        self._get_results(sol)

    def _get_results(self, sol):

        self.base_install_costs = sol[0]
        self.base_maintenance_costs = sol[1]
        self.base_closing_costs = sol[2]
        self.installed_bases = sol[3]
        self.closed_bases = sol[4]
        self.maintained_bases = sol[5]
        self.compensation_cost = sol[6]
        self.displace_cost = sol[7]
        self.electricians_costs=sol[8]
        self.hiring_costs =self._get_hiring_cost(sol[8])
        self.firing_costs =self._get_firing_cost(sol[8])
        self.transfer_costs = self._get_transfer_cost(sol[8])
        self.crew_costs = self._get_crew_cost(sol[8])        
        self.elec_by_base = sol[9]  # acesso: dic[cod_base][cod_processo]        
        self.elec_qtd=sol[10] #acesso [cod_processo][(0,1,2 ou 3)] 0=total, 1=contratados, 2=demitidos, 3=transferidos
        self.sugested_bases=sol[11]
        
    def _get_hiring_cost(self, electricians_costs):
        hiring_cost=0
        for cod_processo in electricians_costs:
            hiring_cost+=electricians_costs[cod_processo][0]
        return hiring_cost

    def _get_firing_cost(self, electricians_costs):
        firing_cost=0
        for cod_processo in electricians_costs:
            firing_cost+=electricians_costs[cod_processo][1]
        return firing_cost

    def _get_transfer_cost(self,electricians_costs):
        transfer_cost=0
        for cod_processo in electricians_costs:
            transfer_cost+=electricians_costs[cod_processo][2]
        return transfer_cost

    def _get_crew_cost(self, electricians_costs):
        crew_cost=0
        for cod_processo in electricians_costs:
            crew_cost+=electricians_costs[cod_processo][3]
        return crew_cost   
class CALCULA_PAYBACK(ATUALIZA_ELETRICISTAS,solutions):
    #TMA VIRA COMO PARAMETRO AQUI. REVER ISSO.
    def _get_current_costs(self):      
        if self.existing_bases_cod: #se houver bases  
            ATENDIMENTO_LOCALIDADE=get_atendimento_base(self.locs_bases,self.loc_times,self.existing_bases_cod)                
            install, manutencao, desinstall, bases_alocadas,bases_desinstaladas,bases_mantidas,electricians_costs,qtd_perfil,custocompensacoes,custo_desloc,atendimento_loc_dic,electricians_bases=self._FO_SAIDA(self.existing_bases_cod,self.existing_bases_cod)                
            manutencao_eletricistas=self._get_crew_cost(electricians_costs)
            OPEX= manutencao+custo_desloc+manutencao_eletricistas+custocompensacoes        
            return  OPEX
        else:
            return 0

    def get_payback(self, input_payback, output_interface):        
        OPEX_ATUAL=self._get_current_costs()                
        for i in range(len(input_payback)):
            OPEX_SUGERIDO,CAPEX=self._CUSTO_PROPOSTA(input_payback[i])
            FLUXO_CAIXA=self._FLUXO_CAIXA_ANO(OPEX_ATUAL, OPEX_SUGERIDO, QTD_ANOS_FUTURO=5, TMA_ANO=0.12, PAYBACK_SIMPLES=False)            
            payback=self._PAYBACK(CAPEX,FLUXO_CAIXA)            
            output_interface[i].payback=payback #salva parametro payback (faltante na solucao)

    def _CUSTO_PROPOSTA(self,input_payback):
        install=input_payback[0]
        manutencao = input_payback[1]
        desinstall = input_payback[2]
        electricians_costs=input_payback[3]
        custo_desloc=input_payback[4]
        compensacoes=input_payback[5]
        #CAPEX:
        firing=self._get_firing_cost(electricians_costs)
        hiring=self._get_hiring_cost(electricians_costs)
        transfer=self._get_transfer_cost(electricians_costs)
        CAPEX = install + desinstall + hiring + firing + transfer        
        #OPEX
        manutencao_eletricistas=self._get_crew_cost(electricians_costs)
        OPEX = manutencao + custo_desloc + manutencao_eletricistas+compensacoes
        return OPEX, CAPEX

    def _FLUXO_CAIXA_ANO(self,OPEX_ATUAL, OPEX_SUGERIDO, QTD_ANOS_FUTURO=5, TMA_ANO=0.12, PAYBACK_SIMPLES=False):
        if PAYBACK_SIMPLES == True:
            if OPEX_ATUAL==0:
                return OPEX_ATUAL
            else:
                FLUXO_CAIXA_ANO = (OPEX_ATUAL - OPEX_SUGERIDO)
            return FLUXO_CAIXA_ANO
        else:
            if OPEX_ATUAL==0:
                return OPEX_ATUAL
            else:
                FLUXO_CAIXA_ANO = (OPEX_ATUAL - OPEX_SUGERIDO) / (1 + TMA_ANO) ** QTD_ANOS_FUTURO
            return FLUXO_CAIXA_ANO

    def _PAYBACK(self,CAPEX, FLUXO_CAIXA):
        if FLUXO_CAIXA!=0:            
            payback_ano = CAPEX / FLUXO_CAIXA
            return payback_ano
        else:
            return 0
class LocationModel:
    print("\nLocation.py, Linha 732, executando class LocationModel")   # Remover - RL
    def __init__(self, L, B, S, d, c, q, beta, m, f, a, g, P, T, D, E, l, r, h, hotel_link, fixed_bases):
        self.fixed_bases = fixed_bases
        self.L = L
        self.B = B
        self.S = S
        self.d = d
        self.c = c
        self.q = q
        self.beta = beta
        self.m = m
        self.f = f
        self.a = a
        self.g = g
        self.P = P
        self.T = T
        self.D = D
        self.E = E
        self.l = l
        self.h = h
        self.hotel_link = hotel_link
        # variables
        self.x = {}
        self.y = {}
        self.yf = {}
        self.z = {}
        self.w = {}
        self.alpha = {}
        self.theta = {}
        print("\nLocation.py, linha 761, dentro do LocationModel, def __init__. Variáveis inicializadas com sucesso")   # Remover - RL
        total_elect = {}
        for j, rj in r.items():
            for p, rjp in rj.items():
                if p not in total_elect:
                    total_elect[p] = 0
                total_elect[p] += rjp
        self.r = total_elect
        print("\nLinha 769. Terminou def __init__")
    def _create_variables(self):
        print("\nLocation.py. Linha 771, def _create_variables(self) executando")   # Remover - RL
        self.x = LpVariable.dicts('x', (self.L, self.B, self.L), 0, 1, LpInteger)
        self.y = LpVariable.dicts('y', (self.L, self.B), 0, 1, LpInteger)
        self.yf = LpVariable.dicts('yf', (self.L, self.B), 0, 1, LpInteger)
        self.yi = LpVariable.dicts('yi', (self.L, self.B), 0, 1, LpInteger)
        self.z = LpVariable.dicts('z', self.E)
        self.w = LpVariable.dicts('w', self.P, lowBound=0, cat=LpInteger)
        self.alpha = LpVariable.dicts('alpha', self.P, 0)
        self.theta = LpVariable.dicts('theta', self.P, 0)
        print("\nLocation.py, linha 780, dentro do def _create_variables(self) fim")   # Remover - RL
    def _get_objective_function_terms(self):
        f1_terms = []
        f2_terms = []
        print("\nLocation.py. Linha 784. Começo do def _get_objetive_function_terms. Indo executar for")   # Remover - RL
        print("\nìndice de Localidades L: ",self.L)   # Remover - RL
        print("\nÍndice de perfis base B: ",self.B)   # Remover - RL
        print("\nCódigos de serviço S: ",self.S)      # Remover - RL
        # adiciona a primeira e quarta parcelas da funcao objetivo
        for i in self.L:
            #print("\nlocation.py, linha 789, loop for self.L: ",i)   # Remover - RL
            for b in self.B:
                #print("\nlocation.py, linha 791, loop for self.B: \n",b)   # Remover - RL
                for j in self.L:
                    #print("\nlocation.py, linha 793, segundo loop for self.L: \n",j)   # Remover - RL
                    if self.g[i][b][j] == float('inf') or self.g[i][b][j] == 0.0:
                        print("\nSe ver essa mensagem é pq não existe variável, pois o atendimento é inviável ou não tem OS do bloco")   # Remover - RL
                        continue  # nao existe variavel, pois atendimento eh inviavel ou nao tem OS do bloco
                    sum_aux = 0
                    for s in self.S:
                        #print("\nLocation.py, linha 798, loop for self.S: \n",s)   # Remover - RL
                        if self.hotel_link[s][i][b][j]:
                            sum_aux += (self.d[i][i] + self.d[i][i]) * (self.c[s] * self.q[s][i]) / self.beta[s][i][i]
                            sum_aux += self.m[s][i][i]
                        else:
                            sum_aux += (self.d[i][j] + self.d[j][i]) * (self.c[s] * self.q[s][i]) / self.beta[s][i][j]
                            sum_aux += self.m[s][i][j]
                    
                    f2_terms.append(sum_aux * self.x[i][b][j])
                    f2_terms.append(self.g[i][b][j] * self.x[i][b][j])
        print("\nLocation.py, linha 802, adicionadas primeira e quarta parcelas da FO")   # Remover - RL
        for j in self.L:
            print("\nlocation.py, linha 806, loop for self.L: ",j)   # Remover - RL
            for b in self.B:
                print("\nlocation.py, linha 808, loop for self.B: \n",b)   # Remover - RL
                ajb = self.a[j].get(b, 0)
                f1_terms.append(self.f[b][0] * self.y[j][b])
                if ajb == 0:
                    f1_terms.append(self.f[b][1]*self.yi[j][b])
                else:
                    f1_terms.append(self.f[b][2] * self.yf[j][b])
        print("\nLocation.py, linha 811. Prestes a adicionar quinta e sexta parcelas da FO")   # Remover - RL
        # adiciona quinta e sexta parcelas da funcao objetivo
        for p in self.P:
            tp = self.T[p]
            f2_terms.append(tp * self.alpha[p])
        print("\nLocation.py, linha 816")
        for p in self.P:
            dp = self.D[p]
            f2_terms.append(dp * self.theta[p])
        print("\nLocation.py, linha 820. Fim do def _get_objetive_function_terms")   # Remover - RL
        return f1_terms, f2_terms

    def _add_constraints(self, problem, delta_bases, forced_nb_bases):
        print("\nLocation.py, linha 824. def _add_constraints executando")   # Remover - RL
        # força que toda localidade seja atendida por exatamente 1 base
        for i in self.L:
            terms = []
            for b in self.B:
                for j in self.L:
                    if self.g[i][b][j] == float('inf') or self.g[i][b][j] == 0.0:
                        problem += self.x[i][b][j] == 0
                        continue
                    terms.append(self.x[i][b][j])
                    problem += self.x[i][b][j] <= self.y[j][b]
            if terms:
                problem += lpSum(terms) == 1
        print("Location.py, Linha 837. executar loop que evita abertura de mais de uma base pro locadidade")   # Remover - RL
        # evita abertura de mais de uma base por localidade
        for j in self.L:
            terms = []
            for b in self.B:
                terms.append(self.y[j][b])
            problem += lpSum(terms) <= 1
        print("Location.py, linha 844. Executado. A executar fechamento e instalação de bases")   # Remover - RL
        # captura fechamento e instalacao de base
        for b in self.B:
            for j in self.L:
                if self.a[j].get(b,0) == 1:
                    problem += self.yf[j][b] >= self.a[j].get(b, 0) - self.y[j][b]
                    problem += self.yi[j][b] == 0
                else:
                    problem += self.yi[j][b] >= self.y[j][b] - self.a[j].get(b, 0)
                    problem += self.yf[j][b] == 0
        print("Location.py, linha 854. Executado. A executar delta_bases")   # Remover - RL
        # delta_bases é a diferença do numero atual de bases 
        # (e.x. se for -5 terá que achar uma solução com 5 bases a menos e se for 3, uma solução com 3 bases a mais)
        if delta_bases != None: # força numero fixo de bases
            terms, nb_bases = [], 0
            install_term = []
            terms_all = []
            for b in self.B:
                for j in self.L:
                    terms_all.append(self.y[j][b])
                    if self.a[j].get(b, 0):
                        terms.append(self.y[j][b])
                        nb_bases += 1
                    else:
                        install_term.append(self.yi[j][b])
            if delta_bases <= 0:
                problem += lpSum(terms) == nb_bases + delta_bases 
                problem += lpSum(terms_all) == nb_bases + delta_bases 
            else:
                problem += lpSum(terms) == nb_bases
                problem += lpSum(install_term) == delta_bases
        print("Location.py, linha 875. Executado. A executar loop para encontrar k bases")   # Remover - RL
        # precisa encontrar exatamente k bases
        if forced_nb_bases != None:
            terms = []
            for b in self.B:
                for j in self.L:
                    terms.append(self.y[j][b])
            problem += lpSum(terms) == forced_nb_bases
        print("Location.py, linha 883. Executado. A executar verificação das bases fixas!")   # Remover - RL
        # Multi objetivo não permite bases fixas
        #if soluc_type == 0:   # Mono-objetivo com ou sem var de bases. Novo - RL
        # proibe fechar certas bases
        for fb in self.fixed_bases:
            for b in self.B:
                if self.a[fb].get(b, 0) == 1:
                    problem += self.yf[fb][b] == 0
                    break
        #elif soluc_type == 1:   # Novo - RL
        #    soluc_type = 1;
        print("Location.py, linha 894. Executado.")   # Remover - RL
        # evita abertura de base sem atendimento (possivel devido demissao e contratacao)
        for j in self.L:
            for b in self.B:
                terms = []
                for i in self.L:
                    if self.g[i][b][j] == float('inf') or self.g[i][b][j] == 0.0:
                        continue
                    terms.append(self.x[i][b][j])
                problem += self.y[j][b] <= lpSum(terms)
        print("Location.py, linha 904. A encontrar quantidade de equipes")   # Remover - RL
        # captura quantidade de equipes 
        for e in self.E:
            terms = []
            for j in self.L:
                for i in self.L:
                    for b in self.B:
                        if self.g[i][b][j] == float('inf') or self.g[i][b][j] == 0.0:
                            continue
                        sum_aux = 0.0
                        for s in self.S:
                            sum_aux += self.h[s][i][b][j][e]
                        terms.append(sum_aux * self.x[i][b][j])
            if terms:
                problem += self.z[e] == lpSum(terms)
        print("Location.py, linha 919. A encontrar quantidade de eletricistas")   # Remover - RL
        # Captura quantidade de eletricistas 
        for p in self.P:
            terms = []
            for e in self.E:
                terms.append(self.l.get((p, e),0) * self.z[e])
            problem += self.w[p] >= lpSum(terms)
            problem += self.w[p] <= lpSum(terms) + 1
        print("Location.py, linha 927. Captura contração")   # Remover - RL
        # Captura contratacao
        for p in self.P:
            problem += self.alpha[p] >= (self.w[p]- self.r.get(p,0))
        print("Location.py, linha 931. Captura demissão")   # Remover - RL
        # Captura demissao
        for p in self.P:
            problem += self.theta[p] >= (self.r.get(p,0) - self.w[p])
        print("Location.py, linha 935, fim do def _add_constraints")   # Remover - RL
    def solve(self, f1_bound=0, f2_bound=0, fixed=None, delta_bases=None, write_details=False, forced_nb_bases = None):
        print("\nLocation.py, linha 937. def solve")   # Remover - RL
        self._create_variables()
        f1_terms, f2_terms = self._get_objective_function_terms()

        problem = LpProblem("Location", LpMinimize)
        print("Location.py, linha 942. A definir termos da FO de acordo com parametros passados")   # Remover - RL
        # define termos da funcao objetivo de acordo com os parametros passados
        if fixed == None:
            problem += lpSum(f1_terms + f2_terms), "Z"
        elif fixed == 'all':
            problem += lpSum(f1_terms + f2_terms), "Z"
            problem += lpSum(f1_terms) <= f1_bound+1
            problem += lpSum(f2_terms) <= f2_bound+1
        elif fixed == 'f1':
            problem += lpSum(f2_terms), "Z"
            problem += lpSum(f1_terms) <= f1_bound+1
        elif fixed == 'f2':
            problem += lpSum(f1_terms), "Z"
            problem += lpSum(f2_terms) <= f2_bound+1
        else:
            problem += lpSum(f1_terms + f2_terms), "Z"
        print("Location.py, linha 958. Adicionar restrições")   # Remover - RL
        self._add_constraints(problem, delta_bases, forced_nb_bases)
        print("Location.py, linha 960. Restrições adicionadas")   # Remover - RL 
        nearestBase = False # solucao respeita premissa de base mais proxima
        status, objVal = 0, -1
        print("\nLocation.py, linha 963. Começar solução")   # Remover - RL
        print("\n########### solving")

        problem.writeLP("location.lp")
        problem.solve(COIN(msg=0, maxSeconds=60)) # chama solver

        status = LpStatus[problem.status]
        print("\nStatus: ", status)
        if status == 'Infeasible':
            return -1, []
        objVal = value(problem.objective)
        print("Objective value: ", objVal)
        print("Location.py, linha 975")   # Remover - RL
        bases_locations = set()
        for j in self.L:
            for b in self.B:
                if self.y[j][b].varValue > 0.99:
                    bases_locations.add(j)
        print("Location.py, linha 981")   # Remover - RL
        bases_locations_verify = bases_locations
        bases_locations = {l: [] for l in bases_locations}
        print("Location.py, linha 984")   # Remover - RL
        cost_f1, cost_f2, cost_f3 = 0.0, 0.0, 0.0
        nb_installed, nb_closed = 0, 0
        nearest_base = {l: [0,0,float('inf')] for l in self.L} # nao esta generico p/ blocos de servico (porem blocos de servico serao extindos na refatacao)
        total_bases = 0
        for b in self.B:
            for j in self.L:
                ajb = self.a[j].get(b, 0)
                if self.y[j][b].varValue > 0.99:
                    total_bases += 1
                    cost_f1 += self.f[b][0]
                    # atualiza base mais proxima das localidades
                    for i in self.L:
                        if self.d[j][i] < nearest_base[i][2]:
                            nearest_base[i] = [b, j, self.d[j][i]]
                if ajb == 1 and self.yf[j][b].varValue > 0.99:
                    nb_closed += 1
                    print('Base em', j, 'foi fechada!')
                    cost_f3 += self.f[b][2]
                if ajb == 0 and self.yi[j][b].varValue > 0.99:
                    nb_installed += 1
                    print('Base em', j, 'foi aberta!')
                    cost_f2 += self.f[b][1]

        print("Location.py, linha 1008. Atualização das bases")   # Remover - RL

        nb_recruitment = 0 # quantidade a mais de eletricistas no geral (ou a menos)
        cost_recruitment, cost_dismiss = 0.0, 0.0
        for p in self.P:
            if self.alpha[p].varValue > 0.0:
                print(' Quantidade de eletricistas do perfil ' + str(p) + ': ', self.w[p].varValue,
                        '\n Quantidade anterior: ', self.r.get(p, 0),
                        '\n Quantidade de eletricistas a serem contratados: ', self.alpha[p].varValue)
                cost_recruitment += self.alpha[p].varValue * self.T[p]
                nb_recruitment += self.alpha[p].varValue
            if self.theta[p].varValue > 0.0:
                print(' Quantidade de eletricistas do perfil ' + str(p) + ': ', self.w[p].varValue,
                        '\n Quantidade anterior: ', self.r.get(p, 0),
                        '\n Quantidade de eletricistas a serem demitidos: ', self.theta[p].varValue)
                cost_dismiss += self.theta[p].varValue * self.D[p]
                nb_recruitment -= self.theta[p].varValue

        cost_crew, cost_displac, cost_indemn = 0.0, 0.0, 0.0
        for i in self.L:
            for b in self.B:
                for j in self.L:
                    if (self.g[i][b][j] != float('inf') and self.g[i][b][j] > 0.0) and self.x[i][b][j].varValue > 0.99:
                        cost_crew += self.g[i][b][j]
                        for s in self.S:
                            if self.hotel_link[s][i][b][j]:
                                cost_displac += (self.d[i][i] + self.d[i][i]) * (self.c[s] * self.q[s][i]) / self.beta[s][i][i]
                                cost_indemn += self.m[s][i][i]
                            else:
                                cost_displac += (self.d[i][j] + self.d[j][i]) * (self.c[s] * self.q[s][i]) / self.beta[s][i][j]
                                cost_indemn += self.m[s][i][j]
                        bases_locations[j].append(i)

        print()
        bases = []
        for l, locs in bases_locations.items():
            bases.append(l)
            print("Base em", l, "atende:", end=' ')
            print(', '.join(str(x) for x in locs))
        print()

        print("Total de bases: ", total_bases)
        total_cost = cost_f1 + cost_displac + cost_indemn
        print("Custo com manutenção de bases: ", cost_f1)
        print("Custo com instalação de bases: ", cost_f2)
        print("Custo com fechamento de bases: ", cost_f3)
        total_cost += cost_f2 + cost_f3
        print("Custo com deslocamento: ", cost_displac)
        print("Custo com compensacoes: ", cost_indemn)
        print("Custo com equipes: ", cost_crew)
        total_cost += cost_crew
        print("Custo com contratação de eletricistas: ", cost_recruitment)
        print("Custo com demissão de eletricistas: ", cost_dismiss)
        total_cost += cost_recruitment + cost_dismiss
        print("Custo total: ", total_cost) 

        return total_cost, bases
def get_dataframe(status, cod_estudo, modulo, id_situacao,msg=''):
        data = datetime.datetime.now()
        data = datetime.datetime.strftime(data, '%y/%m/%d %H:%M:%S')

        if modulo == 'forecast':
            if id_situacao == 2:
                msg = 'Processado'
                df = pd.DataFrame({'IND_SITUACAO_MODULO_PREVISAO': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_PREVISAO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
            elif id_situacao == 3:
                if status == 1:
                    msg = 'Erro no cálculo de previsão anual'
                elif status == 2:
                    msg = 'Erro no cálculo de previsão horária'
                elif status == 3:
                    msg = 'Erro no cálculo de previsão emergencial'
                df = pd.DataFrame({'IND_SITUACAO_MODULO_PREVISAO': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_PREVISAO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
        elif modulo == 'strategic':
            if id_situacao == 2:
                msg = 'Processado'
                df = pd.DataFrame({'IND_SITUACAO_MODULO_ETTGC': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_ESTRATEGICO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
            elif id_situacao == 3:
                if status == 1:
                    msg = 'Erro na simulação multiobjetivo'
                #elif status == 2:     # originalmente comentado - RL
                    #msg = 'Erro na simulação mono-objetivo'      # originalmente comentado - RL
                df = pd.DataFrame({'IND_SITUACAO_MODULO_ETTGC': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_ESTRATEGICO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
        return df
def update_bases(COD_BASES,COD_PERFIL_BASE,locs_bases):
    locs_bases['POSSUI_BASE']='N'
    locs_bases['COD_PERFIL_BASE']=''    
    for b in COD_BASES:
        locs_bases.loc[b,'POSSUI_BASE']='S'
        locs_bases.loc[b,'COD_PERFIL_BASE']=COD_PERFIL_BASE    
    return locs_bases    
class Particle:
    def __init__(self):
        self.position = None
        self.velocity = None
        self.cost = None
        self.best_position = None
        self.best_cost = None
        self.base_profile=None
def coord_to_cod(coord_bases,loc_lat_long):
    lista=[]
    if len(coord_bases)==0:
        return 0
    else:
        for coord in coord_bases:
            for loc in loc_lat_long:
                if coord[0]==loc_lat_long[loc][0] and coord[1]==loc_lat_long[loc][1]:
                    lista.append(loc)
                    break
        return lista
def get_bases_location(BASES, LOCALIDADES,cod_prohibited):
    'Essa função posiciona as bases na localidade mais próxima'
    nbas = int(len(BASES) / 2)  # número de bases
    LatVec = BASES[:nbas]  # Latitude  de cada base
    LongVec = BASES[nbas:]  # Longitude de cada base

    #vetBases = np.array(np.zeros((nbas, 2)))  # reservando espaço
    vetBases=[]
    for i in range(nbas):  # arrumando o vetor para o formato [x,y]
        #vetBases[i] = [LatVec[i], LongVec[i]]
        vetBases.append([LatVec[i], LongVec[i]])

    for i in range(nbas):
        menordist = np.inf
        for key in LOCALIDADES:
            dist = distance.GreatCircleDistance(vetBases[i], LOCALIDADES[key])
            if dist < menordist:
                menordist = dist
                cod = key
        vetBases[i] = LOCALIDADES[cod]

    #salva bases que não podem ser fechadas
    if cod_prohibited:
        for cod in cod_prohibited:
            vetBases.append(LOCALIDADES[cod])

    novasBases = np.unique(vetBases, axis=0)  # tira valores repetidos (só pode haver uma base por localidade)

    return novasBases    
def SelectLeader(rep,Pos_aux):
    index=random.randint(0,len(rep)-1)
    leader_pos = Pos_aux[index]
    return leader_pos
def get_lat_long(locs_bases):
    'Essa função calcula os limites superiores e inferiores que as variáveis do problema podem assumir'
    X=np.array(locs_bases['COORD_X'],dtype=float)
    Y=np.array(locs_bases['COORD_Y'],dtype=float)
    VarMinX=np.inf
    VarMaxX=-np.inf
    VarMinY=np.inf
    VarMaxY=-np.inf
    for coord in range(len(X)):
        if X[coord]<VarMinX:
            VarMinX=X[coord]
        elif X[coord]>VarMaxX:
            VarMaxX=X[coord]
        if Y[coord]<VarMinY:
            VarMinY=Y[coord]
        elif Y[coord]>VarMaxY:
            VarMaxY=Y[coord]
    return VarMinX,VarMaxX,VarMinY,VarMaxY
def pareto_update(custos_aux,payback_aux,output_aux):
    custos = []
    payback = []
    output=[]        
    for i in range(0, len(custos_aux)):
        cont_dominated = 0
        for j in range(0, len(custos_aux)):
            r = dominate(custos_aux[j], custos_aux[i])
            if r == True:
                cont_dominated += 1

        if cont_dominated == 0: #se n for dominada, adiciona a lista auxiliar
            custos.append(custos_aux[i])
            payback.append(payback_aux[i])   
            output.append(output_aux[i])         
    		
    custos,payback,output=remove_duplicates_update(custos,payback,output)

    return custos,payback,output
def remove_duplicates_update(custos, payback,output):
    'essa função tira valores duplicados do vetor'
    duplicados = []
    for i in range(len(custos)):
        for j in range(i + 1, len(custos)):
            if np.all(custos[i] == custos[j]):
                duplicados.append(i)
    duplicados=np.array(duplicados)
    custos=np.delete(custos,duplicados,axis=0)
    payback=np.delete(payback, duplicados, axis=0)    
    output=np.delete(output, duplicados, axis=0) 
    
    return custos, payback,output
def pareto(rep_aux,Pos_aux):
    rep = []
    Pos = []    
    solutions_cod=[]
    for i in range(0, len(rep_aux)):
        cont_dominated = 0
        for j in range(0, len(rep_aux)):
            r = dominate(rep_aux[j], rep_aux[i])
            if r == True:
                cont_dominated += 1

        if cont_dominated == 0: #se n for dominada, adiciona a lista auxiliar
            rep.append(rep_aux[i])
            Pos.append(Pos_aux[i])
                        
            
    Pos=np.array(Pos)
    rep,Pos=remove_duplicates(rep,Pos)
    return rep,Pos
def remove_duplicates(rep, pos):
    'essa função tira valores duplicados do vetor'
    duplicados = []
    for i in range(len(rep)):
        for j in range(i + 1, len(rep)):
            if np.all(rep[i] == rep[j]):
                duplicados.append(i)
    duplicados=np.array(duplicados)
    rep=np.delete(rep,duplicados,axis=0)
    pos=np.delete(pos, duplicados, axis=0)    
    return rep, pos    
def dominate(x,y):

    retorno = (np.all(x<=y) & np.any(x<y))

    return retorno
def cod_para_coord(nVar,LOCALIDADES,bases_lista):
    'Essa funcao pega o cod das localidades e devolve as coordenadas para o pso'
    tam_lista=len(bases_lista)
    nBases=int(nVar/2)
    sol=np.zeros(nVar)
    for i in range(nBases):
        if i<tam_lista:
            sol[i]=LOCALIDADES[bases_lista[i]][0]
            sol[i+nBases]=LOCALIDADES[bases_lista[i]][1]
        else:#repete bases existentes
            sol[i]=LOCALIDADES[bases_lista[0]][0]
            sol[i+nBases]=LOCALIDADES[bases_lista[0]][1]
    return sol
def ELETRICISTAS_POR_EQUIPE(EQUIPES_ENTRADA):
    #aux=EQUIPES_ENTRADA[['COD_PERFIL_EQUIPE','QTD_ELETRICISTAS']]
    aux=EQUIPES_ENTRADA
    dic={}
    for index,row in aux.iterrows():
        dic[row['COD_PERFIL_EQUIPE']]=[row['COD_PERFIL_ELETRICISTA'],row['QTD_ELETRICISTAS']] #[tipo, qtd.]
    return dic
def get_electricians_data(resume_all, P, M):
    qtd_perfil = {}  # acesso [cod_processo,(0,1,2 ou 3)] 0=total, 1=contratados, 2=demitidos, 3=transferidos
    costs = {}  # acesso [cod_processo,(0,1 ou 2)] 0=contratacao, 1=demissao, 2= transferencias, 3= manutencao
    qtd = 'Quantidade'  # var aux
    custo = 'Custo'  # var aux

    for cod_processo in P:
        costs[cod_processo] = {}
        qtd_perfil[cod_processo] = {}

    for cod_processo in P:
        qtd_aux = 'Eletricistas (' + str(cod_processo) + ')'
        cont_aux = 'Contratacoes (' + str(cod_processo) + ')'
        demissao_aux = 'Demissoes (' + str(cod_processo) + ')'
        transf_aux = 'Transferencias (' + str(cod_processo) + ')'

        cost_0_aux = 'Contratacoes (' + str(cod_processo) + ')'
        cost_1_aux = 'Demissoes (' + str(cod_processo) + ')'
        cost_2_aux = 'Transferencias (' + str(cod_processo) + ')'

        qtd_perfil[cod_processo][0] = resume_all.loc[qtd, qtd_aux]  # salva qtd total
        qtd_perfil[cod_processo][1] = resume_all.loc[qtd, cont_aux]  # salva qtd contratados
        qtd_perfil[cod_processo][2] = resume_all.loc[qtd, demissao_aux]  # salva qtd demitidos
        qtd_perfil[cod_processo][3] = resume_all.loc[qtd, transf_aux]  # salva qtd transferidos

        costs[cod_processo][0] = resume_all.loc[custo, cost_0_aux]  # custo de contratacao
        costs[cod_processo][1] = resume_all.loc[custo, cost_1_aux]  # custo de demissao
        costs[cod_processo][2] = resume_all.loc[custo, cost_2_aux]  # custo de transferencia
        costs[cod_processo][3] = qtd_perfil[cod_processo][0] * M[cod_processo]  # custo de manutencao

    return costs, qtd_perfil
def get_electricians_per_base(resume_all_quant, P): 
    "Essa funcao pega a qtd de eletricistas estimada por base"
    cod_bases=list(resume_all_quant.index)    
    qtd_per_base={cod:{} for cod in cod_bases}       
    
    for b in cod_bases:                
        for cod_processo in P:
            qtd_aux = 'Eletricistas (' + str(cod_processo) + ')'
            
            if cod_processo not in qtd_per_base[b]:
                qtd_per_base[b][cod_processo]=resume_all_quant.loc[b,qtd_aux]
            else:
                qtd_per_base[b][cod_processo]+=resume_all_quant.loc[b,qtd_aux]
    return qtd_per_base
def get_custos_bases(bases_costs_data):
    rows = bases_costs_data.iterrows()
    f = {}
    for cod_perf, row in rows:
        f[cod_perf] = [row['CUSTO_MANUTENCAO'],
                       row['CUSTO_INSTALACAO'], row['CUSTO_FECHAMENTO']]
    return f
def get_km_cost(veicle_km_costs_data, services_data):
    c = {}
    for s, row in services_data.iterrows():
        c[s] = veicle_km_costs_data.at[row['COD_PERFIL_VEICULO'], 'CUSTO_KM']
    return c
def get_qtd_eletricistas(electricians_per_loc_data, L):
    r = {l: {} for l in L}
    for cod_loc, row in electricians_per_loc_data.iterrows():
        if cod_loc in r:
            r[cod_loc][row['COD_PERFIL_ELETRICISTA']] = row['QTD_ELETRICISTAS']
        else:
            print("Localidade ", cod_loc, " não existe no conjunto de localidades L!")
            break
    return r
def get_qtd_elet_eq(crew_formation_data):
    rows = crew_formation_data.iterrows()
    l = {}
    for cod_eq, row in rows:
        l[row['COD_PERFIL_ELETRICISTA'], row['COD_PERFIL_EQUIPE']] = row['QTD_ELETRICISTAS']
    return l
def get_loc_volume(oss_data_volume):
    q = oss_data_volume.groupby(['COD_LOCALIDADE']).sum().drop(['ANO','MES'],axis=1)
    rename_col = {}
    for k in q.columns:
        rename_col[k] = int(k.split(' ')[1].strip(" "))
    q = q.rename(columns=rename_col).to_dict() # acesso q[s][i] (num. de OS's do tip s na localidade i)
    return q
def get_a(loc_bases_data, bases_profiles_data, L):
    bases_codes = bases_profiles_data['COD_PERFIL_BASE'].unique()

    bases = {b: 0 for b in bases_codes}
    a = {i: copy.deepcopy(bases) for i in L}

    for l, row in loc_bases_data.iterrows():
        if row['POSSUI_BASE'].lower() == 's':
            a[l][row['COD_PERFIL_BASE']] = 1 
    return a
def get_atendimento_base(locs_bases_data,distances_data,cod_bases):
    localidades_cod=locs_bases_data.index #pega cod das localidades                            
                         
    #transformando colunas tipo string para int
    for col in distances_data.columns:
        distances_data.rename(columns={col:int(col)},inplace=True)
        
    atendimento_loc_dic = {}  # esse dic salva quais localidades cada base atende.
    for cod in localidades_cod:
        cod_loc_atendente = distances_data.loc[cod, cod_bases].idxmin()  # pega cod da localidade q atendeu a localidade 'cod'
        if cod_loc_atendente not in atendimento_loc_dic:
            atendimento_loc_dic[cod_loc_atendente] = [cod]
        else:
            atendimento_loc_dic[cod_loc_atendente] += [cod]
                         
    df=pd.DataFrame() #gera dataframe de saida
    for cod_loc_atendente in atendimento_loc_dic:
        for cod in atendimento_loc_dic[cod_loc_atendente]:
            data=pd.DataFrame({'COD_LOCALIDADE_CLIENTE':[cod],'COD_LOCALIDADE_BASE':[cod_loc_atendente]})        
            df=df.append(data)             
    return df
def get_tem_saida(L, locs_bases_data):
    tem_saida = {}
    for j in L:
        tem_saida[j] = locs_bases_data.loc[j]['TEMPO_SAIDA']
    return tem_saida
def return_qtd_ele(elec_qtd):
    qtd_total = 0
    qtd_hired = 0
    qtd_fired = 0
    qtd_transf = 0

    for cod in elec_qtd:
        qtd_total += elec_qtd[cod][0]
        qtd_hired += elec_qtd[cod][1]
        qtd_fired += elec_qtd[cod][2]
        qtd_transf += elec_qtd[cod][3]

    return qtd_total, qtd_hired, qtd_fired, qtd_transf
def get_general_results(sols,cod_modulo):
    dados_solucoes = []
    for i in range(len(sols)):
        qtd_total, qtd_hired, qtd_fired, qtd_transf = return_qtd_ele(sols[i].elec_qtd)

        dados_solucoes.append([cod_modulo,i + 1, sols[i].compensation_cost, sols[i].displace_cost, len(sols[i].installed_bases),
                               sols[i].base_install_costs, len(sols[i].closed_bases), sols[i].base_closing_costs,
                               len(sols[i].maintained_bases), sols[i].base_maintenance_costs, qtd_total,
                               sols[i].crew_costs,
                               qtd_hired, sols[i].hiring_costs, qtd_transf, sols[i].transfer_costs, qtd_fired,
                               sols[i].firing_costs, sols[i].payback])

    dados_solucoes = np.array(dados_solucoes)
    print("/nlocation.py, linha 1415, dados_solucoes: ",dados_solucoes)   # Remover - RL
    df = pd.DataFrame(columns=['IDT_PRMET_PRCSM_MODUL_ETTGC','NUM_CENARIO', 'VLR_TOTAL_CUSTO_COMPENSACAO', 'VLR_TOTAL_CUSTO_DESLOCAMENTO',
                               'QTD_ABERTURA_BASE', 'VLR_TOTAL_CUSTO_ABERTURA_BASE', 'QTD_FECHAMENTO_BASE',
                               'VLR_TOTAL_CUSTO_FECTO_BASE', 'QTD_MANUTENCAO_BASE',
                               'VLR_TOTAL_CUSTO_MANUT_BASE', 'QTD_MANUTENCAO_ELETRICISTA',
                               'VLR_TOTAL_CUSTO_MANUT_ELTCT', 'QTD_CONTRATACAO_ELETRICISTA',
                               'VLR_TOTAL_CUSTO_CTRTC_ELTCT', 'QTD_TRANSFERENCIA_ELETRICISTA',
                               'VLR_TOTAL_CUSTO_TRFRC_ELTCT', 'QTD_DEMISSAO_ELETRICISTA',
                               'VLR_TOTAL_CUSTO_DEMISSAO_ELTCT', 'VLR_RETORNO_INVESTIMENTO'], data=dados_solucoes,dtype='float64')

    return df
def get_saida_atendimento(locs_bases_data, loc_times, sols, B,cod_modulo):
    # transformando colunas tipo string para int
    for col in loc_times.columns:
        loc_times.rename(columns={col: int(col)}, inplace=True)

    df = pd.DataFrame()  # gera dataframe de saida
    for i in range(len(sols)):
        cod_bases = sols[i].sugested_bases
        atendimento_loc_dic = {}  # esse dic salva quais localidades cada base atende.
        for cod in loc_times:
            cod_loc_atendente = loc_times.loc[cod, cod_bases].idxmin()  # pega cod da localidade q atendeu a localidade 'cod'
            if cod_loc_atendente not in atendimento_loc_dic:
                atendimento_loc_dic[cod_loc_atendente] = [cod]
            else:
                atendimento_loc_dic[cod_loc_atendente] += [cod]

        for cod_loc_atendente in atendimento_loc_dic:
            for cod in atendimento_loc_dic[cod_loc_atendente]:
                data = pd.DataFrame(
                    {'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_LOCALIDADE_BASE': [cod_loc_atendente], 'COD_LOCALIDADE_CLIENTE': [cod],
                     'IDT_PERFIL_BASE_OPERACIONAL': [B[0]]})
                df = df.append(data)
    return df
def get_bases_configuration(sols, B,cod_modulo):
    df = pd.DataFrame()
    for i in range(len(sols)):

        installed = sols[i].installed_bases
        closed = sols[i].closed_bases
        maintained = sols[i].maintained_bases

        for cod in installed:
            data = pd.DataFrame(
                {'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_LOCALIDADE_VIRTUAL': [cod], 'IDT_PERFIL_BASE_OPERACIONAL': [B[0]], 'IND_ACAO_BASE': ['I']})
            df = df.append(data)

        for cod in closed:
            data = pd.DataFrame(
                {'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_LOCALIDADE_VIRTUAL': [cod], 'IDT_PERFIL_BASE_OPERACIONAL': [B[0]], 'IND_ACAO_BASE': ['D']})
            df = df.append(data)

        for cod in maintained:
            data = pd.DataFrame(
                {'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_LOCALIDADE_VIRTUAL': [cod], 'IDT_PERFIL_BASE_OPERACIONAL': [B[0]], 'IND_ACAO_BASE': ['M']})
            df = df.append(data)

    return df
def get_electricians_out(sols,cod_modulo):
    df = pd.DataFrame()
    for i in range(len(sols)):

        qtd = sols[i].elec_qtd
        costs = sols[i].electricians_costs

        for cod in qtd:
            to = qtd[cod][0]
            c = qtd[cod][1]
            d = qtd[cod][2]
            t = qtd[cod][3]
            cm = costs[cod][3]
            cc = costs[cod][0]
            ct = costs[cod][2]
            cd = costs[cod][1]

            data = pd.DataFrame({'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_PERFIL_ELETRICISTA': [cod], 'QTD_MANUTENCAO_ELETRICISTA': [to],
                                 'VLR_CUSTO_TOTAL_MANUT_ELTCT': [cm], 'QTD_CONTRATACAO_ELETRICISTA': [c],
                                 'VLR_CUSTO_TOTAL_CTRTC_ELTCT': [cc], 'QTD_TRANSFERENCIA_ELETRICISTA': [t],
                                 'VLR_CUSTO_TOTAL_TRFRC_ELTCT': [ct], 'QTD_DEMISSAO_ELETRICISTA': [d],
                                 'VLR_CUSTO_TOTAL_DEMISSAO_ELTCT': [cd]})
            df = df.append(data)

    return df
def get_electricians_locality(sols,r,L,cod_modulo):
    df = pd.DataFrame()
    for i in range(len(sols)):
        dic = sols[i].elec_by_base
        for cod_base in dic:
            for cod_processo in dic[cod_base]:
                qtd = dic[cod_base][cod_processo]
                qtd_e = r[cod_base].get(cod_processo, 0)
                data = pd.DataFrame(
                    {'IDT_PRMET_PRCSM_MODUL_ETTGC':[cod_modulo],'NUM_CENARIO': [i + 1], 'COD_LOCALIDADE_VIRTUAL': [cod_base], 'COD_PERFIL_ELETRICISTA': [cod_processo],
                     'QTD_SUGESTAO_ELETRICISTA': [qtd], 'QTD_ANTERIOR_ELETRICISTA': [qtd_e]})
                df = df.append(data)
    return df