from pulp import *
import pandas as pd
from time import time as seconds
import precalc
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

class Scheduling:
    pd.options.mode.chained_assignment = None  # default='warn'

    def __init__(self, data, cod_modulo, cod_estudo):
        self.data = data
        self.sol = []
        self.sol = []
        self.cod_modulo = cod_modulo
        self.cod_estudo = cod_estudo
        self.runtime = 0.0
        self.elect_by_sched = pd.DataFrame(columns=['IDT_PRMET_PRCSM_TATIC_OPERL','COD_ESCALA','COD_LOCALIDADE_VIRTUAL','COD_PERFIL_ELETRICISTA',
                                                    'QTD_SUGESTAO_ELETRICISTA','VLR_TOTAL_CUSTO_ELETRICISTA'])
        self.gen_output = pd.DataFrame(columns=['IDT_PRMET_PRCSM_TATIC_OPERL','COD_PERFIL_ELETRICISTA', 'QTD_SUGESTAO_ELETRICISTA','VLR_CUSTO_TOTAL_MANUT_ELTCT',
                                                'QTD_CONTRATACAO_ELETRICISTA', 'VLR_CUSTO_TOTAL_CTRTC_ELTCT','QTD_TRANSFERENCIA_ELETRICISTA', 'VLR_CUSTO_TOTAL_TRFRC_ELTCT',
                                                'QTD_DEMISSAO_ELETRICISTA','VLR_CUSTO_TOTAL_DEMISSAO_ELTCT'])
        self.base_output = pd.DataFrame(columns=['IDT_PRMET_PRCSM_TATIC_OPERL', 'COD_LOCALIDADE_VIRTUAL','COD_PERFIL_ELETRICISTA',' QTD_SUGESTAO_ELETRICISTA',
                                                'VLR_CUSTO_TOTAL_MANUT_ELTCT', 'QTD_CONTRATACAO_ELETRICISTA','VLR_CUSTO_TOTAL_CTRTC_ELTCT','QTD_DEMISSAO_ELETRICISTA',
                                                'VLR_CUSTO_TOTAL_DEMISSAO_ELTCT', 'PCT_OCIOSIDADE'])
        self.plots_output = pd.DataFrame(columns=['IDT_PRMET_PRCSM_TATIC_OPERL','COD_LOCALIDADE_VIRTUAL','IDT_MES_DEMANDA','COD_PERFIL_ELETRICISTA',
                                                 'IDT_INDICADOR_RETORNO_DEMANDA','IDT_HORA_RETORNO_DEMANDA','VLR_RETORNO_DEMANDA'])
        self.status_output = pd.DataFrame(columns=['IND_SITUACAO_PROCESSAMENTO','DSC_MENSAGEM_SITUACAO_PRCSM','DTH_INCLUSAO_RETORNO', 'CODIGO_DO_ESTUDO'])

    def run(self, sizing=True):
        ######################## Inicializar planilha de status da execução
        date = datetime.now()
        date = date.strftime('%y/%m/%d %H:%M:%S')
        self.status_output.loc[0] = [2, 'Processado', date, int(self.cod_estudo)]

        ####################### INICIO - Obter dados usados explicitamente em ambos os módulos
        E = self.data.crew_formation['COD_PERFIL_EQUIPE'].unique() # indices de perfis de equipe
        P = self.data.electrician_profiles['COD_PERFIL_ELETRICISTA'].unique() # indices de perfis de eletricista
        L = self.data.locs_bases.index.values # indices de localidades

        r = get_qtd_eletricistas(self.data.electrician_per_loc, L) # acesso r[j].get(p, 0), local. j e perfil p
        l = get_qtd_elet_eq(self.data.crew_formation) # qtd elet. na equipe, acesso l[p,e] ou l.get((p,e),0) que retorna 0 se n tiver
        T, D, R = get_custos_contratacao_demissao(self.data.electricians_costs)  # acesso T[p] e D[p]
        ####################### FIM - Obter dados para ambos os módulos

        ####################### INICIO - Precalculo da produtividade
        print("\nCalculando constantes (modulo de bases) e gama (modulo dimensionamento)... ", end=" ", flush=True)

        # converte tipo das colunas de object para int. Acesso é self.data.loc_distances.at[i,j], onde i e j são inteiros
        self.data.loc_distances.columns = self.data.loc_distances.columns.astype(int)
        self.data.loc_times.columns = self.data.loc_times.columns.astype(int)

        # cria dicionarios pra tmd, tmp e tme
        d, tmd = d_and_times_calc(self.data.loc_distances, self.data.loc_times, L)
        tme = {s: row['TME'] for s, row in self.data.services.iterrows()}
        tmp = {s: row['TMP'] for s, row in self.data.services.iterrows()}

        # tem_saida = get_tem_saida(L, data.locs_bases)

        q = self.data.os_volume.groupby(['COD_LOCALIDADE']).sum().drop(['ANO', 'MES'], axis=1)
        rename_col = {}
        for k in q.columns:
            rename_col[k] = int(k.split(' ')[1].strip(" "))
        # acesso q[s][i], qtd de OS's s na localidade i
        q = q.rename(columns=rename_col).to_dict()

        print("\nPrecalculando constantes para execução do módulo tático...", end=" ", flush=True)
        try:
            self.data.misc_input.loc['CARREGA_PRECALC','VALOR'] == 'S'
            path_precalc = 'precalc/PRECALCULO_'+str(hash((int(self.data.cod_modulo),int(self.data.cod_estudo))))+'.csv'
            print('\nCarregando constantes já calculadas em '+path_precalc+'...', end=' ', flush=True)
            if os.path.exists(path_precalc):
                B = self.data.bases_profiles['COD_PERFIL_BASE'].unique() # indices de perfis de base
                gama = precalc.load(path_precalc, E, L, B, self.data.services, strategic=False) # carrega valores já existentes (muito mais rápido!)
            else:
                print('\nCaminho de precalculo '+path_precalc+' não existe! Calculando do zero...')
                gama = precalc.productivity(self.data, q, L, tmd, tmp, tme, E, P, l)
        except Exception as e:
            #print('\nERRO::::', e, end=' ', flush=True)
            gama = precalc.productivity(self.data, q, L, tmd, tmp, tme, E, P, l)
        
        ####################### FIM - Precalculo da produtividade

        print("\nObtendo dados especificos para execução do modelo de escala...", end=' ', flush=True)

        # informações sobre execução do modelo de escala - Planilha OUTRAS_ENTRADAS.csv
        backlog_perc = float(self.data.misc_input.loc['PERCENTUAL_BACKLOG','VALOR']) / 100.0    # percentual de backlog
        inic_hora_comercial = int(float(self.data.misc_input.loc['INICIO_HORA_COMERCIAL','VALOR']))# inicio de horario comercial
        fim_hora_comercial = int(float(self.data.misc_input.loc['FIM_HORA_COMERCIAL','VALOR'])) # fim de horario comercial
        toda_base = 1 #int(float(self.data.misc_input.loc['EXEC_COMPLETA_BASES','VALOR'])) # 1 - a execução é para todas as bases; 0 - caso contrário e roda para a base dada em local_cod_exec

        # mes_demanda = int(self.data.misc_input.loc['MES_DEMANDA','VALOR'])
        meses = [int(float(self.data.misc_input.loc['MES_DEMANDA','VALOR']))]
        #meses = self.data.misc_input.loc['MES_DEMANDA','VALOR']
        #meses = list(map(int, meses.split(',')))

        # Indices de horas da semana
        T_sched = [i for i in range(24 * 7)]
        TC = [i for i in range(24 * 7) if inic_hora_comercial <=
              i % 24 <= fim_hora_comercial]

        # Obtem os códigos das bases
        if toda_base == 1: # Rodar para todas as bases
            depot_set = self.data.depot['COD_LOCALIDADE_BASE'].unique()
        else: # Rodar apenas para a base especificada na planilha
            #local_cod_exec = int(self.data.misc_input.loc['COD_LOCALIDADE_GARAGEM_EXEC','VALOR']) # base de execução do modelo
            local_cod_exec = self.data.misc_input.loc['COD_LOCALIDADE_GARAGEM_EXEC','VALOR']
            depot_set = list(map(int, local_cod_exec.split(',')))
            # depot_set = local_cod_exe
        
        if self.data.depot.empty or len(depot_set) == 0:
            msg_status = 'Não foram identificadas base na entrada'                
            return 3, msg_status, None

        # Identifica os códigos dos perfis de eletricistas terceirizadas
        Terc = []
        rows = self.data.electrician_profiles.iterrows()
        for cod_perf, row in rows:
            string = row['DESC_PERFIL_ELETRICISTA']
            if "terceiro" in string.lower():
                Terc.append(row['COD_PERFIL_ELETRICISTA'])

        resume_results_base, resume_results_plots = pd.DataFrame(), pd.DataFrame()  # Armazena os resultados de todas as bases
        elect_by_sched_aux = self.elect_by_sched.copy() # Dataframe auxiliar
        print("\nExecutando modelo de dimensionamento e escala... ", flush=True)

        for mes_demanda in meses:
            # Resgata demanda e escalas para o mês de execução
            try:
                demand_file = self.data.os_hour.loc[self.data.os_hour['MES'] == mes_demanda] # Demanda do mês especificado
            except:
                msg_status = 'Erro ao acessar demanda para o mês ' + str(mes_demanda)                
                return 3, msg_status, None

            try:
                if mes_demanda != 0: # Demanda da SEMANA_TIPICA_MEDIA usa escalas do mês de Janeiro
                    schedule_data = self.data.schedule.loc[self.data.schedule['MES'] == mes_demanda]
                else:
                    schedule_data = self.data.schedule.loc[self.data.schedule['MES'] == 1]
            except:
                msg_status = 'Erro ao acessar escalas definidas para o mês ' + str(mes_demanda)                
                return 3, msg_status, None

            if demand_file.empty or schedule_data.empty:
                msg_status = 'Não existe demanda ou escalas definidas para o mês ' + str(mes_demanda)                
                return 3, msg_status, None
            
            # Resgata códigos das escalas
            S = schedule_data['COD_ESCALA'].unique()  # todas as escalas
            SP = schedule_data.loc[schedule_data['TIPO'] == 'P', 'COD_ESCALA'].unique()  # escalas proprio
            ST = schedule_data.loc[schedule_data['TIPO'] == 'T', 'COD_ESCALA'].unique()  # escalas terceiro

            # Obtem participacao semanal da escala a[s][t] e o custo c[j][s]
            a_sched, c = get_a_sched(schedule_data, S, T_sched, self.data.electricians_costs, P, inic_hora_comercial, fim_hora_comercial)
            g = {s: schedule_data.loc[schedule_data['COD_ESCALA'] == s, 'GRUPO_TRABALHO'].unique()[0] for s in S}
            
            print('done!')
            print("Mes:", mes_demanda, flush=True)

            qntdade_total_eletricistas = 0  # Contabiliza a quantidade total de eletricistas
            #print(gama)   # Originalmente descomentado - RL
            print('\nObter dados especificos para cada base e executar modelo de dimensionamento')
            demand_control = 0 # checador de demanda, se igual a len(depot_set) não existe demanda para nenhuma base selecionada
            for depot in depot_set:                

                depot_customers = self.data.depot.loc[self.data.depot['COD_LOCALIDADE_BASE'] 
                                                 == depot, 'COD_LOCALIDADE_CLIENTE'].values.tolist() # Obtem clientes da base (depot)

                rb = get_qtd_eletricistas_base(self.data.electrician_per_loc, depot, depot_customers)
                
                print('\nRemover localidades sem demanda cadastrada (evitar erros)',end=' ', flush=True)
                locs_aux = set(demand_file.index.values.tolist())
                depot_customers = list(set(depot_customers).intersection(locs_aux))
                
                if len(depot_set) >= 1 and len(depot_customers) == 0:
                    demand_control += 1
                    if len(depot_set) == demand_control:
                        msg_status = 'Não há demanda cadastrada para as garagens selecionadas'
                        print(msg_status)
                        return 3, msg_status, None
                    continue

                print('\nObtem produtividade de atendimento na base e suas localilades clientes',end=' ', flush=True)
                gama_loc = get_gama_sched_local(depot, depot_customers, gama, self.data.locs_bases, self.data.bases_profiles, self.data.crew_service)
                
                print('\nObtem demada da base',end=' ', flush=True)
                try:
                    d_sched, d_backlog, d_emergecial_hhora, d_total_hhora, hh_backlog_reg, hh_backlog_nreg = get_week_demand_sched(
                    demand_file, self.data.services, depot_customers, gama_loc, E, P, l, TC)  # Obtem demanda semanal
                except:
                    msg_status = 'Erro ao acessar demanda da garagem ' + str(depot)
                    print(msg_status)
                    return 3, msg_status, None

                print('\nObtem o minimo de backlog a ser atendido',end=' ', flush=True)
                b_min = {}
                for bl in depot_customers:
                    b_min[bl] = backlog_perc * d_backlog[bl]

                print('\nObtem todos os servicos do departamento',end=' ', flush=True)
                O = [s for s in self.data.services.index.values.tolist()]

                print('\nResolve o modelo e retorna as soluções',end=' ', flush=True)
                try:
                    model = SchedModel(J=P, S=S, SP=SP, ST=ST, T=T_sched, TC=TC, E=E, O=O, a=a_sched, q=l, d=d_sched, b_min=b_min,
                    c=c, gama=gama_loc, r=rb[depot], C=T, D=D, g=g, L=depot_customers, Terc=Terc, ced=sizing)
                    status_solution, eletri_disp_emerg, eletri_folga, teams_form, demanda_hh_em, escalas_info, resume_results, qntdade_eletricistas = model.solve()
                except:
                    msg_status = 'Erro na resolução do modelo para a garagem ' + str(depot)
                    print(msg_status)
                    return 3, msg_status, None

                if status_solution == "Optimal" or status_solution == "Not Solved":
                    elect_by_sched_aux = elect_by_sched_aux.iloc[0:0]
                    for index, row in escalas_info.iterrows():
                        elect_by_sched_aux.loc[index] = [ self.cod_modulo, row['Cod Escala'], depot, row['Tipo'], row['N. Eletricistas'], row['Custo']]
                    self.elect_by_sched = self.elect_by_sched.append(elect_by_sched_aux, ignore_index=False)
                    
                    qntdade_total_eletricistas += qntdade_eletricistas

                    resume_tranposed = resume_results.T
                    new_header = resume_tranposed.iloc[0] #the first row for the header
                    resume_tranposed = resume_tranposed[1:] #take the data less the header row
                    resume_tranposed.columns = new_header #set the header row as the df header
                    resume_tranposed['Base'] = depot #insert location code in the row

                    print('\nPrinta as informações sobre a base',end=' ', flush=True)
                    try:
                        #Info_base = print_info_base(self.data.locs_bases, depot, depot_customers, b_min, backlog_perc)
                        idle_perc, eletri_disp_total = idle_check(P, a_sched, S, T_sched, escalas_info, demanda_hh_em, hh_backlog_reg, hh_backlog_nreg, backlog_perc)
                        #backlog_atend = backlog_check(hh_backlog_reg, hh_backlog_nreg, eletri_folga, backlog_perc)
                        resume_tranposed['Ociosidade'] = idle_perc
                        resume_tranposed['Mes'] = mes_demanda
                        # print_data(Info_base, escalas_info, resume_results, idle_perc, backlog_atend)
                    except Exception as e:
                        #print('\nERRO:::: ', e)
                        msg_status = 'Erro na recuperação de solução para a garagem ' + str(depot) + '. Certifique-se que os dados de entrada estejam consistentes.'
                        print(msg_status)
                        return 3, msg_status, None
                        

                    print('\nPlotar graficos\n',end=' ', flush=True)
                    resume_plot = plot_graphs(depot, mes_demanda, P, eletri_disp_total, eletri_disp_emerg, demanda_hh_em, d_emergecial_hhora, hh_backlog_reg)

                    resume_results_base = resume_results_base.append(resume_tranposed, ignore_index=False)
                    resume_results_plots = resume_results_plots.append(resume_plot, ignore_index=False)
                else:
                    msg_status = 'Modelo não resolvido'
                    print(msg_status)
                    return 3, msg_status, None
                    
        print("\nPreparando dataframes para inserts...",end=' ', flush=True)
        if sizing:
            tab_quant = resume_results_base.loc[resume_results_base.index == 'Quantidade'].set_index('Base')
            tab_costs = resume_results_base.loc[resume_results_base.index == 'Custo'].set_index('Base')
            resume_all = get_main_information(P, meses, tab_costs, tab_quant, Terc, T, D, R, sizing) # resume information on a sigle sheet
                       
            count = 0
            for index, row in tab_quant.iterrows():
                if index == 'TOTAL':
                    continue
                for j in P:
                    self.base_output.loc[count] = [self.cod_modulo, index, j, row['Eletricistas ('+str(j)+')'], 0.0, row['Contratacoes ('+str(j)+')'], 0.0, row['Demissoes ('+str(j)+')'], 0.0, row['Ociosidade']]
                    count += 1
            
            count = 0
            for index, row in tab_costs.iterrows():
                if index == 'TOTAL':
                    continue
                for j in P:
                    self.base_output.loc[count, 'VLR_CUSTO_TOTAL_MANUT_ELTCT'] = row['Eletricistas ('+str(j)+')']
                    self.base_output.loc[count, 'VLR_CUSTO_TOTAL_CTRTC_ELTCT'] = row['Contratacoes ('+str(j)+')']
                    self.base_output.loc[count, 'VLR_CUSTO_TOTAL_DEMISSAO_ELTCT'] = row['Demissoes ('+str(j)+')']
                    count += 1
            
            count = 0
            for j in P:
                self.gen_output.loc[count] = [self.cod_modulo, j,
                                                resume_all.loc['Quantidade', 'Eletricistas ('+str(j)+')'],
                                                resume_all.loc['Custo', 'Eletricistas ('+str(j)+')'],
                                                resume_all.loc['Quantidade', 'Contratacoes ('+str(j)+')'],
                                                resume_all.loc['Custo', 'Contratacoes ('+str(j)+')'],
                                                resume_all.loc['Quantidade', 'Transferencias ('+str(j)+')'],
                                                resume_all.loc['Custo', 'Transferencias ('+str(j)+')'],
                                                resume_all.loc['Quantidade', 'Demissoes ('+str(j)+')'],
                                                resume_all.loc['Custo', 'Demissoes ('+str(j)+')']]
                count += 1
                        
            print("\ndone!")          

            # Gerando tabela de saida para plots dos gráficos
            resume_results_plots = resume_results_plots.replace({'Descricao':{'D. Regulada':1, 'D. Emergencial':2, 'Total':3, 'Emergencial':4}})
            rename_col = {'Base':'COD_LOCALIDADE_VIRTUAL','Mes':'IDT_MES_DEMANDA','Cod_perfil':'COD_PERFIL_ELETRICISTA',
                          'Descricao':'IDT_INDICADOR_RETORNO_DEMANDA','Hora':'IDT_HORA_RETORNO_DEMANDA','Valor':'VLR_RETORNO_DEMANDA'}
            resume_results_plots.insert(0, 'IDT_PRMET_PRCSM_TATIC_OPERL', self.cod_modulo)
            resume_results_plots = resume_results_plots.rename(columns=rename_col)
            self.plots_output = resume_results_plots                        
            return resume_all, tab_quant, tab_costs
        else:
            tab_quant = resume_results_base.loc[resume_results_base.index == 'Quantidade'].set_index('Base')
            tab_costs = resume_results_base.loc[resume_results_base.index == 'Custo'].set_index('Base')
            resume_all = get_main_information(P, meses, tab_costs, tab_quant, Terc, T, D, R, sizing) # resume information on a sigle sheet
 
            print("\ndone!")
        
            # Gerando tabela de saida para plots dos gráficos
            resume_results_plots = resume_results_plots.replace({'Descricao':{'D. Regulada':1, 'D. Emergencial':2, 'Total':3, 'Emergencial':4}})
            rename_col = {'Base':'COD_LOCALIDADE_VIRTUAL','Mes':'IDT_MES_DEMANDA','Cod_perfil':'COD_PERFIL_ELETRICISTA',
                          'Descricao':'IDT_INDICADOR_RETORNO_DEMANDA','Hora':'IDT_HORA_RETORNO_DEMANDA','Valor':'VLR_RETORNO_DEMANDA'}
            resume_results_plots.insert(0, 'IDT_PRMET_PRCSM_TATIC_OPERL', self.cod_modulo)
            resume_results_plots = resume_results_plots.rename(columns=rename_col)
            self.plots_output = resume_results_plots
            
            return None, None, None

class SchedModel:

    def __init__(self, J, S, SP, ST, T, TC, E, O, a, q, d, b_min, c, gama, r, C, D, g, L, Terc, ced=True):
        self.J = J
        self.S = S
        self.T = T
        self.TC = TC
        self.E = E
        self.O = O
        self.a = a
        self.q = q
        self.d = d
        self.b_min = b_min
        self.c = c
        self.gama = gama
        self.r = r
        self.C = C
        self.D = D
        self.g = g
        self.L = L
        self.Terc = Terc
        self.ced = ced
        self.SP = SP
        self.ST = ST
        # variables
        self.x = {}
        self.y = {}
        self.w = {}
        self.f = {}
        if ced:
            self.alpha = {}
            self.theta = {}
        else:
            self.v = {}
            self.z = {}

    def _create_variables(self):
        self.x = LpVariable.dicts('x', (self.J, self.S), lowBound=0, cat=LpInteger)
        self.y = LpVariable.dicts('y', (self.J, self.T), 0)

        self.w = LpVariable.dicts('w', (self.E, self.T, self.L), 0)
        self.f = LpVariable.dicts('f', (self.J, self.TC, self.L), 0)

        if self.ced:
            self.alpha = LpVariable.dicts('alpha', self.J, 0)
            self.theta = LpVariable.dicts('theta', self.J, 0)
        else:
            self.z = LpVariable.dicts('z', (self.E, self.T, self.L), 0)
            self.v = LpVariable.dicts('v', (self.J, self.TC, self.L), 0)

    def _get_objective_function_terms(self):
        oft = []

        # adiciona termos da primeira parcela da f.o.
        for j in self.J:
            for s in self.S:
                oft.append(self.c[j][s] * self.x[j][s])

        if self.ced:
            # adiciona termos da Terceira parcela da f.o.
            for j in self.J:
                oft.append(self.C[j] * self.alpha[j])

            # adiciona termos da quarta parcela da f.o.
            for j in self.J:
                #oft.append(self.D[j] * self.theta[j])
                cd = self.D[j]*0.001
                oft.append(cd * self.theta[j])
        else:
            # adiciona variáveis de "punição" para o caso de manter quantidade atual de eletricistas
            for e in self.E:
                for t in self.T:
                    for l in self.L:
                        oft.append(self.z[e][t][l] * 99999)
            for j in self.J:
                for t in self.TC:
                    for l in self.L:
                        oft.append(self.v[j][t][l] * 9999)

        return oft

    def _add_constraints(self, problem):
        # constraint 1
        for j in self.J:

            Escalas = self.S
            if j in self.Terc:
                Escalas = self.ST #Escalas para terceiros
            else:
                Escalas = self.SP #Escalas para proprios

            for t in self.TC:
                terms = []

                for s in Escalas:
                    terms.append(self.a[s][t] * self.x[j][s])

                terms2 = []
                for l in self.L:
                    terms2.append(self.f[j][t][l])

                problem += lpSum(terms) >= (self.y[j][t] + lpSum(terms2))#, "Constraint 1; j = " + str(
                    #j) + "; t = " + str(t)

        # constraint 2

        TNC = set(self.T) - set(self.TC)
        for j in self.J:

            Escalas = self.S
            if j in self.Terc:
                Escalas = self.ST #Escalas para terceiros
            else:
                Escalas = self.SP #Escalas para proprios

            for t in TNC:
                terms = []

                for s in Escalas:
                    terms.append(self.a[s][t] * self.x[j][s])
                problem += lpSum(terms) >= self.y[j][t] #, "Constraint 2; j = " + str(j) + "; t = " + str(t)

        # constraint 3

        for j in self.J:
            for t in self.T:
                terms = []
                for e in self.E:
                    for l in self.L:
                        terms.append(self.q.get((j, e), 0) * self.w[e][t][l])
                problem += lpSum(terms) <= self.y[j][t]#, "Constraint 3; j = " + str(j) + "; t = " + str(t)

        # constraint 4

        for o in self.O:
            for t in self.T:
                for l in self.L:
                    terms = []
                    for e in self.E:
                        if self.gama.get(e, {o: 0}).get(o, {l: 0}).get(l, 0) != float('inf'):
                            if self.ced:
                                terms.append(self.gama.get(e, {o: 0}).get(o, {l: 0}).get(l, 0) * self.w[e][t][l])
                            else:
                                terms.append(self.gama.get(e, {o: 0}).get(o, {l: 0}).get(l, 0) * (self.w[e][t][l] + self.z[e][t][l]))

                    if terms:
                        problem += lpSum(terms) >= self.d.get(l, {o: 0}).get(o, {t: 0})[t]#, "Constraint 4; o = " + str(o) + "; t = " + str(t) + "; l = " + str(l)

        # constraint 5
        # Restrições para veículos deletada

        # constraint 6

        for l in self.L:
            terms = []
            for j in self.J:
                for t in self.TC:
                    if self.ced:
                        terms.append(self.f[j][t][l])
                    else:
                        terms.append(self.f[j][t][l] + self.v[j][t][l])
            problem += lpSum(terms) >= self.b_min.get(l, 0)#, "Constraint 6; l = " + str(l)

        if self.ced:
            # constraints 7 and 9
            for j in self.J:
                terms_x = []
                for s in self.S:
                    terms_x.append(self.g[s] * self.x[j][s])
                problem += self.alpha[j] >= (lpSum(terms_x) - self.r.get(j, 0))#, "Constraint 7; j = " + str(j)
                problem += self.theta[j] >= (self.r.get(j, 0) - lpSum(terms_x))#, "Constraint 9; j = " + str(j)

        else: # mantem quantidade atual de eletricistas/
            for j in self.J:
                min_aux = 9999999999
                terms_x = []
                for s in self.S:
                    min_aux = min(min_aux, self.g[s])
                    terms_x.append(self.g[s] * self.x[j][s])
                if self.r.get(j, 0) == 0:
                    problem += lpSum(terms_x) == self.r.get(j, 0) #, "Use current number of electrician"
                else:
                    problem += lpSum(terms_x) == max(min_aux, self.r.get(j, 0))

    def solve(self):
        self._create_variables()
        of_terms = self._get_objective_function_terms()

        problem = LpProblem("Crew Scheduling", LpMinimize)
        problem += lpSum(of_terms), "Total Cost"
        self._add_constraints(problem)

        problem.writeLP("CrewScheduling.lp")

        time = seconds()
        problem.solve(COIN(msg=0, maxSeconds=120, fracGap=0.01))
        time = seconds() - time

        status = LpStatus[problem.status]

        if status == "Optimal" or status == "Not Solved":            
            schedules = set()
            escalas_info, resume_results, qntdade_total_eletricistas = self._get_general_results(schedules)
            eletricistas_hh_em, eletricistas_hh_ne, equipes_formadas, demanda_hh_em = self._get_results_by_hour()

            #Verifica se algum instante de tempo ficou sem atendimento por falta de escala definidas pra aquele horário

            schedules_times = set()
            for s in schedules:
                for t in self.T:
                    if self.a[s][t] > 0.0:
                        schedules_times.add(t)

            # for l, os in self.d.items():
            #     for o, do in os.items():
            #         for t, dot in do.items():
            #             if dot > 0 and t not in schedules_times:
            #                 print('tempo ' + str(t) + ' tem demanda ' + str(dot) + ' para a os ' + str(
            #                     o) + ' na localidade ' + str(l) + ', mas nao ha escala que cubra esse horario.', flush=True)

        return status, eletricistas_hh_em, eletricistas_hh_ne, equipes_formadas, demanda_hh_em, escalas_info, resume_results, qntdade_total_eletricistas

    def _get_general_results(self, schedules):

        #Regata resultados gerais sobre quantidade de eletricistas em escalas e custos

        #Armazena resultados sobre as escalas utilizadas
        escalas_info = pd.DataFrame(columns=['Cod Escala','Coletiva','N. Eletricistas','Tipo','Custo'])

        #Armazena o resumo dos resultados, considerando contratações e demissões
        resume_results = pd.DataFrame(columns=['1','Quantidade','Custo'])

        custo_total = 0.0
        qntdade_total_eletricistas = 0

        #Resgata resultados sobre escalas
        for j in self.J:
            qntdade = 0
            custo = 0.0
            custo_escala = 0.0
            for s in self.S:
                var_value = self.x[j][s].varValue
                if var_value > 0.1:
                    n = str(int(var_value))
                    escala_status = 'N'
                    n_eletricistas = n
                    if self.g[s] > 1:
                        escala_status = 'S'                        
                        n_eletricistas = str(int(n) * self.g[s])
                        custo = self.c[j][s] * int(n)
                        custo_escala += custo
                    else:
                        custo = self.c[j][s] * int(n)
                        custo_escala += custo

                    schedules.add(s)
                    qntdade += int(n) * self.g[s]

                    escalas_info = escalas_info.append({'Cod Escala' : s, 'Coletiva' : escala_status, 'N. Eletricistas' : n_eletricistas, 'Tipo' : j, 'Custo' : custo} , ignore_index=True)

            custo_total += custo_escala
            qntdade_total_eletricistas += qntdade
            aux = 'Eletricistas (' + str(j) + ')'
            resume_results = resume_results.append({'1' : aux , 'Quantidade' : qntdade, 'Custo' : custo_escala}, ignore_index=True)

        # Resgata resultados sobre contratações
        if self.ced:
            for j in self.J:

                qntdade = int(self.alpha[j].varValue)
                custo = qntdade * self.C[j]
                custo_total += custo

                aux = 'Contratacoes (' + str(j) + ')'
                resume_results = resume_results.append({'1' : aux , 'Quantidade' : qntdade, 'Custo' : custo}, ignore_index=True)

            #Resgata resultados sobre demissões
            for j in self.J:

                qntdade = int(self.theta[j].varValue)
                custo = qntdade * self.D[j]
                custo_total += custo

                aux = 'Demissoes (' + str(j) + ')'
                resume_results = resume_results.append({'1' : aux , 'Quantidade' : qntdade, 'Custo' : custo}, ignore_index=True)

        #Resgata resultados do custo total
        resume_results = resume_results.append({'1' : 'Total' , 'Quantidade' : qntdade_total_eletricistas, 'Custo' : custo_total}, ignore_index=True)
        
        return escalas_info, resume_results, qntdade_total_eletricistas

    def _get_results_by_hour(self):

        #Resgata Homem-Hora em OS emergenciais e não emergenciais
        eletricistas_hh_em = {}
        eletricistas_hh_ne = {}

        for j in self.J:
            eletricistas_hh_ne[j] = {}
            eletricistas_hh_em[j] = {}
            for t in self.T:
                eletricistas_hh_em[j][t] = self.y[j][t].varValue
                if t in self.TC:
                    for l in self.L:
                        if t in eletricistas_hh_ne[j]:
                            eletricistas_hh_ne[j][t] += self.f[j][t][l].varValue
                        else:
                            eletricistas_hh_ne[j][t] = self.f[j][t][l].varValue
                else:
                    eletricistas_hh_ne[j][t] = 0


        #Resgata quantas OS emergenciais foram atendidas por hora
        #Resgata a quantidade de equipes formadas por hora pra atender as OSs emergenciais

        demanda_hh_em = {}
        equipes_formadas = {}

        for e in self.E:
            equipes_formadas[e] = {}
            demanda_hh_em[e] = {}
            for t in self.T:
                for l in self.L:
                    var_value = self.w[e][t][l].varValue

                    if t in equipes_formadas[e]:
                        equipes_formadas[e][t] += var_value
                    else:
                        equipes_formadas[e][t] = var_value

                    nEquipe = 0
                    for j in self.J:
                        if self.q.get((j, e), 0) > 0:
                            nEquipe = self.q.get((j, e), 0)

                    if t in demanda_hh_em[e]:
                        demanda_hh_em[e][t] += nEquipe * var_value
                    else:
                        demanda_hh_em[e][t] = nEquipe * var_value

        #Converte dados para tabelas indexadas pela hora semanal

        eletricistas_hh_em = pd.DataFrame.from_dict(eletricistas_hh_em)
        eletricistas_hh_em = eletricistas_hh_em.rename_axis('Hora')

        eletricistas_hh_ne = pd.DataFrame.from_dict(eletricistas_hh_ne)
        eletricistas_hh_ne = eletricistas_hh_ne.rename_axis('Hora')

        demanda_hh_em = pd.DataFrame.from_dict(demanda_hh_em)
        demanda_hh_em = pd.DataFrame(demanda_hh_em.sum(skipna=True, axis=1), columns=['D. Emergencial']).rename_axis('Hora')

        equipes_formadas = pd.DataFrame.from_dict(equipes_formadas)
        equipes_formadas = pd.DataFrame(equipes_formadas.sum(skipna=True, axis=1), columns=['Equipes']).rename_axis('Hora')

        return eletricistas_hh_em, eletricistas_hh_ne, equipes_formadas, demanda_hh_em

def get_custos_contratacao_demissao(electrician_costs_data):
    T = {cod_proc: row['CUSTO_CONTRATACAO']
         for cod_proc, row in electrician_costs_data.iterrows()}
    D = {cod_proc: row['CUSTO_DEMISSAO']
         for cod_proc, row in electrician_costs_data.iterrows()}
    R = {cod_proc: row['CUSTO_TRANSFERENCIA']
         for cod_proc, row in electrician_costs_data.iterrows()}
    return T, D, R

def get_qtd_eletricistas(electricians_per_location_data, L):
    r = {l: {} for l in L}
    for cod_loc, row in electricians_per_location_data.iterrows():
        if cod_loc in r:
            r[cod_loc][row['COD_PERFIL_ELETRICISTA']] = row['QTD_ELETRICISTAS']
        else:
            print("Localidade ", cod_loc, " não existe em L!")
            break
    return r

def get_qtd_eletricistas_base(electricians_per_location_data, depot, depot_customers):
    r = {depot: {}}
    for cod_loc, row in electricians_per_location_data.iterrows():
        if cod_loc in depot_customers:
            if not row['COD_PERFIL_ELETRICISTA'] in r[depot]:
                r[depot][row['COD_PERFIL_ELETRICISTA']] = row['QTD_ELETRICISTAS']
            else:
                r[depot][row['COD_PERFIL_ELETRICISTA']] += row['QTD_ELETRICISTAS']

    return r

def get_qtd_elet_eq(crew_formation_data):
    rows = crew_formation_data.iterrows()
    l = {}
    for cod_eq, row in rows:
        l[row['COD_PERFIL_ELETRICISTA'], row['COD_PERFIL_EQUIPE']] = row['QTD_ELETRICISTAS']
    return l

def d_and_times_calc(distances_data, times_data, L):
    d = {i: {j: distances_data.at[i, j] for j in L} for i in L}
    times = {i: {j: times_data.at[i, j] for j in L} for i in L}

    return d, times

def get_a_sched(schedule_data, S, T, electrician_costs_data, J, inic_hh_comercial, fim_hh_comercial):
    a = {s: {t: 0 for t in T} for s in S}
    days_init_t = {'SEG': 0, 'TER': 24, 'QUA': 48,
                   'QUI': 72, 'SEX': 96, 'SAB': 120, 'DOM': 144}
    t, cur_sched, prev_day_id = 0, -1, 0
    freq_t = {s: {t: 0 for t in T} for s in S}
    total_hh = {s: 0 for s in S}  # numero de horas da escala no mes
    total_hh_noturno = {s: 0 for s in S}  # numero de horas da escala no mes
    total_hh_fds = {s: 0 for s in S}  # numero de horas da escala no mes
    # numero de horas da escala no mes
    total_hh_fds_noturno = {s: 0 for s in S}
    for l, row in schedule_data.iterrows():

        if row['COD_ESCALA'] != cur_sched:
            cur_sched = row['COD_ESCALA']
        t = days_init_t[row['DIA_SEMANA']]
        t_init = t

        for i in range(24):
            if row[str(i)].upper() == 'X':
                a[row['COD_ESCALA']][t] += int(row['ELETRICISTAS_POR_HORA'])
                if t >= 120:
                    if (t - t_init) >= inic_hh_comercial and (t - t_init) < fim_hh_comercial:
                        total_hh_fds[row['COD_ESCALA']] += int(row['ELETRICISTAS_POR_HORA'])
                    else:
                        total_hh_fds_noturno[row['COD_ESCALA']] += int(row['ELETRICISTAS_POR_HORA'])
                else:
                    if (t - t_init) >= inic_hh_comercial and (t - t_init) < fim_hh_comercial:
                        total_hh[row['COD_ESCALA']] += int(row['ELETRICISTAS_POR_HORA'])
                    else:
                        total_hh_noturno[row['COD_ESCALA']] += int(row['ELETRICISTAS_POR_HORA'])
            freq_t[row['COD_ESCALA']][t] += 1
            t += 1

    for s, a_s in a.items():
        for t, a_st in a_s.items():
            a[s][t] = a_st / freq_t[s][t]

    c = {j: {s: electrician_costs_data.at[j, 'CUSTO_HH'] * total_hh[s] +
             electrician_costs_data.at[j, 'CUSTO_HH_NOTURNO'] * total_hh_noturno[s] +
             electrician_costs_data.at[j, 'CUSTO_HH_FDS'] * total_hh_fds[s] +
             electrician_costs_data.at[j, 'CUSTO_HH_FDS_NOTURNO'] * total_hh_fds_noturno[s] for s in S} for j in J}
    
    return a, c

def get_week_demand_sched(demand_file, services_data, location_set, gama, E, P, l, TC):
      
    no_emergencial_oss_ids = set()
    emergencial_oss_ids = set()
    unregulated_oss_ids = set()
    regulated_oss_ids = set()
    for os_id, row in services_data.iterrows():
        if row['TIPO_OCORRENCIA'] != 'E':
            no_emergencial_oss_ids.add(os_id)
        if row['TIPO_OCORRENCIA'] == 'E':
            emergencial_oss_ids.add(os_id)
        if row['IND_OS_REGULADA'] == 'S' and row['TIPO_OCORRENCIA'] != 'E':
            regulated_oss_ids.add(os_id)
        if row['IND_OS_REGULADA'] == 'N' and row['TIPO_OCORRENCIA'] != 'E':
            unregulated_oss_ids.add(os_id)

    total_oss_ids = no_emergencial_oss_ids | emergencial_oss_ids
        
    total_hh_backlog = {}
    hh_backlog_reg = {}
    hh_backlog_nreg = {}

    d = {}
    d_total_hhora = {}
    d_emergecial = {}
    d_emergecial_hhora = {}

    for o in emergencial_oss_ids:
        d_emergecial[o] = {}
        d_emergecial_hhora[o] = {}
    for o in total_oss_ids:
        d_total_hhora[o] = {}
        
    ##########
    # Para acessar demanda horária corretamente de acordo com o novo modelo da tabela de demanda horaria feita em jan/2020
    time_instant = 0
    key = {}
    for i in sorted(demand_file['DIA_DA_SEMANA'].unique()):
        for j in demand_file['HORA'].unique():
            key[time_instant] = (i,j)
            time_instant = time_instant+1
    ##########

    for location in location_set:
        demand = demand_file.loc[location]
        week_demand = demand_by_mean_week(demand)
        
        #week_demand.to_csv('DEMANDA_SEMANNAL.csv',sep=';')
        
        d[location] = {}
        for o in emergencial_oss_ids:
            d[location][o] = {}
            for t in range(0,time_instant):# Calculo da demanda semanal total de OS's emergenciais por localidade
                # Demanda de OS's emergenciais por localidade atendida
                d[location][o][t] = week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA']

        # homem-hora de eletricistas para atender o backlog (nao emergencial) na localidade l
        total_hh_backlog[location] = 0.0
        hh_backlog_reg[location] = {}
        hh_backlog_nreg[location] = {}

        for o in total_oss_ids:  # Calculo da demanda semanal total em todas as localidades atendidas pela base
            for t in range(0,time_instant):
                productivity, nbElect = 0.0, 0
                for e in E:
                    if gama[e].get(o, {location: 0}).get(location, 0) > productivity:
                        productivity = gama[e].get(o, {location: 1}).get(location, 1)
                        nbElect = 0
                        for p in P:
                            nbElect += l.get((p, e), 0)

                # Demanda total em homem hora
                if t in d_total_hhora[o]:
                    d_total_hhora[o][t] += nbElect * \
                        (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)
                else:
                    d_total_hhora[o][t] = nbElect * \
                        (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)

                # Demanda emergencial em homem hora
                if o in d_emergecial_hhora:
                    if t in d_emergecial_hhora[o]:
                        d_emergecial_hhora[o][t] += nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)
                    else:
                        d_emergecial_hhora[o][t] = nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)

                # Demanda não emergencial em homem hora
                if o in no_emergencial_oss_ids:
                    total_hh_backlog[location] += nbElect * \
                        (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)

                # Demanda não emergencial regulada em homem hora por hora e localidade
                if o in regulated_oss_ids:
                    if t in hh_backlog_reg[location]:
                        hh_backlog_reg[location][t] += nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)
                    else:
                        hh_backlog_reg[location][t] = nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)

                # Demanda não emergencial nao regulada em homem hora por hora e localidade
                if o in unregulated_oss_ids:
                    if t in hh_backlog_reg[location]:
                        hh_backlog_nreg[location][t] += nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)
                    else:
                        hh_backlog_nreg[location][t] = nbElect * \
                            (week_demand.loc[(key[t][0],key[t][1],o),'QTD_OS_DEMANDA'] / productivity)

    return d, total_hh_backlog, d_emergecial_hhora, d_total_hhora, hh_backlog_reg, hh_backlog_nreg

def demand_by_mean_week(demand):
    week_demand = demand.reset_index()
    week_demand = week_demand.groupby(['COD_LOCALIDADE', 'DIA_DA_SEMANA', 'HORA', 'IDT_BLOCO_SERVICO'], as_index=False).mean().drop(['DIA', 'ANO', 'MES'], axis=1)
    week_demand = week_demand.set_index(['DIA_DA_SEMANA', 'HORA', 'IDT_BLOCO_SERVICO']).drop(['COD_LOCALIDADE'], axis=1)

    return week_demand

def get_tem_saida(L, locs_bases_data):
    tem_saida = {}
    for j in L:
        tem_saida[j] = locs_bases_data.loc[j]['TEMPO_SAIDA']
    return tem_saida

def get_gama_sched_local(l, depot_customers, general_gama, locs_bases_data, bases_profiles_data, profile_service_data):
        
    base_profile = locs_bases_data.at[l, 'COD_PERFIL_BASE']
    #base_profile = 1
    base_teams = bases_profiles_data.loc[bases_profiles_data['COD_PERFIL_BASE'] == base_profile]

    teams_codes = base_teams['COD_PERFIL_EQUIPE'].tolist()

    team_services = profile_service_data.groupby(
        'COD_PERFIL_EQUIPE', sort=False)['COD_SERVICO']

    gama = {}
    for e in teams_codes:
        gama[e] = {}
        for o in team_services.get_group(e):
            gama[e][o] = {}
            for location in depot_customers:
                if general_gama[e][o][l][location] < 1:
                    gama[e][o][location] = 1
                else:
                    gama[e][o][location] = general_gama[e][o][l][location]

    return gama

def idle_check(P, a, S, T, escalas_info, demanda_hh_em, hh_backlog_reg, hh_backlog_nreg, backlog_perc):

    # J = escalas_info['Tipo'].unique()
    hh_disp = {j: {t: 0 for t in T} for j in P}

    for i, row in escalas_info.iterrows():
        s = row['Cod Escala']
        n = row['N. Eletricistas']
        j = row['Tipo']
        for t in T:
            hh_disp[j][t] = hh_disp[j][t] + (int(n) * a[s][t])

    #ren_col = {1: 'Sinergia P', 2: 'Sinergia T', 3: 'Corte P', 4: 'N. Tensão T', 5: 'R. Danos P'}
    hh_disp = pd.DataFrame(hh_disp)
    hh_disp = pd.DataFrame(hh_disp).rename_axis('Hora')#.rename(columns=ren_col)
    hh_disp_sum = hh_disp.sum(skipna=True, axis=1)
    hh_backlog_reg = pd.DataFrame(hh_backlog_reg)
    hh_backlog_reg = hh_backlog_reg.sum(skipna=True, axis=1)

    hh_backlog_nreg = pd.DataFrame(hh_backlog_nreg)
    hh_backlog_nreg = hh_backlog_nreg.sum(skipna=True, axis=1)

    hh_em_total = demanda_hh_em['D. Emergencial'].sum()
    hh_breg_total = hh_backlog_reg.sum() * backlog_perc
    hh_bnreg_total = hh_backlog_nreg.sum() * backlog_perc

    if hh_disp_sum.sum() <= 0:
        hh_disp_total = 100000
    else:
        hh_disp_total = hh_disp_sum.sum()

    idle_perc = 1 - ((hh_em_total + hh_breg_total + hh_bnreg_total) / hh_disp_total)
    return (idle_perc * 100), hh_disp

def backlog_check(total_hh_backlog_reg, total_hh_backlog_nreg, eletri_folga, backlog_perc):

    total_hh_backlog_reg = pd.DataFrame(total_hh_backlog_reg)
    total_hh_backlog_reg = total_hh_backlog_reg.sum(skipna=True, axis=1)

    total_hh_backlog_nreg = pd.DataFrame(total_hh_backlog_nreg)
    total_hh_backlog_nreg = total_hh_backlog_nreg.sum(skipna=True, axis=1)

    eletri_folga = eletri_folga.sum(skipna=True, axis=1)

    aux = [0, 24, 48, 72, 96, 120, 144]
    aux2 = [0, 18, 42, 66, 90, 114, 138, 162]
    backlog_compare = pd.DataFrame(
        index=aux, columns=['Regulado', 'Nao Regul', 'Total', 'Alocado'])

    for t in aux:
        backlog_compare.at[t, 'Total'] = 0.0
        backlog_compare.at[t, 'Regulado'] = 0.0
        backlog_compare.at[t, 'Nao Regul'] = 0.0
        for td in range(t, t + 24):
            backlog_compare.at[t, 'Regulado'] += (total_hh_backlog_reg.iloc[td] * backlog_perc)
            backlog_compare.at[t,'Nao Regul'] += (total_hh_backlog_nreg.iloc[td] * backlog_perc)
            backlog_compare.at[t, 'Total'] += (total_hh_backlog_reg.iloc[td] + total_hh_backlog_nreg.iloc[td]) * backlog_perc

        backlog_compare.at[t, 'Alocado'] = 0.0
        for td in range(t + 8, t + 18):
            backlog_compare.at[t, 'Alocado'] += eletri_folga.iloc[td]

    backlog_compare.loc["Total"] = backlog_compare.sum()
    return backlog_compare

def print_info_base(locs_bases_data, location, depot_customers, d_backlog, backlog_perc):
    Info_base = pd.DataFrame(columns=['1', '2'])

    aux = locs_bases_data['NOME_LOCALIDADE'][location] + \
        ' (' + str(location) + ')'
    Info_base = Info_base.append(
        {'1': 'Localidade da base:', '2': aux}, ignore_index=True)

    aux = ''
    for l in depot_customers:
        if aux == '':
            aux = locs_bases_data['NOME_LOCALIDADE'][l] + ' (' + str(l) + ')'
            Info_base = Info_base.append(
                {'1': 'Localidades antendidas:', '2': aux}, ignore_index=True)
        else:
            aux = locs_bases_data['NOME_LOCALIDADE'][l] + ' (' + str(l) + ')'
            Info_base = Info_base.append(
                {'1': '', '2': aux}, ignore_index=True)

    Info_base = Info_base.append(
        {'1': 'HH necessário para backlog:', '2': d_backlog}, ignore_index=True)

    aux = str(backlog_perc * 100) + '%'
    Info_base = Info_base.append(
        {'1': 'Percent. de backlog atendido:', '2': aux}, ignore_index=True)

    return Info_base

def get_main_information(P, meses, resume_all_costs, resume_all_quant, Terc, T, D, R, sizing):

    resume_all = pd.DataFrame()
    for mes in meses:
        all_costs = resume_all_costs.loc[resume_all_costs['Mes'] == mes]
        all_quant = resume_all_quant.loc[resume_all_quant['Mes'] == mes]

        all_costs.loc['TOTAL'] = all_costs.sum(axis = 0, skipna = True).astype(float)
        all_quant.loc['TOTAL'] = all_quant.sum(axis = 0, skipna = True).astype(int)

        resume_aux = pd.DataFrame(index=['Quantidade', 'Custo'], columns=resume_all_quant.columns)
        resume_aux = resume_aux.drop(['Total', 'Ociosidade'], axis=1)

        for j in P:
            aux = 'Eletricistas (' + str(j) + ')'
            resume_aux[aux] = [all_quant.loc['TOTAL'][aux], round(all_costs.loc['TOTAL'][aux],2)]

            if sizing:
                aux1 = 'Contratacoes (' + str(j) + ')'
                aux2 = 'Demissoes (' + str(j) + ')'
                aux3 = 'Transferencias (' + str(j) + ')'

                con = all_quant.loc['TOTAL'][aux1]
                dem = all_quant.loc['TOTAL'][aux2]

                dif = con - dem

                if j not in Terc:
                    if dif >= 0:
                        resume_aux[aux1] = [dif, dif * T[j]]
                        resume_aux[aux2] = [0, 0.00]
                        transferencia = [dem, dem * R[j]]
                        resume_aux[aux3] = transferencia
                    else:
                        resume_aux[aux1] = [0, 0.00]
                        resume_aux[aux2] = [abs(dif), abs(dif) * D[j]]
                        transferencia = [con, con * R[j]]
                        resume_aux[aux3] = transferencia
                else:
                    resume_aux[aux1] = [all_quant.loc['TOTAL'][aux1], round(all_costs.loc['TOTAL'][aux1],2)]
                    resume_aux[aux2] = [all_quant.loc['TOTAL'][aux2], round(all_costs.loc['TOTAL'][aux2],2)]
                    resume_aux[aux3] = [0, 0.00]

        resume_aux['Mes'] = mes
        resume_all = resume_all.append(resume_aux, ignore_index=False)

    return resume_all

def print_data(Info_base, escalas_info, resume_results, idle_perc, backlog_atend):
    print('-----------------------------------------------------------')
    print(Info_base.to_string(index=False, header=False), flush=True)
    print()
    print(escalas_info.to_string(index=False), flush=True)
    print()
    print(resume_results.to_string(index=False), flush=True)
    print()
    print("Ociosidade (%): " + str(idle_perc), flush=True)
    print()
    print(backlog_atend)

def plot_graphs(depot, mes_demanda, P, eletri_disp_total, eletri_disp_emerg, demanda_hh_em, d_emergecial_hhora, total_hh_backlog_reg):

    ren_col1, ren_col2, ren_col3 = {}, {}, {}
    for j in P:
        ren_col1[j] = 'Total ' + str(j)
        ren_col2[j] = 'Emerg ' + str(j)

    # Demanda emergencial por hora
    demanda_emergencial_hhora = pd.DataFrame.from_dict(d_emergecial_hhora)
    demanda_total_hhora = demanda_emergencial_hhora
    # demanda_emergencial_hhora = pd.DataFrame(demanda_emergencial_hhora.sum(skipna=True, axis=1), columns=[
    #     'D. Emergencial']).rename_axis('Hora')#.reset_index()
    demanda_emergencial_hhora = pd.DataFrame(demanda_emergencial_hhora.sum(skipna=True, axis=1), columns=[
        'Valor']).rename_axis('Hora')
    demanda_emergencial_hhora.insert(0, 'Cod_perfil', 'XXX')
    demanda_emergencial_hhora.insert(1, 'Descricao', 'D. Emergencial')

    # Eletricistas, para demanda emergencial, disponiveis por hora
    eletri_disp_emerg = eletri_disp_emerg.sort_index(axis=1)#.rename(columns=ren_col2)
    eletri_disp_emerg_aux = pd.DataFrame()
    for j in P:
        aux = eletri_disp_emerg[[j]].copy().rename(columns={j:'Valor'})
        aux.insert(0, 'Cod_perfil', j)
        aux.insert(1, 'Descricao', 'Emergencial')
        eletri_disp_emerg_aux = eletri_disp_emerg_aux.append(aux, ignore_index=False)

    # Demanda regulada por hora (inclui demanda emergencial e regulada não emergencial)
    total_hh_backlog_reg = pd.DataFrame(total_hh_backlog_reg)
    total_hh_backlog_reg = total_hh_backlog_reg.sum(skipna=True, axis=1)
    total_hh_backlog_reg = pd.DataFrame(total_hh_backlog_reg, columns=['Demanda'])

    # demanda_total_hhora = demanda_hh_em
    demanda_total_hhora = demanda_total_hhora.add(total_hh_backlog_reg, fill_value=0)
    #demanda_total_hhora = pd.DataFrame(demanda_total_hhora.sum(skipna=True, axis=1), columns=['D. Regulada']).rename_axis('Hora')#.reset_index()
    demanda_total_hhora = pd.DataFrame(demanda_total_hhora.sum(skipna=True, axis=1), columns=['Valor']).rename_axis('Hora')
    demanda_total_hhora.insert(0, 'Cod_perfil', 'XXX')
    demanda_total_hhora.insert(1, 'Descricao', 'D. Regulada')

    # Eletricistas, para a demanda total, disponiveis por hora
    eletri_disp_total = eletri_disp_total.sort_index(axis=1)#.rename(columns=ren_col1)
    eletri_disp_total_aux = pd.DataFrame()
    for j in P:
        aux = eletri_disp_emerg[[j]].copy().rename(columns={j:'Valor'})
        aux.insert(0, 'Cod_perfil', j)
        aux.insert(1, 'Descricao', 'Total')
        eletri_disp_total_aux = eletri_disp_total_aux.append(aux, ignore_index=False)
    
    # Tabela com informações para o plot dos gráficos
    plot_table_aux = pd.concat([demanda_total_hhora, demanda_emergencial_hhora, eletri_disp_total_aux, eletri_disp_emerg_aux], axis=0, sort=False)
    # plot_table_aux = plot_table.T.rename_axis('Descricao').reset_index()
    plot_table_aux.insert(0,'Base', depot)
    plot_table_aux.insert(1,'Mes', mes_demanda)
    plot_table_aux = plot_table_aux.reset_index().rename(columns={'index':'Hora'})
    cols = ['Base','Mes','Cod_perfil','Descricao','Hora','Valor']
    plot_table_aux = plot_table_aux.loc[:, cols]
    
    return plot_table_aux
