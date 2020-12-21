import copy
import math
import os
import pandas as pd
import re
import time
from data import *

class Cliente:
    def __init__(self, cod, x, y, cod_localidade, grupo, lim_dic_mes, lim_dic_tri, lim_dic_ano, lim_dmic, eusd):
        self.cod = cod
        self.x = x
        self.y = y
        self.cod_localidade = cod_localidade
        self.grupo = grupo
        self.lim_dic_mes = lim_dic_mes
        self.lim_dic_tri = lim_dic_tri
        self.lim_dic_ano = lim_dic_ano
        self.lim_dmic = lim_dmic
        self.eusd = eusd

class OS:
    def __init__(self, cod, mes, ano, cod_localidade, cod_servico, clientes):
        self.cod = cod
        self.mes = mes
        self.ano = ano
        self.cod_localidade = cod_localidade
        self.cod_servico = cod_servico
        self.clientes = clientes

class Servico:
    def __init__(self, cod, tipo_ocor, TMP, TME, cod_veiculo):
        self.cod = cod
        self.tipo_ocor = tipo_ocor
        self.TMP = TMP
        self.TME = TME
        self.cod_veiculo = cod_veiculo

class BlocoServico:
    def __init__(self, cod, servicos):
        self.cod = cod
        self.servicos = servicos

class Localidade:
    def __init__(self, nome, cod, x, y, possui_base, cod_base):
        self.nome = nome
        self.cod = cod
        self.x = x
        self.y = y
        self.possui_base = possui_base
        self.cod_base = cod_base

# Calcula compensação estimada para cada par de localidades (cliente e base)
def msij_calc(tab_loc, tab_os, tab_cliente, tab_servico, tab_tempos):

    localidades = {}  # conjunto de localidades

    for l,row in tab_loc.iterrows():
        localidades[l] = Localidade(row['NOME_LOCALIDADE'], l, row['COORD_X'], row['COORD_Y'],
                                        row['POSSUI_BASE'], row['COD_PERFIL_BASE'])

    servicos = {}  # lista de servicos regulados, mapeamento servico
    for servId, row in tab_servico.iterrows():
        if row['IND_OS_REGULADA'] == 'N' or row['TIPO_OCORRENCIA'] == 'P':
            continue  # servico nao eh emergencial regulado, ignore-o

        # cod, tipo_serv, tipo_ocor, reg, imediato, TMP, TME, cod_veiculo)
        obj = Servico(servId, row['TIPO_OCORRENCIA'],
                      row['TMP'], row['TME'], row['COD_PERFIL_VEICULO'])
        servicos[servId] = obj

    oss = {}  # conjunto de OSs reguladas
    oss_localidade = {cod: {s: [] for s in servicos} for cod in localidades}  # conjunto de OSs reguladas separadas por localidade e serviço
    meses_str = []  # meses contemplados pelas OSs (string ano-mes)

    for index, row in tab_os.iterrows():
        if int(row['COD_SERVICO']) not in servicos:
            continue  # OS nao regulada ou servico de outro departamento
        osId = int(row['NUM_OS'])
        affected_clients = []
        if not pd.isnull(row['COD_UC']):
            affected_clients = row['COD_UC'].split(',') 
        else:
            continue
        obj = OS(osId, int(row['MES_CONCLUSAO_OS']), int(row['ANO_CONCLUSAO_OS']),
                 row['COD_LOCALIDADE'], \
                 int(float(row['COD_SERVICO'])), [int(float(affected_clients[0]))])
        oss[osId] = obj
        oss_localidade[row['COD_LOCALIDADE']][row['COD_SERVICO']].append(osId)
        pos = len(meses_str)
        for (i, mes) in enumerate(meses_str):
            if str(obj.ano) + '-' + str(obj.mes) == mes:
                pos = -1;
                break
            elif (int(obj.ano) < int(mes.split('-')[0])) or (int(obj.ano) == int(mes.split('-')[0]) and int(obj.mes) < int(mes.split('-')[1])):
                pos = i;
                break
        if pos != -1:
            meses_str.insert(pos, str(obj.ano) + '-' + str(obj.mes))

        for ac in affected_clients[1:]:
            oss[osId].clientes.append(int(ac))

    meses_str = detectaMesDoIntervaloSemOS(meses_str)

    meses = [i for i in range(len(meses_str))]  # indice de meses (p.e. 0,1,2..,20, para mais de 1 ano)

    clientes = {}  # conjunto de clientes
    # clientes separados por localidades (e.x. UC da localidade i vai para celula map_localidade[i] em clientes_local.)
    clientes_localidade = {cod: [] for cod in localidades}

    for l in range(len(tab_cliente['COD_UC'])):
        if float(tab_cliente['EUSD_MEDIO'][l]) == 0.0 or math.isnan(tab_cliente['EUSD_MEDIO'][l]):
            continue # ignora cliente
        cliId = int(tab_cliente['COD_UC'][l])
        codLoc = tab_cliente['COD_LOCALIDADE'][l]
        obj = Cliente(cliId, float(tab_cliente['COORD_X'][l]), float(tab_cliente['COORD_Y'][l]), codLoc, \
                      tab_cliente['GRUPO_FORNECIMENTO'][l], float(tab_cliente['LIMITE_DIC_MENSAL'][l]),
                      float(tab_cliente['LIMITE_DIC_TRIMESTRAL'][l]), \
                      float(tab_cliente['LIMITE_DIC_ANUAL'][l]), float(tab_cliente['LIMITE_DMIC'][l]), float(tab_cliente['EUSD_MEDIO'][l]))
        clientes[cliId] = obj
        clientes_localidade[codLoc].append(cliId)  # separa cliente por localidade

    m2 = {key: 0.0 for key, val in localidades.items()}
    m1 = {key: copy.deepcopy(m2) for key, val in localidades.items()} 
    m = {s: copy.deepcopy(m1) for s in tab_servico.index.values}# m[s][i][j], compensacao estimada

    kei = {'B': 15, 'M': 20, 'A': 27}  # fator de majoracao

    for s in servicos:
        for i in localidades:  # localidade atendida
            if len(oss_localidade[i][s]) == 0:
                continue  # nao tem OS na localidade

            for j in localidades:  # localidade com base

                TMD = tab_tempos[j][i]  # deslocamento estimado (em horas) de j para i

                # para cada cliente da localidade, mantem par (DIC_apurado,DMIC apurado) para cada mes para calculo
                # do DIC e DMIC, respectivamente (apurado em horas)
                interrupcao = {}
                for cliente in clientes_localidade[i]:
                    interrupcao[cliente] = {mes: [0.0, 0.0] for mes in meses}

                for os_ in oss_localidade[i][s]:

                    os_info = oss[os_]
                    cod_serv = os_info.cod_servico
                    mes = meses_str.index(str(os_info.ano) + '-' + str(os_info.mes))
                    TMA = servicos[cod_serv].TMP + TMD + servicos[cod_serv].TME  # duracao total de interrupcao
                    for c in os_info.clientes:  # atualiza DIC e DMIC para clientes afetados pela os
                        interrupcao_ = interrupcao.get(c, False)
                        if interrupcao_ != False:       

                            interrupcao_[mes][0] += TMA
                            if TMA > interrupcao_[mes][1]:
                                interrupcao_[mes][1] = TMA

                # calcula compensacao por DIC e DMIC mensal 
                for c in clientes_localidade[i]:

                    # calcula compensacao por DIC trimestral
                    dic_tri_apurado, dic_ano_apurado = 0.0, 0.0
                    cliente = clientes[c]
                    eusd_grupo_fac = (cliente.eusd/730) * kei[cliente.grupo]
                    DIC_p = cliente.lim_dic_mes
                    DMIC_p = cliente.lim_dmic
                    DIC_p_tri = cliente.lim_dic_tri
                    DIC_p_ano = cliente.lim_dic_ano
                    compensacao_DIC_mes_ = {mes : 0.0 for mes in meses}
                    interrupcao_ = interrupcao[c]

                    for (ind,mes) in enumerate(meses):

                        # DIC e DMIC mensal
                        DIC_v = interrupcao_[mes][0]
                        DMIC_v = interrupcao_[mes][1]
                        dic_tri_apurado += DIC_v
                        dic_ano_apurado += DIC_v

                        if DIC_v > DIC_p:  # extrapolou o limite
                            valor_comp = (DIC_v - DIC_p) * eusd_grupo_fac
                            compensacao_DIC_mes_[mes] = valor_comp
                            m[s][i][j] += valor_comp

                        if DMIC_v > DMIC_p:
                            m[s][i][j] += (DMIC_v - DMIC_p) * eusd_grupo_fac
                
                        if mes%3 == 2: # DIC trimestral

                            if dic_tri_apurado > DIC_p_tri:
                                valor_comp_princ = (dic_tri_apurado - DIC_p_tri) * eusd_grupo_fac
                                mesesNaoViolados = [l for l in range(ind - 2, ind + 1) if
                                                    compensacao_DIC_mes_[meses[l]] == 0.0]
                                sumApuradoMesesNViol = 0.0  # soma dos DICs apurados para meses nao violados
                                for l in mesesNaoViolados:
                                    sumApuradoMesesNViol += interrupcao_[meses[l]][0]

                                if len(mesesNaoViolados) > 0 and sumApuradoMesesNViol > 0.0:  # caso 1
                                    m[s][i][j] += valor_comp_princ * (sumApuradoMesesNViol / dic_tri_apurado)
                                else:  # casos 2 e 3
                                    sumCompMensaisPagas = 0.0
                                    for l in range(ind - 2, ind + 1):
                                        sumCompMensaisPagas += compensacao_DIC_mes_[meses[l]]
                                    m[s][i][j] += max(0, valor_comp_princ - sumCompMensaisPagas)
                            
                            dic_tri_apurado = 0.0
                
                        if mes%12 == 11: # DIC anual

                            if dic_ano_apurado > DIC_p_ano:
                                valor_comp_princ = (dic_ano_apurado - DIC_p_ano) * eusd_grupo_fac
                                mesesNaoViolados = [l for l in range(ind - 11, ind + 1) if
                                                    compensacao_DIC_mes_[meses[l]] == 0.0]
                                sumApuradoMesesNViol = 0.0  # soma dos DICs apurados para meses nao violados
                                for l in mesesNaoViolados:
                                    sumApuradoMesesNViol += interrupcao_[meses[l]][0]

                                if len(mesesNaoViolados) > 0 and sumApuradoMesesNViol > 0.0:  # caso 1
                                    m[s][i][j] += valor_comp_princ * (sumApuradoMesesNViol / dic_ano_apurado)
                                else:  # casos 2 e 3
                                    sumCompMensaisPagas = 0.0
                                    for l in range(ind - 11, ind + 1):
                                        sumCompMensaisPagas += compensacao_DIC_mes_[meses[l]]
                                    m[s][i][j] += max(0, valor_comp_princ - sumCompMensaisPagas)

                            dic_ano_apurado = 0.0
    return m

def detectaMesDoIntervaloSemOS(meses_str):
    meses = []
    # testa falta de algum mês entre os meses considerados
    for (i, mes) in enumerate(meses_str):
        if i > 0:
            ano1, ano2 = meses_str[i - 1].split('-')[0], mes.split('-')[0]
            mes1, mes2 = meses_str[i - 1].split('-')[1], mes.split('-')[1]
            if int(mes1) != 12:
                if (ano1 != ano2): # anos diferentes
                    # print("Problema durante cálculo de compensação! Existe um mês sem OS no intervalo de meses considerado: ", meses_str[i - 1], "/", mes)
                    for m in range(int(mes1)+1, 13):
                        meses.append(ano1 + '-' + str(m))
                    for y in range(int(ano1) + 1, int(ano2) + 1):
                        if y != int(ano2):
                            for m in range(1, 13):
                                meses.append(str(y) + '-' + str(m))
                        else:
                            for m in range(1, int(mes2)):
                                meses.append(str(y) + '-' + str(m))

                elif (int(mes2) - int(mes1) != 1):
                    # print("Problema durante cálculo de compensação! Existe um mês sem OS no intervalo de meses considerado: ", meses_str[i - 1], "/", mes)
                    for m in range(int(mes1)+1, int(mes2)):
                        meses.append(ano1 + '-' + str(m))
            else:
                if ((int(ano1) + 1) != int(ano2)):
                    for y in range(int(ano1) + 1, int(ano2) + 1):
                        if y != int(ano2):
                            for m in range(1, 13):
                                meses.append(str(y) + '-' + str(m))
                        else:
                            for m in range(1, int(mes2)):
                                meses.append(str(y) + '-' + str(m))
                    # print("Problema durante cálculo de compensação! Existe um mês sem OS no intervalo de meses considerado: ", meses_str[i - 1], "/", mes)
                elif (int(mes2) != 1):
                    for m in range(1, int(mes2)):
                        meses.append(ano2 + '-' + str(m))
                    # print("Problema durante cálculo de compensação! Existe um mês sem OS no intervalo de meses considerado: ", meses_str[i - 1], "/", mes)
        meses.append(mes)
    return meses

# Precalcula as principais constantes necessárias para execução do módulo estratégico.    
def strategic_consts(data, q, L, tmd, tmp, tme, hotel_cost, E, P, l):

    hh = {e: row['HH_TOTAL'] for e, row in data.crew_profiles.iterrows()} ## HH disponível diário por perfil de equipe
    tem_saida = {} # tempo de saída da equipe na base para simulação (inclui preparação do veículo e outros desvios)
    for e, row in data.crew_profiles.iterrows():
        cellVal = row['MIP']
        tem_saida[e] = hh[e] - float(cellVal)/100 * hh[e] # define tempo de saída da base
        
    S = data.crew_service['COD_SERVICO'].unique() # codigos de servico
    team_services = {code: set() for code in E} # codigos de servico por perfil de equipe (quais pode executar)
    for index, row in data.crew_service.iterrows():
        team_services[row['COD_PERFIL_EQUIPE']].add(row['COD_SERVICO'])

    team_cost = {e: 0 for e in E} # custo anual médio por perfil de equipe
    for e in E:
        for p in P:
            team_cost[e] += l.get((p,e),0) * data.electricians_costs.loc[p]['CUSTO_MANUTENCAO']

    # Calcula qual equipes fica responsavel por cada serviço em cada base no estratégico (equipe mais barata)
    # Note que para o tático, mais de um perfil pode executar o mesmo tipo de serviço
    B = data.bases_profiles['COD_PERFIL_BASE'].unique()
    crew_by_service = {b: {s: -1 for s in S} for b in B}
    for base_code, rows in data.bases_profiles.groupby('COD_PERFIL_BASE'):
        for team in rows['COD_PERFIL_EQUIPE']:
            for service in team_services[team]:
                e = crew_by_service[base_code][service]
                if e == -1 or team_cost[team] < team_cost[e]:
                    crew_by_service[base_code][service] = team

    # h[s][i][b][j][e] diz quantas equipes são estimadas para o atendimento do serviço s
    # na localidade i por uma equipe do perfil e em uma base do perfil b na localidade j
    h0 = {e: 0.0 for e in E}
    h1 = {j: copy.deepcopy(h0) for j in L}
    h2 = {b: copy.deepcopy(h1) for b in B}
    h3 = {i: copy.deepcopy(h2) for i in L}
    h = {s: copy.deepcopy(h3) for s in S}

    # g[i][b][j] diz o custo de atender a localidade i por uma base do perfil b na localidade j
    g1 = {j: 0.0 for j in L}
    g2 = {b: copy.deepcopy(g1) for b in B}
    g = {i: copy.deepcopy(g2) for i in L}

    # beta[s][i][j] diz quantas OS's do tipo s são feitas por dia na localidade i por uma equipe vindo de j 
    beta1 = {j: 1.0 for j in L}
    beta2 = {i: copy.deepcopy(beta1) for i in L}
    beta = {s: copy.deepcopy(beta2) for s in S}

    # gama[e][s][i][j] é a produtividade estimada para uma equipe do perfil e atender o serviço s na localidade i
    # partindo de uma base na localidade j
    gama0 = {j: 0.0 for j in L}
    gama1 = {i: copy.deepcopy(gama0) for i in L}
    gama2 = {s: copy.deepcopy(gama1) for s in S}
    gama = {e: copy.deepcopy(gama2) for e in E}

    # H[s][i][b][j] indica (quando True) que o atendimento do serviço s em i por uma base b em j
    # foi dimensionado como se as equipes para essa demanda estivessem em hotel em i
    # ou seja, com custo adicional (diárias)
    H1 = {j: False for j in L}
    H2 = {b: copy.deepcopy(H1) for b in B}
    H3 = {i: copy.deepcopy(H2) for i in L}
    H = {s: copy.deepcopy(H3) for s in S} 

    tbo = {} # tempo entre os's na localidade
    tbo_exc = {} # tempo entre os's de exceçao na localidade 
    for j in L:
        tbo[j] = data.locs_bases.loc[j]['TEMPO_MEDIO_ENTRE_OS']
        tbo_exc[j] = data.locs_bases.loc[j]['TEMPO_MEDIO_ENTRE_OS_EXCESSAO']
    # qtd de dias de trabalhos no ano por equipe
    work_days = {e: row['QTD_DIAS_TRABALHO_ANO'] for e, row in data.crew_profiles.iterrows()} 
    
    # Loop principal para preenchimento dos dicionários
    for i in L:
        for j in L:
            ttmd = tmd[i][j] + tmd[j][i]
            for b in B:
                for s in S:
                    if q.get(s, {}).get(i, 0) == 0:
                        for e in E:
                            if s in team_services[e]:
                                gama[e][s][i][j] = float('inf')
                        continue
                    e_cheap = crew_by_service[b][s]
                    if e_cheap == -1: # se pelo menos 1 serviço nao puder ser atendido, o atendimento é proibido
                        g[i][b][j] = float('inf')
                        for e in E:
                            for s in S: 
                                h[s][i][b][j][e] = float('inf')
                        break

                    tbo_i = tbo[i]
                    if data.services.loc[s]['TMD_INTERNO_EXCECAO'] == 'S':
                        tbo_i = tbo_exc[i]

                    for e in E:
                        if not s in team_services[e]:
                            continue

                        hh_disp = hh[e]-ttmd-tem_saida[e]
                        # print("\nprecalc.py, linha 346. hh[e]: ",hh[e])   # Remover - RL
                        # print("tempo médio de deslocamento ttmd: ",ttmd)   # Remover - RL
                        # print("tempo de saída: ",tem_saida[e])   # Remover - RL
                        os_quantity_day_team = 0
                        if hh_disp >= tme[s]: # primeira os nao tem tbs 
                            os_quantity_day_team += 1
                            hh_disp -= tme[s]
                        tempo_por_os = tbo_i + tme[s] # a partir da segunda os
                        if tempo_por_os <= 0.0001:
                            print("tempo_por_os muito baixo (< 0.001)! Execução encerrada!")
                            exit(0)
                        while hh_disp >= tempo_por_os:
                            os_quantity_day_team += 1
                            hh_disp -= tempo_por_os
                        os_quantity_day_team += hh_disp/tempo_por_os # considera sobra de hh

                        if os_quantity_day_team > 0.0: # distancia é viavel para atendimento
                            if e == e_cheap:
                                os_quantity_year_team = os_quantity_day_team * work_days[e]
                                h[s][i][b][j][e] = q[s][i] / os_quantity_year_team
                                g[i][b][j] += team_cost[e] * (q[s][i] / os_quantity_year_team)
                                beta[s][i][j] = max(1.0, os_quantity_day_team)
                            gama[e][s][i][j] = os_quantity_day_team/hh[e]
                        elif os_quantity_day_team <= 0.0: # atendimento inviavel (muito longe)
                            # dimensiona como se a base estivesse em i com custo adicional de hotel
                            ttmd_i = tmd[i][i] + tmd[i][i]
                            hh_disp = hh[e]-ttmd_i-tem_saida[e]
                            os_quantity_day_team_i = 0
                            if hh_disp >= tme[s]: # primeira os nao tem tbs 
                                os_quantity_day_team_i += 1
                                hh_disp -= tme[s]
                            tempo_por_os = tbo_i + tme[s]
                            if tempo_por_os <= 0.0001:
                                print("tempo_por_os muito baixo (< 0.001)! Execução encerrada!")
                                exit(0)
                            while hh_disp >= tempo_por_os:
                                os_quantity_day_team_i += 1
                                hh_disp -= tempo_por_os
                            os_quantity_day_team_i += hh_disp/tempo_por_os # considera sobra de hh

                            if os_quantity_day_team_i > 0.0: # em hotel, atendimento é viável pela distancia
                                if e == e_cheap:
                                    os_quantity_year_team_i = os_quantity_day_team_i * work_days[e]
                                    nbTeams = q[s][i] / os_quantity_year_team_i
                                    h[s][i][b][j][e] = nbTeams
                                    g[i][b][j] += team_cost[e] * nbTeams
                                    beta[s][i][j] = max(1.0, os_quantity_day_team_i)
                                    # adiciona custo com hotel
                                    nbPeople = sum([l.get((p,e),0) for p in P]) * nbTeams 
                                    additive_cost = work_days[e] * nbPeople * hotel_cost
                                    g[i][b][j] += additive_cost
                                    H[s][i][b][j] = True
                                gama[e][s][i][j] = os_quantity_day_team_i/hh[e]
                            elif e == e_cheap: # essa localidade possui deslocamento interno muito grande, atendimento inviavel
                                g[i][b][j] = float('inf')
                                for e in E:
                                    for s in S: 
                                        h[s][i][b][j][e] = float('inf') 
                                break
            
    # calcula estimativas de compensação por DIC (mensal, trimestral e anual) e DMIC       
    m = msij_calc(data.locs_bases, data.os_emergency, data.clients, data.services, tmd) 

    # Salva constantes calculadas em um arquivo para evitar o recalculo em futuras execucoes
    path_to_write = 'precalc/PRECALCULO_'+str(hash((int(data.cod_modulo), int(data.cod_estudo))))+'.csv'
    with open(path_to_write, 'w') as f:
        f.write('CONSTANTE;COD_SERVICO;COD_LOCALIDADE_CLIENTE;COD_PERFIL_BASE;COD_LOCALIDADE_BASE;COD_PERFIL_EQUIPE;VALOR\n')
        for i in L:
            for j in L:
                firstBLoop1 = True
                for s in S:
                    if m[s][i][j] > 0:
                        f.write('m;'+str(s)+';'+str(i)+';;'+str(j)+';;'+str(m[s][i][j])+'\n')
                    if beta[s][i][j] > 1:
                        f.write('beta;'+str(s)+';'+str(i)+';;'+str(j)+';;'+str(beta[s][i][j])+'\n')
                    firstBLoop2 = True   
                    for e in E:
                        if gama[e][s][i][j] > 0:
                            f.write('gama;'+str(s)+';'+str(i)+';;'+str(j)+';'+str(e)+';'+str(gama[e][s][i][j])+'\n')
                        for b in B:
                            if h[s][i][b][j][e] > 0:
                                f.write('h;'+str(s)+';'+str(i)+';'+str(b)+';'+str(j)+';'+str(e)+';'+str(h[s][i][b][j][e])+'\n')
                            if firstBLoop1 and g[i][b][j] > 0:
                                f.write('g;;'+str(i)+';'+str(b)+';'+str(j)+';;'+str(g[i][b][j])+'\n')
                            if firstBLoop2 and H[s][i][b][j] > 0:
                                f.write('H;'+str(s)+';'+str(i)+';'+str(b)+';'+str(j)+';;'+str(H[s][i][b][j])+'\n')
                        firstBLoop1 = False 
                        firstBLoop2 = False 
     
    return m, h, g, beta, gama, H

# Precalcula produtividade para execução do módulo tático
def productivity(data, q, L, tmd, tmp, tme, E, P, l):

    hh = {e: row['HH_TOTAL'] for e, row in data.crew_profiles.iterrows()} ## HH disponível diário por perfil de equipe
    tem_saida = {} # tempo de saída da equipe na base para simulação (inclui preparação do veículo e outros desvios)
    for e, row in data.crew_profiles.iterrows():
        cellVal = row['MIP']
        tem_saida[e] = hh[e] - float(cellVal)/100 * hh[e] # define tempo de saída da base
        
    B = data.bases_profiles['COD_PERFIL_BASE'].unique()
    S = data.crew_service['COD_SERVICO'].unique() # codigos de servico
    team_services = {code: set() for code in E} # codigos de servico por perfil de equipe (quais pode executar)
    for index, row in data.crew_service.iterrows():
        team_services[row['COD_PERFIL_EQUIPE']].add(row['COD_SERVICO'])

    # gama[e][s][i][j] é a produtividade estimada para uma equipe do perfil e atender o serviço s na localidade i
    # partindo de uma base na localidade j
    gama0 = {j: 0.0 for j in L}
    gama1 = {i: copy.deepcopy(gama0) for i in L}
    gama2 = {s: copy.deepcopy(gama1) for s in S}
    gama = {e: copy.deepcopy(gama2) for e in E}

    tbo = {} # tempo entre os's na localidade
    tbo_exc = {} # tempo entre os's de exceçao na localidade 
    for j in L:
        tbo[j] = data.locs_bases.loc[j]['TEMPO_MEDIO_ENTRE_OS']
        tbo_exc[j] = data.locs_bases.loc[j]['TEMPO_MEDIO_ENTRE_OS_EXCESSAO']
    # qtd de dias de trabalhos no ano por equipe
    work_days = {e: row['QTD_DIAS_TRABALHO_ANO'] for e, row in data.crew_profiles.iterrows()} 
    
    # Loop principal para preenchimento dos dicionários
    for i in L:
        for j in L:
            ttmd = tmd[i][j] + tmd[j][i]
            for b in B:
                for s in S:
                    if q.get(s, {}).get(i, 0) == 0:
                        for e in E:
                            if s in team_services[e]:
                                gama[e][s][i][j] = float('inf')
                        continue

                    tbo_i = tbo[i]
                    if data.services.loc[s]['TMD_INTERNO_EXCECAO'] == 'S':
                        tbo_i = tbo_exc[i]

                    for e in E:
                        if not s in team_services[e]:
                            continue

                        hh_disp = hh[e]-ttmd-tem_saida[e]
                        os_quantity_day_team = 0
                        if hh_disp >= tme[s]: # primeira os nao tem tbs 
                            os_quantity_day_team += 1
                            hh_disp -= tme[s]
                        tempo_por_os = tbo_i + tme[s] # a partir da segunda os
                        if tempo_por_os <= 0.0001:
                            print("tempo_por_os muito baixo (< 0.001)! Execução encerrada!")
                            exit(0)
                        while hh_disp >= tempo_por_os:
                            os_quantity_day_team += 1
                            hh_disp -= tempo_por_os
                        os_quantity_day_team += hh_disp/tempo_por_os # considera sobra de hh

                        if os_quantity_day_team > 0.0: # distancia é viavel para atendimento
                            gama[e][s][i][j] = os_quantity_day_team/hh[e]
                        elif os_quantity_day_team <= 0.0: # atendimento inviavel (muito longe)
                            # dimensiona como se a base estivesse em i com custo adicional de hotel
                            ttmd_i = tmd[i][i] + tmd[i][i]
                            hh_disp = hh[e]-ttmd_i-tem_saida[e]
                            os_quantity_day_team_i = 0
                            if hh_disp >= tme[s]: # primeira os nao tem tbs 
                                os_quantity_day_team_i += 1
                                hh_disp -= tme[s]
                            tempo_por_os = tbo_i + tme[s]
                            if tempo_por_os <= 0.0001:
                                print("tempo_por_os muito baixo (< 0.001)! Execução encerrada!")
                                exit(0)
                            while hh_disp >= tempo_por_os:
                                os_quantity_day_team_i += 1
                                hh_disp -= tempo_por_os
                            os_quantity_day_team_i += hh_disp/tempo_por_os # considera sobra de hh

                            if os_quantity_day_team_i > 0.0: # em hotel, atendimento é viável pela distancia
                                gama[e][s][i][j] = os_quantity_day_team_i/hh[e]
            
    # Salva constantes calculadas em um arquivo para evitar o recalculo em futuras execucoes
    path_to_write = 'precalc/PRECALCULO_'+str(hash((int(data.cod_modulo), int(data.cod_estudo))))+'.csv'
    with open(path_to_write, 'w') as f:
        f.write('CONSTANTE;COD_SERVICO;COD_LOCALIDADE_CLIENTE;COD_PERFIL_BASE;COD_LOCALIDADE_BASE;COD_PERFIL_EQUIPE;VALOR\n')
        for i in L:
            for j in L:
                for s in S:
                    for e in E:
                        if gama[e][s][i][j] > 0:
                            f.write('gama;'+str(s)+';'+str(i)+';;'+str(j)+';'+str(e)+';'+str(gama[e][s][i][j])+'\n')

    return gama

# Carrega constantes já calculadas e salvas em arquivo por execução anterior
def load(precalc_path, E, L, B, service_data, strategic=False):

    S = service_data.index.values # codigos de servico

    if strategic:

        h0 = {e: 0.0 for e in E}
        h1 = {j: copy.deepcopy(h0) for j in L}
        h2 = {b: copy.deepcopy(h1) for b in B}
        h3 = {i: copy.deepcopy(h2) for i in L}
        h = {s: copy.deepcopy(h3) for s in S}

        g1 = {j: 0.0 for j in L}
        g2 = {b: copy.deepcopy(g1) for b in B}
        g = {i: copy.deepcopy(g2) for i in L}

        beta1 = {j: 1.0 for j in L}
        beta2 = {i: copy.deepcopy(beta1) for i in L}
        beta = {s: copy.deepcopy(beta2) for s in S}

        gama0 = {j: 0.0 for j in L}
        gama1 = {i: copy.deepcopy(gama0) for i in L}
        gama2 = {s: copy.deepcopy(gama1) for s in S}
        gama = {e: copy.deepcopy(gama2) for e in E}

        H1 = {j: False for j in L}
        H2 = {b: copy.deepcopy(H1) for b in B}
        H3 = {i: copy.deepcopy(H2) for i in L}
        H = {s: copy.deepcopy(H3) for s in S} 

        m2 = {j: 0.0 for j in L}
        m1 = {i: copy.deepcopy(m2) for i in L} 
        m = {s: copy.deepcopy(m1) for s in S}# m[s][i][j], compensacao estimada

        with open(precalc_path, 'r') as f:
            f.readline() # ignore first line
            for line in f:
                row = line.split(';')
                name = row[0]
                s = row[1] if row[1] == '' else int(row[1])
                i = row[2] if row[2] == '' else int(row[2])
                b = row[3] if row[3] == '' else int(row[3])
                j = row[4] if row[4] == '' else int(row[4])
                e = row[5] if row[5] == '' else int(row[5])
                val = row[6]

                if name == 'h':
                    h[s][i][b][j][e] = float(val)   
                elif name == 'm':
                    m[s][i][j] = float(val)
                elif name == 'g':
                    g[i][b][j] = float(val)   
                elif name == 'beta':
                    beta[s][i][j] = float(val)
                elif name == 'gama':
                    gama[e][s][i][j] = float(val)
                elif name == 'H':
                    H[s][i][b][j] = bool(val)

        return m, h, g, beta, gama, H

    else: # tatico

        gama0 = {j: 0.0 for j in L}
        gama1 = {i: copy.deepcopy(gama0) for i in L}
        gama2 = {s: copy.deepcopy(gama1) for s in S}
        gama = {e: copy.deepcopy(gama2) for e in E}

        with open(precalc_path, 'r') as f:
            f.readline() # ignore first line
            for line in f:
                row = line.split(';')
                name = row[0]
                s = row[1] if row[1] == '' else int(row[1])
                i = row[2] if row[2] == '' else int(row[2])
                j = row[4] if row[4] == '' else int(row[4])
                e = row[5] if row[5] == '' else int(row[5])
                val = row[6]
                if name == 'gama':
                    gama[e][s][i][j] = float(val)

        return gama
