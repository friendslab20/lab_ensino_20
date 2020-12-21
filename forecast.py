import pandas as pd
import numpy as np
import calendar
import statistics
import random
import cx_Oracle
import datetime
from data import *
from classes_folder.menu import menu_insert
import time


def forecast(cod_estudo, cod_modulo,cod_empresa):

    data = Data('forecast')
    data.read(cod_modulo,cod_estudo,cod_empresa) # leitura dos dados
    
    prev = Forecast(data.os_input_forecast, data.services, cod_modulo, cod_estudo, cod_empresa)       
    prev.run()
    
    #prev.strategic_forecast=pd.read_csv('output/PREVISAO_DEMANDA_VOLUME.csv', sep=';') # Comentar - RL
    #prev.tatic_forecast=pd.read_csv('output/PREVISAO_DEMANDA_POR_HORA.csv',sep=';')   # Comentar - RL
    #prev.emergency_forecast=pd.read_csv('output/PREVISAO_DEMANDA_EMERGENCIAL.csv',sep=';')   # COmentar - RL

    prev.strategic_forecast.to_csv('PREVISAO_DEMANDA_VOLUME.csv', sep=';', index=False)   # Comentar - RL
    prev.tatic_forecast.to_csv('PREVISAO_DEMANDA_POR_HORA.csv',sep=';',index=False)   # Comentar - RL
    prev.emergency_forecast.to_csv('PREVISAO_DEMANDA_EMERGENCIAL.csv', sep=';', index=False)   # Comentar - RL


class Forecast:

    def __init__(self, os_input, servicos_input,cod_modulo,cod_estudo,cod_empresa):
        self.os_input = os_input
        self.servicos_input = servicos_input
        self.cod_modulo=cod_modulo
        self.cod_estudo=cod_estudo
        self.cod_empresa=cod_empresa
        self.S = self._get_blocks_service()
        self.E=self._get_emergency_blocks()
        self.meses = [m for m in range(1, 13)]
        self.week=[w for w in range(7)]
        self.horas=[h for h in range(24)]
        self.anos = os_input['ANO_INICIO_OS'].unique()
        self.localidades = os_input['COD_LOCALIDADE_VIRTUAL'].unique()
        self.ano_prev = max(self.anos) + 1
        self.strategic_forecast=None
        self.tatic_forecast=None
        self.emergency_forecast=None

    def run(self):

        try:
            conexao_oracle=AbrirConexao(self.cod_empresa)
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Calculando previsao de volume' where IDT_CONTROLE_PRCSM_ETUDO=" +str(self.cod_estudo)
            #print(str(self.cod_estudo)," codigo do estudo")
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            print("Calculando previsão de volume...")   # Status - RL
            #CALCULO DE PREVISAO VOLUME
            self.strategic_forecast = self._demanda_volume()                        
            
            #CALCULO DE PREVISAO HORARIA
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Calculando previsao horaria' where IDT_CONTROLE_PRCSM_ETUDO=" +str(self.cod_estudo)
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            print("Calculando previsão horária...")   # Status - RL
            self.tatic_forecast = self._demanda_hora(self.strategic_forecast)
            semana_tipica = self._get_semana_tipica(self.tatic_forecast)
            self.tatic_forecast = self.tatic_forecast.append(semana_tipica, sort=True)
            cols = ['IDT_PRMET_PRCSM_MODUL_PREVS', 'COD_LOCALIDADE_VIRTUAL', 'IDT_BLOCO_SERVICO',
                    'ANO_PREVISAO_DEMANDA', 'MES_PREVISAO_DEMANDA', 'DIA_PREVISAO_DEMANDA', 'IDT_DIA_SEMANA',
                    'HOR_PREVISAO_DEMANDA', 'QTD_OS_DEMANDA']
            self.tatic_forecast = self.tatic_forecast[cols]
            
            sql="update controle_processamento_estudo set DSC_MENSAGEM_SITUACAO_PRCSM='Calculando previsao emergencial' where IDT_CONTROLE_PRCSM_ETUDO=" +str(self.cod_estudo)
            conexao_oracle.cursor.execute(sql)
            conexao_oracle.con.commit()
            print("\nCalculando previsão emergencial. Processo demorado, por favor aguarde...")   # Status - RL

            #CALCULO DE PREVISAO EMERGENCIAL
            self.emergency_forecast = self._demanda_emergencial(self.strategic_forecast)                        

            #ESCREVENDO RESULTADOS
            print("Escrevendo resultados...")   # Status - RL
            print("\nforecast.py, linha 85, AT01, Emergency Forecast: ",self.emergency_forecast)   # Remover - RL
            print("\nforecast.py, linha 86, AT01, Emergency Forecast, dtype: ",self.emergency_forecast.dtypes)
            print("\nforecast.py, linha 87, AT01, Emergency Forecast, columns: ",self.emergency_forecast.columns)
            print("\nforecast.py, linha 88, AT02, Demanda por Hora: ",self.tatic_forecast)
            print("\nforecast.py, linha 89, AT02, Demanda por Hora, dtype: ",self.tatic_forecast.dtypes)
            print("\nforecast.py, linha 90, AT02, Demanda por Hora, columns: ",self.tatic_forecast.columns)
            print("\nforecast.py, linha 91, AT03, Volume da Demanda: ",self.strategic_forecast)
            print("\nforecast.py, linha 92, AT03, Volume da Demanda, dtype: ",self.strategic_forecast.dtypes)
            print("\nforecast.py, linha 93, AT03, Volume da Demanda, columns: ",self.strategic_forecast.columns)
            menu_insert(conexao_oracle,self.emergency_forecast,'at01') #emergencial
            menu_insert(conexao_oracle,self.strategic_forecast,'at03')  #volume            
            menu_insert(conexao_oracle,self.tatic_forecast,'at02') #horaria            
            conexao_oracle.con.close()
            print('Terminado')

            conexao_oracle = AbrirConexao(self.cod_empresa)
            df = self._get_dataframe(status=1, cod_estudo=self.cod_estudo, modulo='forecast', id_situacao=2)
            menu_insert(conexao_oracle, df, 'at13')
            conexao_oracle.con.close()

        except:
            df = self._get_dataframe(status=1,cod_estudo=self.cod_estudo,modulo='forecast',id_situacao=3)
            conexao_oracle = AbrirConexao(self.cod_empresa)
            menu_insert(conexao_oracle, df, 'at13')
            conexao_oracle.con.close()


    def _demanda_emergencial(self,strategic_forecast):
        os=self.os_input
        contador=0
        emergency_forecast=pd.DataFrame()
        for cod_bloco in self.E:   # E -> função para os serviços emergenciais
            for loc in self.localidades:                       
                data_locality=os[os['COD_LOCALIDADE_VIRTUAL']==loc]
                if len(self.E[cod_bloco])==1:
                    data_locality = data_locality[data_locality['COD_SERVICO']==self.E[cod_bloco]]
                else:
                    data_locality = data_locality[data_locality['COD_SERVICO'].isin(self.E[cod_bloco])]                  
                if data_locality.shape[0]>10: #se tive poucas OSs n faz mta diferença. Pode travar o codigo                    
                    q=self._split_locality(data_locality)
                    prop_q,clientes_por_q=self._get_proportion(data_locality,q)
                    df,contador=self._get_emergency_forecast(strategic_forecast,prop_q,clientes_por_q,loc,self.ano_prev,contador,cod_bloco)                      
                    emergency_forecast=emergency_forecast.append(df)        
        return emergency_forecast

    def _demanda_volume(self):
        os = self.os_input                
        df = pd.DataFrame()
        for cod_loc in self.localidades:                 
            f_loc = os[os['COD_LOCALIDADE_VIRTUAL'] == cod_loc]                
            for mes in self.meses:
                f_mes = f_loc[f_loc['MES_INICIO_OS'] == mes]
                med = self._get_prev_med(f_mes)
                new_df = self._make_df(med, cod_loc, mes, self.ano_prev,self.cod_modulo)
                df = df.append(new_df, ignore_index=True)        
        return df

    def _demanda_hora(self, demanda_volume):
        P = self._get_proporcao(self.os_input, self.S, demanda_volume)  # P[l][m][w][h][s]
        prev_h = self._get_next_year(P, demanda_volume)
        prev_h = self._apply_correction_factor(demanda_volume, prev_h)   # Descomentar caso dê errado - RL
        return prev_h

    def _get_dataframe(self,status, cod_estudo, modulo, id_situacao):
        data = datetime.datetime.now()
        data = datetime.datetime.strftime(data, '%y/%m/%d %H:%M:%S')

        if modulo == 'forecast':
            if id_situacao == 2:
                msg = 'Processado'
                df = pd.DataFrame({'IND_SITUACAO_MODULO_PREVISAO': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_PREVISAO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
            elif id_situacao == 3:
                    msg = 'Erro no cálculo da previsão '
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
                elif status == 2:
                    msg = 'Erro na simulação mono-objetivo'
                df = pd.DataFrame({'IND_SITUACAO_MODULO_ETTGC': [id_situacao], 'DSC_MENSAGEM_SITUACAO_PRCSM': [msg],
                                   'DTH_RETORNO_MODULO_ESTRATEGICO': [data], 'IDT_CONTROLE_PRCSM_ETUDO': [cod_estudo]})
        return df

    def _get_semana_tipica(self,tatic_forecast):
        columns=self._get_columns()
        df=pd.DataFrame()
        for loc in self.localidades:
            f_loc=tatic_forecast[tatic_forecast['COD_LOCALIDADE_VIRTUAL']==loc]
            for m in self.meses:
                f_m=f_loc[f_loc['MES_PREVISAO_DEMANDA']==m]
                for day in self.week:
                    dia=f_m[f_m['IDT_DIA_SEMANA']==day]['DIA_PREVISAO_DEMANDA'].unique()[0]
                    f_d=f_m[f_m['DIA_PREVISAO_DEMANDA']==dia]
                    aux=f_d[columns]
                    df=df.append(aux)
        df=df.groupby(['COD_LOCALIDADE_VIRTUAL','IDT_DIA_SEMANA','IDT_BLOCO_SERVICO','HOR_PREVISAO_DEMANDA']).mean().reset_index()
        df['MES_PREVISAO_DEMANDA']=0
        df['DIA_PREVISAO_DEMANDA']=0
        df['ANO_PREVISAO_DEMANDA']=self.ano_prev
        df['IDT_PRMET_PRCSM_MODUL_PREVS']=self.cod_modulo
        contador=1 #contador p n travar o banco de dados (unique key)
        for i in df.index:
            df.at[i,'DIA_PREVISAO_DEMANDA']=contador
            if contador>=31:
                contador=1
            else:
                contador+=1
        return df

    def _get_columns(self):
        columns = []
        columns.append('IDT_BLOCO_SERVICO')
        columns.append('HOR_PREVISAO_DEMANDA')
        columns.append('IDT_DIA_SEMANA')
        columns.append('COD_LOCALIDADE_VIRTUAL')
        columns.append('IDT_PRMET_PRCSM_MODUL_PREVS')
        columns.append('QTD_OS_DEMANDA')
        return columns
    #funcoes auxiliares para previsao emergencial
    def _get_emergency_forecast(self,strategic_forecast, prop_q, clientes_por_q, cod_localidade, ano_prev, contador, cod_bloco):
        strategic_forecast = strategic_forecast[strategic_forecast['COD_LOCALIDADE_VIRTUAL'] ==int(cod_localidade)]        
        df = pd.DataFrame()
        for m in prop_q:                     
            soma = 0           
            aux = strategic_forecast[(strategic_forecast['MES_PREVISAO_DEMANDA']==m)&(strategic_forecast['IDT_BLOCO_SERVICO']==cod_bloco)]['QTD_OS_DEMANDA'].sum()  # var aux criada para garantir coincidencia do volume emergencial c volume estrategico                        
            for i in prop_q[m]:
                qtd_os = np.around(prop_q[m][i] * aux)                
                soma += qtd_os
                if qtd_os > 0:
                    df_os, contador =self._get_random_OS(clientes_por_q, m, qtd_os, cod_localidade, i, contador, ano_prev,
                                                    cod_bloco)                                                  
                    if df_os.shape[0]>0:
                        df = df.append(df_os, ignore_index=True)
                        
            aux2 = aux - soma
            if aux2 > 0:
                qtd_os = aux2
                i = self._get_ind(clientes_por_q, m)  # pega indice da quad. c maior proporcao de OS                
                df_os, contador = self._get_random_OS(clientes_por_q, m, qtd_os, cod_localidade, i, contador, ano_prev,cod_bloco)
                if df_os.shape[0]>0:                          
                    df = df.append(df_os, ignore_index=True)        
        return df, contador

    def _get_ind(self,clientes_por_q, mes):

        quadriculas = clientes_por_q[clientes_por_q['MES'] == mes]['QUADRICULA'].unique()
        if quadriculas.size:
            indice = random.choice(quadriculas)
            return indice
        else:
            return False
    def _get_random_OS(self,clientes_por_q, mes, qtd_os, cod_localidade, quadricula, contador, ano_prev, cod_bloco):

        clientes_por_q = clientes_por_q[clientes_por_q['MES'] == mes]
        clientes_por_q = clientes_por_q[clientes_por_q['QUADRICULA'] == quadricula]
        df = pd.DataFrame(columns=['IDT_PRMET_PRCSM_MODUL_PREVS','NUM_OS_VIRTUAL_PREVISAO', 'ANO_CONCLUSAO_OS', 'MES_CONCLUSAO_OS',
                                   'COD_LOCALIDADE_VIRTUAL','IDT_BLOCO_SERVICO','DSC_CONJUNTO_UC_PREVISAO'])
        if clientes_por_q.shape[0]==0:
            return df,contador
        else:
            for i in range(int(qtd_os)):
                num_os = random.choice(clientes_por_q.index)
                cliente_afetado = clientes_por_q.loc[num_os, 'DSC_CDC']
                contador += 1
                df = df.append(pd.DataFrame({'IDT_PRMET_PRCSM_MODUL_PREVS':self.cod_modulo,'NUM_OS_VIRTUAL_PREVISAO': contador, 'ANO_CONCLUSAO_OS': ano_prev, 'MES_CONCLUSAO_OS': mes,
                                             'COD_LOCALIDADE_VIRTUAL': cod_localidade,'IDT_BLOCO_SERVICO':cod_bloco,'DSC_CONJUNTO_UC_PREVISAO': [cliente_afetado]}))
            return df, contador

    def _get_proportion(self,data_locality, q):
        # q[lat_min,long_min,lat_max,long_max]
        data = data_locality.copy()
        meses = [m for m in range(1, 13)]
        prop_q = {m: {i: 0 for i in q} for m in meses}
        clientes_por_q = pd.DataFrame(columns=['NUM_OS','MES', 'DSC_CDC','QUADRICULA'])
        for m in meses:
            data_m = data[data['MES_INICIO_OS'] == m]
            qtd_os = data_m.shape[0]
            for ind in data_m.index:
                lat = data_m.loc[ind, 'NUM_COORDENADA_LATITUDE']
                long = data_m.loc[ind, 'NUM_COORDENADA_LONGITUDE']
                cliente_afetado = data_m.loc[ind, 'DSC_CDC']
                if type(cliente_afetado)==str: #desconsidera OSs sem cliente afetado
                    num_os = data_m.loc[ind, 'NUM_OS']
                    for i in q:
                        if q[i][0] <= lat and lat <= q[i][2] and q[i][1] <= long and long <= q[i][3]:
                            prop_q[m][i] += 1
                            clientes_por_q = clientes_por_q.append(pd.DataFrame({'NUM_OS': [num_os], 'MES': [m], 'DSC_CDC': [cliente_afetado], 'QUADRICULA': [i]}))
                            break
            for i in q:
                if qtd_os > 0:
                    prop_q[m][i] = (prop_q[m][i] / qtd_os)
                else:
                    prop_q[m][i] = 0
        clientes_por_q.set_index('NUM_OS', inplace=True)
        return prop_q, clientes_por_q

    def _split_locality(self,data_locality):

        LatMin, LatMax, LongMin, LongMax = self._get_locality_limits(data_locality)
        h_lat, h_long = self._get_discreet_interval(data_locality)
        h_lat = (LatMax - LatMin) / np.round(((LatMax - LatMin) / h_lat),10)
        h_long = (LongMax - LongMin) / np.round(((LongMax - LongMin) / h_long),10)
        quadriculas = {}
        long = LongMin
        contador = 0
        while long < LongMax:
            long_a = long
            long += h_long
            lat = LatMin
            while lat < LatMax:
                lat_a = lat
                lat += h_lat
                contador += 1
                quadriculas[contador] = [lat_a, long_a, lat, long]
        return quadriculas

    def _get_discreet_interval(self,data_locality):

        lat = np.array(data_locality['NUM_COORDENADA_LATITUDE'])
        long = np.array(data_locality['NUM_COORDENADA_LONGITUDE'])

        std_lat = statistics.stdev(lat)
        std_long = statistics.stdev(long)

        h_lat = 3.49 * std_lat * (len(lat) ** (-1 / 3))
        h_long = 3.49 * std_long * (len(long) ** (-1 / 3))

        return h_lat, h_long

    def _get_locality_limits(self,data_locality):
        'Essa função calcula os limites superiores e inferiores que as variáveis do problema podem assumir'
        X = np.array(data_locality['NUM_COORDENADA_LATITUDE'], dtype=float)
        Y = np.array(data_locality['NUM_COORDENADA_LONGITUDE'], dtype=float)
        LatMin = np.inf
        LatMax = -np.inf
        LongMin = np.inf
        LongMax = -np.inf
        for coord in range(len(X)):
            if X[coord] < LatMin:
                LatMin = X[coord]
            elif X[coord] > LatMax:
                LatMax = X[coord]
            if Y[coord] < LongMin:
                LongMin = Y[coord]
            elif Y[coord] > LongMax:
                LongMax = Y[coord]
        return LatMin, LatMax, LongMin, LongMax

    def _get_emergency_blocks(self):
        "Essa funcao pega os blocos de OSs emergenciais"
        idt_bloco=self.servicos_input[self.servicos_input['IND_TIPO_OCORRENCIA']=='E'].index.unique()
        E = {}
        for i in idt_bloco:
            try:
                E[i]=self.servicos_input.loc[i,'COD_SERVICO'].unique()
            except:
                E[i]=self.servicos_input.loc[i,'COD_SERVICO']
        return E

    # funcoes auxiliares previsao horaria
    def _get_next_year(self,P, demanda_volume):
        "Essa funcao gera a previsao horaria para o proximo ano"
        df = pd.DataFrame()
        lista=[]
        for l in self.localidades:
            for m in self.meses:
                days = calendar.monthrange(self.ano_prev, m)[1]
                for d in range(1, days + 1):
                    w = calendar.weekday(self.ano_prev, m, d)
                    for h in self.horas:
                        for s in self.S:
                            total_m = self._get_total_m(l, m, s,demanda_volume)  # total de servicos bloco=s, mes=m,localidade=loc
                            #qtd_os = total_m * P[l][m][w][h][s] 
                            qtd_os = np.round(total_m * P[l][m][w][h][s], 5)  # Arredondamento - RL
                            hour_date=datetime.time(h,0,0).isoformat()
                            lista.append([self.cod_modulo,l,s,self.ano_prev,m,d,w,hour_date,qtd_os])

        df=pd.DataFrame(columns=['IDT_PRMET_PRCSM_MODUL_PREVS','COD_LOCALIDADE_VIRTUAL','IDT_BLOCO_SERVICO', 'ANO_PREVISAO_DEMANDA', 'MES_PREVISAO_DEMANDA', 'DIA_PREVISAO_DEMANDA', 'IDT_DIA_SEMANA','HOR_PREVISAO_DEMANDA','QTD_OS_DEMANDA'],data=lista)
        return df

    def _apply_correction_factor(self, prev_volume, prev_hora):
        for l in self.localidades:
            for m in self.meses:
                for s in self.S:
                    sum_v = self._get_sum_volume(prev_volume, l, m, s)
                    sum_h = self._get_sum_hora(prev_hora, l, m, s)
                    if sum_h>0:
                        #correction_factor = sum_v / sum_h  
                        correction_factor = np.round(sum_v / sum_h, 5)   # Arredondamento - RL
                        index = self._get_index(prev_hora, l, m, s)
                        aux = prev_hora.loc[index, 'QTD_OS_DEMANDA']
                        prev_hora.loc[index, 'QTD_OS_DEMANDA'] = aux.apply(self._get_correction, args=(correction_factor,))
                    else:
                        correction_factor=0
                        index = self._get_index(prev_hora, l, m, s)
                        aux = prev_hora.loc[index, 'QTD_OS_DEMANDA']
                        prev_hora.loc[index, 'QTD_OS_DEMANDA'] = aux.apply(self._get_correction, args=(correction_factor,))
        return prev_hora

    def _get_correction(self,x, correction_factor):

        x = x * correction_factor

        return x

    def _get_index(self,prev_hora, l, m, s):
        index = prev_hora[(prev_hora['COD_LOCALIDADE_VIRTUAL'] == int(l)) & (prev_hora['MES_PREVISAO_DEMANDA'] == m)&(prev_hora['IDT_BLOCO_SERVICO']==s)].index
        return index

    def _get_sum_hora(self,prev_hora, l, m, s):
        sum_h = prev_hora[(prev_hora['COD_LOCALIDADE_VIRTUAL'] == int(l)) & (prev_hora['MES_PREVISAO_DEMANDA'] == m)&(prev_hora['IDT_BLOCO_SERVICO']==s)]['QTD_OS_DEMANDA'].sum()
        return sum_h

    def _get_sum_volume(self,prev_volume, l, m, s):
        sum_v = prev_volume[(prev_volume['COD_LOCALIDADE_VIRTUAL'] == int(l)) & (prev_volume['MES_PREVISAO_DEMANDA'] == m)&(prev_volume['IDT_BLOCO_SERVICO']==s)]['QTD_OS_DEMANDA'].sum()
        return sum_v

    def _get_proporcao(self,os_input, S, demanda_volume):
        d=demanda_volume.copy()
        os = os_input.copy()
        os['HOR_INICIO_OS'] = os['HOR_INICIO_OS'].apply(self._string_to_hour)
        Plmwhs = {l: {m: {i: {j: {s: 0 for s in S} for j in self.horas} for i in self.week} for m in self.meses} for l in
                  self.localidades}
        D = self._get_D()
        for l in self.localidades:
            f_loc = os[os['COD_LOCALIDADE_VIRTUAL'] == l]
            for mes in self.meses:
                f_mes = f_loc[f_loc['MES_INICIO_OS'] == mes]
                for w in self.week:
                    f_week = f_mes[f_mes['IND_DIA_SEMANA_INICIO_OS'] == w]
                    for h in self.horas:
                        f_h = f_week[f_week['HOR_INICIO_OS'] == h]
                        for s in S:
                            soma_h = self._get_soma_h(f_h, S[s])  # qtd OSs na hora=h,bloco=s, mes=m,localidade=loc
                            if soma_h>0:
                                total_m = self._get_total_m(l, mes, s,demanda_volume)  # total de servicos bloco=s, mes=m,localidade=loc
                                #Plmwhs[l][mes][w][h][s] = (soma_h / total_m) / D[mes][w]
                                Plmwhs[l][mes][w][h][s] = np.round((soma_h / total_m) / D[mes][w], 5)   # Arredondamento - RL
                            else:
                                Plmwhs[l][mes][w][h][s] = 0
        return Plmwhs

    def _get_total_m(self,cod_loc, mes, s, demanda_volume):
        """Essa funcao pega o volume total de Oss para um determinado mes e tipo de servico"""
        d = demanda_volume.copy()
        total_m = d[(d['COD_LOCALIDADE_VIRTUAL'] == int(cod_loc)) & (d['MES_PREVISAO_DEMANDA'] == mes)&(d['IDT_BLOCO_SERVICO']==s)]['QTD_OS_DEMANDA'].iloc[0]
        return total_m

    def _get_soma_h(self,f_h, bloco):
        """Essa funcao pega a soma de servicos em cada hora"""
        try:
            f_s = f_h[f_h['COD_SERVICO'].isin(bloco)]
        except:
            f_s = f_h[f_h['COD_SERVICO']==bloco] #caso de haver somente 1 servico
        if f_s.size:
            soma_h = f_s.groupby('ANO_INICIO_OS')['NUM_OS'].count().sum()
            return soma_h
        else:
            return 0

    def _get_D(self):
        'Essa funcao pega o total de dias da semana para todos os anos'
        D = {m: {} for m in self.meses}
        for y in self.anos:
            for m in self.meses:
                m_length = calendar.monthrange(y, m)[1]
                for d in range(1, m_length + 1):
                    w = calendar.weekday(y, m, d)
                    if w not in D[m]:
                        D[m][w] = 1
                    else:
                        D[m][w] += 1
        return D

    def _string_to_hour(self, hour_string):
        hour_string = int(hour_string.split(':')[0])
        return hour_string

    def _get_blocks_service(self):

        cod_serv=self.servicos_input.index.unique()
        S={}
        for i in cod_serv:
            try:
                S[i]=self.servicos_input.loc[i,'COD_SERVICO'].unique()
            except:
                S[i]=self.servicos_input.loc[i,'COD_SERVICO']

        return S

    def _get_prev_med(self, f_mes):
        med = {}
        for s in self.S:
            try:
                f_s = f_mes[f_mes['COD_SERVICO'].isin(self.S[s])]
            except:
                f_s = f_mes[f_mes['COD_SERVICO']==self.S[s]]
            if f_s.size:
                med_s = f_s.groupby('ANO_INICIO_OS')['NUM_OS'].count().mean()
                med[s] = [np.round(med_s)]
            else:
                med[s] = [0]
        return med

    def _make_df(self, med, cod_loc, mes, ano,cod_modulo):
        df=pd.DataFrame()
        for s in med:
            dic1 = {'IDT_PRMET_PRCSM_MODUL_PREVS':cod_modulo,'COD_LOCALIDADE_VIRTUAL': cod_loc,'IDT_BLOCO_SERVICO':s, 'ANO_PREVISAO_DEMANDA': ano, 'MES_PREVISAO_DEMANDA': mes,'QTD_OS_DEMANDA':med[s]}
            df_aux=pd.DataFrame(dic1,dtype='int64')
            df=df.append(df_aux)
        return df

    def _string_to_list_cod(self, string):
        aux = string.replace('[', '').replace(']', '').split(',')
        lista = []
        for i in aux:
            lista.append(int(i))
        return lista




