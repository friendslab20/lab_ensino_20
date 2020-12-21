import cx_Oracle
from collections import defaultdict
import sys
import pandas as pd
import unidecode
from geopy import distance

from location_coord_checker import IsInside
from classes_folder.conexao import AbrirConexao

#os_clientes_corretas = []
#os_clientes_redirecionadas = []
#os_clientes_invalidas = []

#localidades_redirecionada = []
cliente_localidades_redirecionada = []
#contador = 0
#con = None
def normalize_string(string):
    return unidecode.unidecode(string).upper().replace("'", ' ').replace('  ', ' ')


def verificarProcessamento(cod_empresa, mesExecucao, cod_processo):
    permiteProcessamento = True
    con = AbrirConexao(cod_empresa)    
    con.cursor.execute('SELECT cpp.idt_parametro_prprc_dado_imptd, cpp.ind_situacao_processamento FROM controle_preparacao_dado_imptd cpp WHERE cpp.mes_ano_referencia = '+mesExecucao+' and cpp.idt_parametro_prprc_dado_imptd = '+ str(cod_processo) +' ORDER BY cpp.idt_parametro_prprc_dado_imptd')
    res = con.cursor.fetchall()
    for row in res:
        if row[1] == 1 or row[1] == 2:
            permiteProcessamento = False

    return permiteProcessamento

def atualizarMensagemProcesso(cod_empresa, mesExecucao, cod_processo, msgmProcessamento):
    con = AbrirConexao(cod_empresa)
    
    sqlUpdate = "UPDATE controle_preparacao_dado_imptd cppp SET cppp.dsc_mensagem = '"+normalize_string(msgmProcessamento)+"' WHERE cppp.mes_ano_referencia = "+mesExecucao+" AND cppp.idt_parametro_prprc_dado_imptd = "+str(cod_processo)
    con.cursor.execute(sqlUpdate)
    con.con.commit()

    return
    
def finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, statusProcessamento, msgmProcessamento,osCorrestas,osRedirecionadas,osForaLocalidade):
    con = AbrirConexao(cod_empresa)    
    sqlUpdate = "UPDATE controle_preparacao_dado_imptd cppp SET cppp.ind_situacao_processamento = '"+str(statusProcessamento)+"', cppp.dth_processamento = SYSDATE, cppp.dsc_mensagem = '"+normalize_string(msgmProcessamento)+"' WHERE cppp.mes_ano_referencia = "+mesExecucao+" AND cppp.idt_parametro_prprc_dado_imptd = "+str(cod_processo)
  
    con.cursor.execute(sqlUpdate)
    con.con.commit()        
    
    objetoRetorno = {"retornoProcessamento": statusProcessamento , "mensagemProcessamento" :  msgmProcessamento, "osCorrestas": len(osCorrestas) , "osRedirecionadas": len(osRedirecionadas) , "osForaLocalidade":len(osForaLocalidade) }
    return objetoRetorno


def iniciarProcessamento(cod_empresa, mesExecucao, cod_processo, sgl_empregado):
    con = AbrirConexao(cod_empresa)
    
    con.cursor.execute('SELECT cpp.idt_parametro_prprc_dado_imptd, cpp.ind_situacao_processamento FROM controle_preparacao_dado_imptd cpp WHERE cpp.mes_ano_referencia = '+mesExecucao+' and cpp.idt_parametro_prprc_dado_imptd = '+ str(cod_processo) +' ORDER BY cpp.idt_parametro_prprc_dado_imptd')
    res = con.cursor.fetchall()
    if len(res) > 0:
        con.cursor.execute("UPDATE controle_preparacao_dado_imptd cppp SET cppp.ind_situacao_processamento = 1, cppp.dth_processamento = SYSDATE, cppp.sgl_empregado_processamento = '"+sgl_empregado+"', cppp.dsc_mensagem = NULL WHERE cppp.mes_ano_referencia = "+mesExecucao+" AND cppp.idt_parametro_prprc_dado_imptd = "+cod_processo)
        con.con.commit()
    else:    
        insert = "INSERT INTO controle_preparacao_dado_imptd (idt_parametro_prprc_dado_imptd, mes_ano_referencia, ind_situacao_processamento, dth_processamento, sgl_empregado_processamento) VALUES ("+cod_processo+",  "+mesExecucao+", 1, SYSDATE, '"+sgl_empregado+"')"
        con.cursor.execute(insert)
        con.con.commit()
        
    return
def listarClientesOS(cod_empresa,mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    con.cursor.execute("select ao.cod_uc, ao.cod_localidade, ao.num_coordenada_latitude, ao.num_coordenada_longitude, mu.sgluf, mu.nommun, ' ' as cod_localidade_anterior from cliente_afetado ao, localidade_mapeamento lm, municipio mu where lm.cod_localidade = ao.cod_localidade and mu.codmun = lm.cod_municipio_ibge and  ao.num_coordenada_latitude != 0 and ao.ind_cliente_validado = 'N' and ao.mes_ano_extracao = " + mesExecucao)
    res = con.cursor.fetchall()

    return res

def listarOS(cod_empresa,mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    con.cursor.execute("select ao.num_os, ao.cod_localidade, ao.num_coordenada_latitude, ao.num_coordenada_longitude, mu.sgluf, mu.nommun, ' ' as cod_localidade_anterior from atendimento_os ao, localidade_mapeamento lm, municipio mu where lm.cod_localidade = ao.cod_localidade and mu.codmun = lm.cod_municipio_ibge and  ao.num_coordenada_latitude != 0 and ao.ind_os_validada = 'N' and ao.mes_ano_extracao = " + mesExecucao)
    res = con.cursor.fetchall()

    return res

def atualizarClientesOsValidas(cod_empresa,mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    sqlUpdate = "BEGIN "
    sqlUpdate = sqlUpdate + " UPDATE cliente_afetado SET ind_cliente_validado = 'S', COD_LOCALIDADE_VALIDADA = COD_LOCALIDADE  where mes_ano_extracao = " +mesExecucao + " and ind_cliente_validado NOT IN ('E', 'S'); "
    sqlUpdate = sqlUpdate + " END;"
    con.cursor.execute(sqlUpdate)
    con.con.commit()

    return

def atualizarOsValidas(cod_empresa,mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    sqlUpdate = "BEGIN "
    sqlUpdate = sqlUpdate + " UPDATE atendimento_os SET ind_os_validada = 'S', COD_LOCALIDADE_VALIDADA = COD_LOCALIDADE where mes_ano_extracao = " +mesExecucao + " and ind_os_validada NOT IN ('E', 'S'); "
    sqlUpdate = sqlUpdate + " END;"
    con.cursor.execute(sqlUpdate)
    con.con.commit()

    return

def atualizarClientesOsRecadastradas(cod_empresa,oss_list,mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    blocoCommit = 0
    sqlUpdate = "BEGIN "
    for row in oss_list:
        blocoCommit =  blocoCommit + 1
        sqlUpdate = sqlUpdate + " UPDATE cliente_afetado SET ind_cliente_validado = 'S', COD_LOCALIDADE_VALIDADA = "+ row[6]+" where cod_uc = " + row[0] + " and  mes_ano_extracao = " + mesExecucao + "; "
        if blocoCommit > 1000:
            sqlUpdate = sqlUpdate +  " END;"
            con.cursor.execute(sqlUpdate)
            con.con.commit()
            sqlUpdate = "BEGIN "
            blocoCommit = 0

    if sqlUpdate != "BEGIN ":
        sqlUpdate = sqlUpdate + " END;"
        con.cursor.execute(sqlUpdate)
        con.con.commit()
    
    return

def atualizarOsRecadastradas(cod_empresa,oss_list):
    con = AbrirConexao(cod_empresa)
    
    blocoCommit = 0
    sqlUpdate = "BEGIN "
    for row in oss_list:
        blocoCommit =  blocoCommit + 1
        sqlUpdate = sqlUpdate + " UPDATE atendimento_os SET ind_os_validada = 'S',  COD_LOCALIDADE_VALIDADA = "+ row[6]+" where num_os = " + row[0] + "; "
        if blocoCommit > 1000:
            sqlUpdate = sqlUpdate + " END;"
            con.cursor.execute(sqlUpdate)
            con.con.commit()
            sqlUpdate = "BEGIN "
            blocoCommit = 0

    if sqlUpdate != "BEGIN ":
        sqlUpdate = sqlUpdate + " END;"
        con.cursor.execute(sqlUpdate)
        con.con.commit()
    
    return

def atualizarClientesOsForaLocalidade(cod_empresa,oss_list, mesExecucao):
    con = AbrirConexao(cod_empresa)
    
    blocoCommit = 0
    sqlUpdate = "BEGIN "
    for row in oss_list:
        blocoCommit =  blocoCommit + 1
        sqlUpdate = sqlUpdate + " UPDATE cliente_afetado SET ind_cliente_validado = 'E', COD_LOCALIDADE_VALIDADA = COD_LOCALIDADE  where cod_uc = " + row[0] + " and  mes_ano_extracao = " + mesExecucao+ "; "
        if blocoCommit > 1000:
            sqlUpdate = sqlUpdate +  " END;"
            con.cursor.execute(sqlUpdate)
            con.con.commit()
            sqlUpdate = "BEGIN "
            blocoCommit = 0

    if sqlUpdate != "BEGIN ":
        sqlUpdate = sqlUpdate + " END;"
        con.cursor.execute(sqlUpdate)
        con.con.commit()

    return

def atualizarOsForaLocalidade(cod_empresa,oss_list):
    con = AbrirConexao(cod_empresa)
    
    blocoCommit = 0
    sqlUpdate = "BEGIN "
    for row in oss_list:
        blocoCommit =  blocoCommit + 1
        sqlUpdate = sqlUpdate + " UPDATE atendimento_os SET ind_os_validada = 'E', COD_LOCALIDADE_VALIDADA = COD_LOCALIDADE  where num_os = " + row[0] + "; "
        if blocoCommit > 1000:
            sqlUpdate = sqlUpdate +  " END;"
            con.cursor.execute(sqlUpdate)
            con.con.commit()
            sqlUpdate = "BEGIN "
            blocoCommit = 0

    if sqlUpdate != "BEGIN ":
        sqlUpdate = sqlUpdate + " END;"
        con.cursor.execute(sqlUpdate)
        con.con.commit()

    return

def consultarLocalidadeRedirecionada(cod_empresa,city_name,localidades_redirecionada):
    
    cod_localidade_redirecionada = 0
    if len(localidades_redirecionada) > 0:
         for row in localidades_redirecionada:
             if row[1] == city_name:
                cod_localidade_redirecionada = row[0]
                break

    if cod_localidade_redirecionada == 0:
        con = AbrirConexao(cod_empresa)

        sql = "select max(cod_localidade) cod_localidade, nommun from (select lm.cod_localidade, nommun  from localidade_mapeamento lm, municipio mu where mu.codmun = lm.cod_municipio_ibge and mu.nommun = :city_name "
        sql = sql + " union all "
        sql = sql + "select lm.cod_localidade, mu.nommun from municipio_sem_localidade lm, municipio mu where mu.codmun = lm.cod_municipio_ibge and mu.nommun = :city_name) "
        sql = sql + " group by nommun "   
        
        con.cursor.execute(sql, city_name=city_name)
        res = con.cursor.fetchall()
        if len(res) > 0:
            localidades_redirecionada.append(res[0])
            cod_localidade_redirecionada = res[0][0]

    return cod_localidade_redirecionada,localidades_redirecionada

def validarAtendimentoOSPY(cod_empresa, mesExecucao, cod_processo, sgl_empregado):
    try:
        input_folder = "inputPablo"
        permiteProc = verificarProcessamento(cod_empresa,mesExecucao, cod_processo)
        #global os_corretas
        #global os_redirecionadas
        #global os_invalidas
        os_corretas = []
        os_redirecionadas = []
        os_invalidas = []
        localidades_redirecionada=[]

        if permiteProc:
        
            # print("======== Vai iniciar processamento   \n")
            iniciarProcessamento(cod_empresa,mesExecucao, cod_processo, sgl_empregado)
            # print("======== Vai listar OS   \n")           
            msgmProcessamento = "Buscando OSs"            
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)                
            oss_list = listarOS(cod_empresa,mesExecucao)
            msgmProcessamento = "Iniciando Loop"            
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)    
            contador=0 
            for row in oss_list:
                msgmProcessamento = "Quantidade de OSs validadas: " + str(contador)           
                atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)   
                contador = contador + 1
                row = list(row) 
                # print("======== Vai chamar IsInside \n")                 
                                  
                is_inside, city_name = IsInside(input_folder, float(row[2]),
                                                float(row[3]), 'sirgas', row[4].rstrip().lower())
                                                
                # print("======== Saiu da IsInside \n")                                                                 
                city_name_original = city_name
                city_name = normalize_string(city_name)
                if not is_inside:  # coordenadas fora do estado
                    # print("======== Vai montar os_invalidas \n")                 
                    os_invalidas.append(row)
                elif city_name == normalize_string(row[5]):  # coordenadas dentro da localidade designada
                    # print("======== Vai montar os_corretas \n")                                 
                    os_corretas.append(row)
                else:  # recadastramento
                    # print("======== Vai consultarLocalidadeRedirecionada \n")                   
                    localidade_redirecionada,localidades_redirecionada = consultarLocalidadeRedirecionada(cod_empresa, city_name_original,localidades_redirecionada)
                    if localidade_redirecionada != 0:
                        row[6] = localidade_redirecionada
                        # print("======== Vai montar os_redirecionadas \n")                          
                        os_redirecionadas.append(row)
                    else:
                        msgmProcessamento = "Localidade Redirecionada nao existe: " +  city_name_original + " - OS invalida: " + row[0]  
                        # print("======== Vai finalizarProcessamento - Localidade Resdirecionada nao existe \n")   
                        return finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, 3, msgmProcessamento,[],[],[])
            # print("======== Vai atualizarOsRecadastradas \n") 
            msgmProcessamento = "Atualizando OSs Recadastradas."            
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)          
            atualizarOsRecadastradas(cod_empresa,os_redirecionadas)
            # print("======== Vai atualizarOsForaLocalidade \n")  
            msgmProcessamento = "Atualizando OSs Fora Localidade."            
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)              
            atualizarOsForaLocalidade(cod_empresa,os_invalidas)
            # print("======== Vai atualizarOsValidas \n")  
            msgmProcessamento = "Atualizando OSs Validas."            
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)              
            atualizarOsValidas(cod_empresa,mesExecucao)
            # print("======== Vai validarClienteOSPY \n")   
            os_clientes_corretas,os_clientes_redirecionadas,os_clientes_invalidas=validarClienteOSPY(cod_empresa, mesExecucao, cod_processo, sgl_empregado)
            msgmProcessamento = "Processamento Finalizado com Sucesso: com " + str(len(os_corretas)) + " OSs Corretas, "+ str(len(os_redirecionadas)) + " OSs Redirecionadas, " + str(len(os_invalidas)) + " OSs Invalidas, " +  str(len(os_clientes_corretas)) + " OSs Cliente Corretas, "+ str(len(os_clientes_redirecionadas)) + " OSs Cliente Redirecionadas, " + str(len(os_clientes_invalidas)) + " OSs Cliente Invalidas."
            return finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, 2, msgmProcessamento,os_corretas,os_redirecionadas, os_invalidas)
        else:
            objetoRetorno = {"retornoProcessamento": 5 , "mensagemProcessamento" :  "Processamento não iniciado, já existe processo em andamento/finalizado para este periodo", "osCorrestas": 0 , "osRedirecionadas": 0 , "osForaLocalidade":0, "osClientesCorrestas": 0 , "osClientesRedirecionadas": 0 , "osClientesForaLocalidade":0 }
            js = json.dumps(objetoRetorno)
            resp = Response(js, status=200, mimetype='application/json')
            return resp

    except: 
        msgmProcessamento = "Processo finalizado com erro inesperado: "+ str(sys.exc_info()[1])
        return finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, 3, msgmProcessamento,[],[],[])


def validarCoordenadasPY(un, cod_latitude, cod_longitude, nom_localidade ):
    
    retorno = "false"
    input_folder = "input"

    is_inside, city_name = IsInside(input_folder, float(cod_latitude),
                                        float(cod_longitude), 'sirgas', un)
    city_name = normalize_string(city_name)
    if not is_inside:  # coordenadas fora do estado
        retorno = "false"
    elif city_name == normalize_string(nom_localidade):  # coordenadas dentro da localidade designada
        retorno = "true"
    else:  # recadastramento
        retorno = "recadastramento"

    objetoRetorno = {"retornoValidacao": retorno , "nom_localidade_validacao" :  city_name }
    return objetoRetorno

def validarClienteOSPY(cod_empresa, mesExecucao, cod_processo, sgl_empregado):
    try:
        input_folder = "inputPablo"
        #global os_clientes_corretas
        #global os_clientes_redirecionadas
        #global os_clientes_invalidas 
        os_clientes_corretas = []
        os_clientes_redirecionadas = []
        os_clientes_invalidas = []
        localidades_redirecionada=[]
        oss_list = listarClientesOS(cod_empresa,mesExecucao)
        #global contador 
        contador = 0
        for row in oss_list:
            msgmProcessamento = "Quantidade de Clientes validados: " + str(contador)           
            atualizarMensagemProcesso(cod_empresa,mesExecucao, cod_processo, msgmProcessamento)   
            contador = contador + 1
            row = list(row) 
            is_inside, city_name = IsInside(input_folder, float(row[2]),
                                            float(row[3]), 'sirgas', row[4].rstrip().lower())
            city_name_original = city_name
            city_name = normalize_string(city_name)
            if not is_inside:  # coordenadas fora do estado
                os_clientes_invalidas.append(row)
            elif city_name == normalize_string(row[5]):  # coordenadas dentro da localidade designada
                os_clientes_corretas.append(row)
            else:  # recadastramento
                cliente_localidade_redirecionada,localidades_redirecionada = consultarLocalidadeRedirecionada(cod_empresa, city_name_original,localidades_redirecionada)
                if cliente_localidade_redirecionada != 0:
                    row[6] = cliente_localidade_redirecionada
                    os_clientes_redirecionadas.append(row)
                else:
                    msgmProcessamento = "Localidade Redirecionada nao existe: " +  city_name_original
                    return finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, 3, msgmProcessamento,[],[],[])

        atualizarClientesOsRecadastradas(cod_empresa,os_clientes_redirecionadas,mesExecucao)
        atualizarClientesOsForaLocalidade(cod_empresa,os_clientes_invalidas,mesExecucao)
        atualizarClientesOsValidas(cod_empresa,mesExecucao)
        return os_clientes_corretas,os_clientes_redirecionadas,os_clientes_invalidas
    except: 
        msgmProcessamento = "Processo finalizado com erro inesperado: "+ str(sys.exc_info()[1])
        return finalizarProcessamento(cod_empresa, mesExecucao, cod_processo, 3, msgmProcessamento,[],[],[])
