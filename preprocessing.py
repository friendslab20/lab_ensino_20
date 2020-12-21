from oss_clustering import cluster_oss
from oss_filter import filter_oss
import pandas as pd

def clustering(cod_estudo, cod_modulo, cod_empresa):
    oss_data = pd.read_csv('input/ENTRADA_CLUSTERIZACAO.csv', sep=';', encoding='latin-1')
    misc_input = pd.read_csv('input/OUTRAS_ENTRADAS.csv', sep=";", encoding='latin-1', index_col='PARAMETRO')
    # # CS04 OK
    # misc_input_raw=menu_select(conexao_oracle, "CS04", cod_estudo)[0]             
    # self.misc_input = pd.DataFrame(columns=['PARAMETRO', 'VALOR'])
    # self.misc_input.loc[0] = ['CLUSTERIZACAO_LOCALIDADE', misc_input_raw.iloc[0]['CLUSTERIZACAO_LOCALIDADE']]
    # self.misc_input.loc[1] = ['CLUSTERIZACAO_NUM_CLUSTERS', misc_input_raw.iloc[0]['CLUSTERIZACAO_NUM_CLUSTERS']]
    # self.misc_input.loc[2] = ['CLUSTERIZACAO_MAX_LOC_VIRT', misc_input_raw.iloc[0]['CLUSTERIZACAO_MAX_LOC_VIRT']]
    loc_exec = int(misc_input.loc['CLUSTERIZACAO_LOCALIDADE','VALOR'])
    k = int(misc_input.loc['CLUSTERIZACAO_NUM_CLUSTERS','VALOR'])
    n = int(misc_input.loc['CLUSTERIZACAO_MAX_LOC_VIRT','VALOR'])

    clustered_oss_data, cluters_centers_data = cluster_oss(oss_data, loc_exec, k, n)
    clustered_oss_data.to_csv("output/SAIDA_CLUSTERIZACAO_OS.csv", index=False, sep=';', encoding='latin-1')
    cluters_centers_data.to_csv("output/SAIDA_CLUSTERIZACAO_CENTROS.csv", index=False, sep=';', encoding='latin-1')

# noinspection DuplicatedCode
# def filter(cod_estudo, cod_modulo):
#     locations_data = pd.read_csv('input/LOCALIDADES_MUNICIPIOS.csv', sep=';', encoding='latin-1')
#     oss_data = pd.read_csv('input/OS_ENTRADA_FILTRO_SIDEC.csv', sep=';', encoding='latin-1')
#     misc_input = pd.read_csv('input/OUTRAS_ENTRADAS.csv', sep=";", encoding='latin-1', index_col='PARAMETRO')
#     # # CS04 OK
#     # misc_input_raw=menu_select(conexao_oracle, "CS04", cod_estudo)[0]             
#     # self.misc_input = pd.DataFrame(columns=['PARAMETRO', 'VALOR'])
#     # self.misc_input.loc[0] = ['FILTRO_UNIDADE_NEGOCIO', misc_input_raw.iloc[0]['FILTRO_UNIDADE_NEGOCIO']]
#     unidade_negocio = misc_input.loc['FILTRO_UNIDADE_NEGOCIO','VALOR'].lower()

#     all_oss_data, not_filtered_data, reregistered_data, filtered_data, errors_data, locations_data = filter_oss(
#         locations_data, oss_data, unidade_negocio)

#     print("Salvando os resultados", flush=True)
#     # writer = pd.ExcelWriter('output/SAIDA_FILTRO.xlsx', engine='xlsxwriter')
#     # not_filtered_data.to_excel(writer, 'mantidos', index=False)
#     not_filtered_data.to_csv('output/SAIDA_FILTRO_MANTIDOS.csv', sep=';', encoding='latin-1', index=False)
#     # reregistered_data.to_excel(writer, 'recadastrados', index=False)
#     errors_data = errors_data.drop(columns=['LOCALIDADE_REAL'])
#     reregistered_data = pd.concat([reregistered_data, errors_data])
#     reregistered_data.to_csv('output/SAIDA_FILTRO_RECADASTRADOS.csv', sep=';', encoding='latin-1', index=False)
#     # filtered_data.to_excel(writer, 'filtrados', index=False)
#     filtered_data.to_csv('output/SAIDA_FILTRO_EXCLUIDOS.csv', sep=';', encoding='latin-1', index=False)
#     # errors_data.to_excel(writer, 'localidade_invalida', index=False)

#     # writer.save()

#     # writer = pd.ExcelWriter('output/OS_ENTRADA_SIDEC.xlsx', engine='xlsxwriter')
#     # all_oss_data.to_excel(writer, index=False)
#     # writer.save()

#     # writer = pd.ExcelWriter('output/LOCALIDADES_MUNICIPIOS_SAIDA.xlsx', engine='xlsxwriter')
#     # locations_data.to_excel(writer, 'LOCALIDADES_MUNICIPIOS', index=False)
#     # writer.save()
