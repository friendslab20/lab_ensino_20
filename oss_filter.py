from collections import defaultdict

import pandas as pd
import unidecode
from geopy import distance

from location_coord_checker import IsInside
from location_coord_checker import IsInsideByList   # Novo - RL


def normalize_string(string):
    return unidecode.unidecode(string).upper().replace("'", ' ').replace('  ', ' ')


def __main__():
    locations_data = pd.read_csv('input/LOCALIDADES_MUNICIPIOS.csv', sep=';', encoding='latin-1')
    oss_data = pd.read_csv('input/OS_ENTRADA_SIDEC.csv', sep=';', encoding='latin-1')
    all_oss_data, not_filtered_data, reregistered_data, filtered_data, errors_data, locations_data = filter_oss(
        locations_data, oss_data, 'MT')

    print("Salvando os resultados", flush=True)
    writer = pd.ExcelWriter('output/OSS_SAIDA.xlsx', engine='xlsxwriter')
    not_filtered_data.to_excel(writer, 'mantidos', index=False)
    reregistered_data.to_excel(writer, 'recadastrados', index=False)
    filtered_data.to_excel(writer, 'filtrados', index=False)
    errors_data.to_excel(writer, 'localidade_invalida', index=False)

    writer.save()

    writer = pd.ExcelWriter('output/OS_ENTRADA_SIDEC.xlsx', engine='xlsxwriter')
    all_oss_data.to_excel(writer, index=False)
    writer.save()

    writer = pd.ExcelWriter('output/LOCALIDADES_MUNICIPIOS_SAIDA.xlsx', engine='xlsxwriter')
    locations_data.to_excel(writer, 'LOCALIDADES_MUNICIPIOS', index=False)
    writer.save()


def filter_oss(locations_data, oss_data, un):
    oss_list = oss_data.to_dict('records')

    loc_city_name = {row['CODLCD']: normalize_string(row['NOMEMUN']) for _, row in locations_data.iterrows()}

    print("Filtro a nível de estado", flush=True)
    # filtra oss que estão fora do estado
    not_filtered = []
    filtered = []
    # separa oss que estão em um municipio com mais de uma localidade para serem recadastradas posteriormente
    to_reregister = []
    
    # Compara lista de estados com uma UN especifica (RJ atualmente). Comment - RL
    # Ver linha 104 de location_coord_checker.py Comment - RL
    #is_inside_un, city_list = IsInsideByList('input',float(row['NUM_COORDENADA_LATITUDE']),
    #                                         float(row['NUM_COORDENADA_LONGITUDE']), 'sirgas', un)   # Novo - RL
    
    
    for row in oss_list:
        is_inside, city_name = IsInside('input', float(row['NUM_COORDENADA_LATITUDE']),
                                        float(row['NUM_COORDENADA_LONGITUDE']), 'sirgas', un)
        city_name = normalize_string(city_name)
        
        if not is_inside:  # coordenadas fora do estado
            filtered.append(row)
        elif city_name == loc_city_name[row['COD_LOCALIDADE']]:  # coordenadas dentro da localidade designada
            not_filtered.append(row)
        else:  # recadastramento
            row['COD_LOCALIDADE_ORIGEM'] = row['COD_LOCALIDADE']
            to_reregister.append(row)

    filtered_data = pd.DataFrame(filtered, columns=oss_data.columns)
    not_filtered_data = pd.DataFrame(not_filtered, columns=oss_data.columns)
    
    # Salva resultado do filtro. Para remover depois
    #city_name.to_csv('OSS_FILTER_CITY_NAME.csv', sep=';', index=False)   # Remover - RL
    
    # info das localidades
    locs_with_os = not_filtered_data['COD_LOCALIDADE'].unique()
    locations_data = locations_data[locations_data['CODLCD'].isin(locs_with_os)]
    locations_list = locations_data.to_dict('records')
    city_locs = defaultdict(list)
    for _, row in locations_data.iterrows():
        city_locs[row['CODMUN']].append(row['CODLCD'])
    city_name_to_city_id = {normalize_string(row['NOMEMUN']): row['CODMUN'] for _, row in locations_data.iterrows()}

    # já recadastra oss cujas coordenadas estão em um municipio com apenas uma localidade
    reregistered = []
    # oss cujo municipio não está na lista de municipios
    errors = []
    pending = []
    print("Recadastramento de OS's em municípios com apenas uma localidade")
    for row in to_reregister:
        is_inside, city_name = IsInside('input', float(row['NUM_COORDENADA_LATITUDE']),
                                        float(row['NUM_COORDENADA_LONGITUDE']), 'sirgas', un)
        city_name = normalize_string(city_name)
        city_id = city_name_to_city_id.get(city_name, None)

        if city_id is None:
            row['LOCALIDADE_REAL'] = city_name
            errors.append(row)
            continue

        if len(city_locs[city_id]) == 1:  # municipio tem apenas uma localidade
            # loc_ori = row['COD_LOCALIDADE']
            row['COD_LOCALIDADE'] = city_locs[city_id][0]
            # print(row['NUM_OS'], loc_ori, row['COD_LOCALIDADE'] )
            reregistered.append(row)
        else:
            pending.append(row)

    # calculo dos centroides
    grouped = not_filtered_data.groupby('COD_LOCALIDADE')
    # latitudes = grouped['NUM_COORDENADA_LATITUDE'].apply(pd.to_numeric).mean().to_dict()
    latitudes = grouped['NUM_COORDENADA_LATITUDE'].mean().to_dict()
    # longitudes = grouped['NUM_COORDENADA_LONGITUDE'].apply(pd.to_numeric).mean().to_dict()
    longitudes = grouped['NUM_COORDENADA_LONGITUDE'].mean().to_dict()
    for loc in locations_list:
        loc['LATITUDE'] = latitudes.get(loc['CODLCD'], None)
        loc['LONGITUDE'] = longitudes.get(loc['CODLCD'], None)
    locations = pd.DataFrame(locations_list, columns=(locations_data.columns.tolist() + ['LATITUDE', 'LONGITUDE']))

    print("Recadastramento de OS's em municípios com múltiplas localidades por KNN ", flush=True)
    # recadastramento por knn
    for row in pending:
        oss_lat = float(row['NUM_COORDENADA_LATITUDE'])
        oss_long = float(row['NUM_COORDENADA_LONGITUDE'])
        is_inside, city_name = IsInside('input', oss_lat, oss_long, 'sirgas', un)
        city_name = normalize_string(city_name)
        city_id = city_name_to_city_id[city_name]

        city_oss = not_filtered_data[not_filtered_data['COD_LOCALIDADE'].isin(city_locs[city_id])]

        city_oss = city_oss.apply(lambda row: pd.Series({
            'cod_loc': int(row['COD_LOCALIDADE']),
            'distance': distance.distance((oss_lat, oss_long), (
                float(row['NUM_COORDENADA_LATITUDE']), float(row['NUM_COORDENADA_LONGITUDE']))).meters
        }), axis=1).sort_values(by='distance').head(5)
        loc_id = int(city_oss['cod_loc'].mode()[0])

        row['COD_LOCALIDADE'] = loc_id
        reregistered.append(row)

    print("Recadastramento de OS's fora do município por KNN em dois níveis (OS's em cidades desconhecidas) ",
          flush=True)
    # recadastramento das oss que a localidade real não foi encontrada na tabela de municipios
    # o recadastramento é feito por knn das 3 localidades mais proximas
    for row in errors:
        oss_lat = float(row['NUM_COORDENADA_LATITUDE'])
        oss_long = float(row['NUM_COORDENADA_LONGITUDE'])
        closer_locations = locations.apply(lambda location: pd.Series({
            'loc': int(location['CODLCD']),
            'd': distance.distance((oss_lat, oss_long),
                                   (float(location['LATITUDE']), float(location['LONGITUDE']))).meters,
        }), axis=1).sort_values(by='d').head(3)
        closer_locations = closer_locations['loc'].values

        locs_oss = not_filtered_data[not_filtered_data['COD_LOCALIDADE'].isin(closer_locations)]
        locs_oss = locs_oss.apply(lambda row: pd.Series({
            'cod_loc': int(row['COD_LOCALIDADE']),
            'distance': distance.distance((oss_lat, oss_long), (
                float(row['NUM_COORDENADA_LATITUDE']), float(row['NUM_COORDENADA_LONGITUDE']))).meters
        }), axis=1).sort_values(by='distance').head(5)
        loc_id = int(locs_oss['cod_loc'].mode()[0])
        row['COD_LOCALIDADE'] = loc_id

    reregistered_data = pd.DataFrame(reregistered, columns=oss_data.columns.tolist() + ['COD_LOCALIDADE_ORIGEM'])
    errors_data = pd.DataFrame(errors, columns=oss_data.columns.tolist() + ['COD_LOCALIDADE_ORIGEM', 'LOCALIDADE_REAL'])
    all_oss_data = pd.concat([not_filtered_data[oss_data.columns], reregistered_data[oss_data.columns]])

    return all_oss_data, not_filtered_data, reregistered_data, filtered_data, errors_data, locations


if __name__ == "__main__":
    __main__()
