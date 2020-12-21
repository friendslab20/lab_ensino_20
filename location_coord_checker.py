# !/usr/bin/env python
# coding: utf-8

# Sistema Geodésico – SIRGAS2000
# pip install --upgrade fiona
# conda install -c intel fiona geopandas
import fiona


# Determina se um ponto está dentro do polígono
# Polygon é uma lista de pares (x,y).
# Rotina de modelagem geométrica para verificar se um ponto está dentro de um polígono qualquer (côncavo ou convexo: ver livro velho do Rogers)
def Point_Inside_Polygon(x, y, poly):
    n = len(poly)
    inside = False

    p1x, p1y = poly[0]
    for i in range(n + 1):
        p2x, p2y = poly[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside


# Apenas variáveis globais
lstStates = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt', 'pa',
             'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to']
lstGeodeticSystem = ['sirgas']


# Interface para a bilbioteca de verificação
def IsInside(path, latitude, longitude, geodetic_system, state):
    inside = False
    where = 'Fora do Estado '

    # Testa se o indicador de estado está correto
    if not (state in lstStates):
        print('state: escolha a correta designação de estado.')
        print(lstStates)
        return inside, where
    SHP_file = 'MalhaMunicipios/' + state + '/municipios.shp'
    where = where + state

    # Testa se o sistema geodésico está correto
    if not (geodetic_system in lstGeodeticSystem):
        print('geodetic_system: escolha a correta designação de sistema geodésico.')
        print(lstGeodeticSystem)
        return inside, where

        # Testa se os valores de latitude e longitude são float
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except ValueError:
        print('latitude,longitude: valores devem ser float.')

    # Tudo conferido, pronto para rodar
    shapes = fiona.open(SHP_file)
    for s in shapes:
        if s['type'] == 'Feature':
            if s['geometry']['type'] == 'Polygon':
                # Um Polygon é uma lista de anéis, cada anel uma lista de tuplas
                # (x,y) = (Long,Lat)
                for ring in s['geometry']['coordinates']:
                    if Point_Inside_Polygon(longitude, latitude, ring):
                        if state == 'mt':
                           where = s['properties']['NM_MUNICIP'].encode('iso-8859-1').decode('utf-8')
                        else:
                           where = s['properties']['NM_MUNICIP']
                        inside = True

    shapes.close()
    return inside, where


# Exemplo de chamada
# dentro,municipio = IsInside(-22.9518018,-43.1844011,'sirgas','rj')
# dentro,municipio = IsInside(-22.9132525,-43.7261797,'sirgas','rj')
# dentro, municipio = IsInside(-10.781331, -36.993735, 'sirgas', 'se')
# dentro,municipio = IsInside(-7.1464332,-34.9516385,'sirgas','pb')
# print(dentro)
# print(municipio)








# Interface para a bilbioteca de verificação usando List de Estados
def IsInsideByList(path, latitude, longitude, geodetic_system, state_list):
    lstIsInside = []
    lstWhere = []

    for state in state_list:
        bInside,strWhere = IsInside(path,latitude,longitude,geodetic_system,'rj')
        lstIsInside.append(bInside)
        lstWhere.append(strWhere)

    return lstIsInside,lstWhere
# Exemplo de chamada
#estados = ['pb','se','rj']
#dentro,municipio = IsInsideByList('input',-7.1464332,-34.9516385,'sirgas',estados)
#print(dentro)
#print(municipio)