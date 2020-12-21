import pandas as pd
from sklearn.cluster import MeanShift, KMeans
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
# from mpl_toolkits.basemap import Basemap
import numpy as np
import random
import sys


# def __main__():
#     oss_data = pd.read_csv('input/OSS_SAIDA.csv', sep=';', encoding='latin-1')
#     clustered_oss_data, cluters_centers_data = cluster_oss(oss_data)
#     clustered_oss_data.to_csv("output/clustered_oss.csv", index=False, sep=';', encoding='latin-1')
#     cluters_centers_data.to_csv("output/clusters_centers.csv", index=False, sep=';', encoding='latin-1')


def cluster_oss(oss_data, loc_exec, k, n):
    max_clustering_oss = 50000

    oss = oss_data.loc[:, ['NUM_OS', 'NUM_COORDENADA_LATITUDE', 'NUM_COORDENADA_LONGITUDE']].values

    args = get_parameters(sys.argv)

    groups = oss_data.groupby('COD_LOCALIDADE').groups
    if loc_exec != -1:
        if loc_exec not in groups:
            print('Não existe oss para a localidade informada')
            exit()
        groups = {loc_exec: groups[loc_exec]}

    oss_per_loc = [(loc, oss[num_oss]) for loc, num_oss in groups.items()]

    def sort_oss_per_loc(val):
        return len(val[1])
    oss_per_loc.sort(key=sort_oss_per_loc, reverse=True)  # ordena por localidades com maior numero de oss

    solver = None
    if k != 0:
        solver = KMeans(n_clusters=k)
    else:
        solver = MeanShift()

    remaining_locs = len(oss_per_loc)
    # max_locs = remaining_locs * 20
    max_locs = n
    if max_locs < remaining_locs:
        print("Numero máximo de localidades é menor que o numero real de localidades.")
        exit()

    num_locs = 0
    maxed = False

    oss_out = []
    clusters_centers = []

    for loc, oss in oss_per_loc:
        remaining_locs -= 1

        # separa oss que excedem o numero máximo para clusterizacao
        # essas oss irão para o cluster cujo centroide seja mais proximo
        oss2 = None
        if len(oss) > max_clustering_oss:
            np.random.shuffle(oss)
            oss2 = oss[max_clustering_oss:]
            oss = oss[:max_clustering_oss]

        cods_oss = oss[:, 0]
        coords = oss[:, 1:3]

        print("Clusterizando localidade " + str(loc) + ' com ' + str(len(coords)) + ' os\'s')
        clustering_data = solver.fit(coords)
        labels = clustering_data.labels_
        n_labels = len(set(labels))
        centers = clustering_data.cluster_centers_

        if not maxed and num_locs + n_labels + remaining_locs > max_locs:
            clustering_data = KMeans(n_clusters=(max_locs - num_locs - remaining_locs)).fit(coords)
            labels = clustering_data.labels_
            centers = clustering_data.cluster_centers_
            n_labels = len(centers)
            maxed = True
            solver = KMeans(n_clusters=1)

        num_locs += n_labels

        # plotmap(coords, labels, loc)

        for i in range(len(coords)):
            oss_out.append([int(cods_oss[i]), "{}-{}".format(loc, labels[i])])

        for i in range(len(centers)):
            clusters_centers.append(["{}-{}".format(loc, i), centers[i][0], centers[i][1]])

        # para cada oss excedente, verifica qual o cluster que tem centroide mais proximo
        # insere a oss neste cluster
        if oss2 is not None:
            cods_oss = oss2[:, 0]
            coords = oss2[:, 1:3]

            for i in range(len(oss2)):
                coord = coords[i]
                min_dist = float('inf')
                cluster = None

                for j in range(len(centers)):
                    center = centers[j]
                    dist = (coord[0] - center[0])**2 + (coord[1] - center[1])**2
                    if dist < min_dist:
                        min_dist = dist
                        cluster = j

                oss_out.append([int(cods_oss[i]), "{}-{}".format(loc, cluster)])

    clustered_oss_data = pd.DataFrame(oss_out, columns=['NUM_OS', 'COD_LOCALIDADE_VIRTUAL'])

    cluters_centers_data = pd.DataFrame(clusters_centers, columns=['COD_LOCALIDADE_VIRTUAL', 'NUM_COORDENADA_LATITUDE', 'NUM_COORDENADA_LONGITUDE'])

    return clustered_oss_data, cluters_centers_data


def rand_color():
    return "#%06x" % random.randint(0, 0xFFFFFF)


def plotmap(coords, labels, loc):
    n_clusters = np.amax(labels) + 1
    colors = [rand_color() for _ in range(n_clusters)]

    min_lat, min_lon = np.amin(coords, axis=0)
    max_lat, max_lon = np.amax(coords, axis=0)

    offset = 0.02
    fig, ax = plt.subplots()
    m = Basemap(projection='mill', llcrnrlat=min_lat - offset, llcrnrlon=min_lon - offset, urcrnrlat=max_lat + offset,
                urcrnrlon=max_lon + offset, resolution='l')
    m.ax = ax
    shp = m.readshapefile('input/gadm36_BRA_shp/gadm36_BRA_2', 'states', drawbounds=True)
    for nshape, seg in enumerate(m.states):
        poly = Polygon(seg, facecolor='0.75', edgecolor='k')
        ax.add_patch(poly)

    lats = [[] for _ in range(n_clusters)]
    lons = [[] for _ in range(n_clusters)]

    for i in range(len(coords)):
        lats[labels[i]].append(coords[i][0])
        lons[labels[i]].append(coords[i][1])

    for cluster in range(n_clusters):
        x, y = m(lons[cluster], lats[cluster])
        m.plot(x, y, 'bo', markersize=2, color=colors[cluster])

    # for i in range(len(coords)):
    #     x, y = m(float(coords[i][1]), float(coords[i][0]))
    #     m.plot(x, y, marker='o', markersize=3, color=colors[labels[i]])

    # plt.show()
    plt.savefig('output/' + str(loc) + '.png')
    plt.close()


def get_parameters(argv):
    args = {}
    if(len(argv) & 1) == 0:  # se tem um numero par de argumentos
        print("Número de argumentos invalidos")
        exit()

    p = 1

    while p < len(argv):
        if argv[p][0] != '-':
            print("Argumento invalido: {}".format(argv[p]))
            exit()
        try:
            args[argv[p][1:]] = int(argv[p+1])
        except ValueError:
            print("Argumento invalido: {}".format(argv[p+1]))
            exit()
        p += 2

    return args


if __name__ == "__main__":
    __main__()
