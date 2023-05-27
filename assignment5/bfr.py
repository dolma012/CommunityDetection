import os
import random
import time
import math
from collections import Counter
import json
import copy
import math
import argparse
import time
import random
import os
import json
from sklearn.cluster import KMeans


import numpy as np

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A1T1')
    # parser.add_argument('--community_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    parser.add_argument('--input_path', type=str, default='./hw5/test2/')
    parser.add_argument('--n_cluster', type=int, default=10)
    parser.add_argument('--out_file1', type=str, default='out_1.json')
    parser.add_argument('--out_file2', type=str, default='out_1.csv')
    args = parser.parse_args()

final_dict = {}  
input_path = args.input_path
n_cluster = args.n_cluster
out_file1 = args.out_file1
out_file2 = args.out_file2
final_ds_dict = {}
cs_dict = {}  
final_rs_dict = {} 
ds_pt_dict = {}  
cs_pt_dict = {} 
path_array = []
files= os.listdir(input_path)
fst = input_path+ 'data0.txt'
path_array.append(fst)
for i in files:
    cur = input_path+ i
    if cur == fst:
        continue
    path_array.append(cur)

def get_stat_dist(point, stat):
    SUM = np.array(stat['SUM'])
    SUMSQ = np.array(stat['SUMSQ'])
    N = stat['N']
    avg = SUM / N
    std = np.sqrt(SUMSQ / N - (SUM / N) ** 2)
    distance = np.sqrt(np.sum(((point - avg) / np.maximum(0.000001, std)) ** 2))
    return distance

def calculate_k_thresh(round):
    if round < 2000000:
        return 35
    else:
        return 30
    
def calculate_m(n_cluster):
    return 1 if n_cluster < 8 else 1


def run_kmeans(dict_kmeans, n_cluster, d=1):
    point_num = len(dict_kmeans)
    its = d * point_num * n_cluster
    k_thresh = calculate_k_thresh(its)
    m = calculate_m(n_cluster)
    X = [data_feature for data_index, data_feature in dict_kmeans]
    k = min(np.ceil(n_cluster * m), np.ceil(point_num / 2)).astype(int)
    kmeans = KMeans(n_clusters=k, init='k-means++', max_iter=k_thresh, random_state=0)
    kmeans.fit(X)
    labels = kmeans.labels_
    cent_dict = {}
    for i, label in enumerate(labels):
        if label not in cent_dict:
            cent_dict[label] = []
        data_index = dict_kmeans[i][0]  # Get the data_index from the tuple
        cent_dict[label].append(data_index)
    return cent_dict

def process_rows(rows):
    processed_rows = []
    for row in rows:
        if isinstance(row, str):
            row = row.strip().split(',')
            row = [row[0]] + [float(x) for x in row[1:]]
        processed_rows.append(row)
    return processed_rows

def initialize_dicts(rows):
    indices = {d[0]: d[1:] for d in rows}
    unass_pts=[]
    ass_pts=[]
    new_point_list = np.array(list(indices.values()))
    median_list = np.median(new_point_list, axis=0)
    std_list = np.std(new_point_list, axis=0)
    scores = np.abs((new_point_list - median_list) / np.maximum(0.00001, std_list))
    count_pts = np.any(scores > 10, axis=1)
    for i, (data_index, data_feature) in enumerate(indices.items()):
        if count_pts[i]:
            final_rs_dict[data_index] = data_feature
        elif random.random() < 0.2:
            unass_pts.append((data_index, data_feature))
        else:
            ass_pts.append((data_index, data_feature))
    return indices, unass_pts, ass_pts

def update_dicts(final_ds_dict, ds_pt_dict, key, point_index, pt_vec):
    if key in final_ds_dict:
        final_ds_dict[key]['N'] += 1
        final_ds_dict[key]['SUM'] = np.add(final_ds_dict[key]['SUM'], pt_vec)
        final_ds_dict[key]['SUMSQ'] = np.add(final_ds_dict[key]['SUMSQ'], np.square(pt_vec))
    else:
        final_ds_dict[key] = {'N': 1, 'SUM': pt_vec, 'SUMSQ': np.square(pt_vec)}
    if key in ds_pt_dict:
        ds_pt_dict[key].append(point_index)
    else:
        ds_pt_dict[key] = [point_index]
    return final_ds_dict, ds_pt_dict

def process_point(point_index, pt_vec, final_ds_dict, ds_pt_dict, d):
    tempdict = {cluster_index: get_stat_dist(pt_vec, cluster_info) for cluster_index, cluster_info in final_ds_dict.items()}
    index = min(tempdict, key=tempdict.get)
    distance = tempdict[index]
    if distance < 4 * np.sqrt(d):
        final_ds_dict, ds_pt_dict = update_dicts(final_ds_dict, ds_pt_dict, index, point_index, pt_vec)
        return final_ds_dict, ds_pt_dict, True
    return final_ds_dict, ds_pt_dict, False

def assign_points(indices, final_ds_dict, ds_pt_dict, final_rs_dict, d):
    for point_index, pt_vec in indices:
        final_ds_dict, ds_pt_dict, assigned = process_point(point_index, pt_vec, final_ds_dict, ds_pt_dict, d)
        if not assigned:
            final_rs_dict[point_index] = pt_vec
    return final_ds_dict, ds_pt_dict, final_rs_dict

def update_final_ds_dict(final_ds_dict, final_rs_dict, d):
    comp_rs_dict = {}
    for point_index, pt_vec in final_rs_dict.items():
        assigned = 0
        tempdict = {}
        for cluster_index, cluster_info in final_ds_dict.items():
            distance = get_stat_dist(pt_vec, cluster_info)
            tempdict[cluster_index] = distance
        index = min(tempdict, key=tempdict.get)
        distance = tempdict[index]
        if distance < 6 * np.sqrt(d):
            ds_pt_dict[index].append(point_index)
            final_ds_dict[index]['N'] += 1
            final_ds_dict[index]['SUM'] += pt_vec
            final_ds_dict[index]['SUMSQ'] += np.square(pt_vec)
            assigned = 1
        if assigned == 0:
            comp_rs_dict[point_index] = pt_vec
    return final_ds_dict, comp_rs_dict

def write_to_csv(run_no, final_ds_dict, cs_dict, final_rs_dict, out_csv, d):
    n_centroids = len(final_ds_dict)
    n_ds_set = sum(cluster_info['N'] for cluster_info in final_ds_dict.values())
    n_cs_set = len(cs_dict)
    n_cs_centroinds = sum(cluster_info['N'] for cluster_info in cs_dict.values())
    n_rs_set = len(final_rs_dict)
    out_csv.write(f'{run_no},{n_centroids},{n_ds_set},{n_cs_set},{n_cs_centroinds},{n_rs_set}\n')

def update_cluster_dicts(cluster_dict, data_index,pt_vec, d):
    if cluster_dict.get('N'):
        cluster_dict['N'] += 1
        cluster_dict['SUM'] = np.add(cluster_dict['SUM'], pt_vec)
        cluster_dict['SUMSQ'] = np.add(cluster_dict['SUMSQ'], np.square(pt_vec))
    else:
        cluster_dict = {'N': 1, 'SUM': pt_vec, 'SUMSQ': np.square(pt_vec)}
    return cluster_dict

def process_cent_dict(cent_dict, cluster_index, final_ds_dict, ds_pt_dict, final_rs_dict, d):
    for point_index in cent_dict[cluster_index]:
        pt_vec = indices[point_index]
        tempdict = {cluster_index: get_stat_dist(pt_vec, cluster_info) for cluster_index, cluster_info in final_ds_dict.items()}
        index = min(tempdict, key=tempdict.get)
        distance = tempdict[index]
        if distance < 4 * np.sqrt(d):
            ds_pt_dict[index].append(point_index)
            final_ds_dict[index] = update_cluster_dicts(final_ds_dict.get(index, {}), point_index, pt_vec, d)
        else:
            final_rs_dict[point_index] = pt_vec
    return final_ds_dict, ds_pt_dict, final_rs_dict


out_csv = open(out_file2, 'w')
run_no = 1
for path in path_array:
    print(run_no)
    fd = open(path, 'r')
    rows = fd.readlines()
    rows = process_rows(rows)
    d = len(rows[0])-1
    if run_no == 1:
        indices, unass_pts, ass_pts = initialize_dicts(rows)
        cent_dict = run_kmeans(unass_pts, args.n_cluster,d)
        final_dict.update({data_index: cid for cid, pids in cent_dict.items() for data_index in pids})
        length_dict = {cid: len(pids) for cid, pids in cent_dict.items()}
        ds_temp = dict(Counter(length_dict).most_common(n_cluster))
        ds_ass = list(ds_temp.keys())
        for cid in ds_ass:
            for data_index in cent_dict[cid]:
                final_ds_dict[cid] = update_cluster_dicts(final_ds_dict.setdefault(cid, {}), data_index, indices[data_index],d)
                ds_pt_dict.setdefault(cid, []).append(data_index)
        ds_stat_dict = {}
        ds_ass_dict = {}
        for cid, data_indices in ds_pt_dict.items():
            for data_index in data_indices:
                distance = get_stat_dist(indices[data_index], final_ds_dict[cid])
                if distance < 4 * np.sqrt(d):
                    ds_stat_dict[cid] = update_cluster_dicts(ds_stat_dict.setdefault(cid, {}), data_index, indices[data_index],d)
                    ds_ass_dict.setdefault(cid, []).append(data_index)
                else:
                    final_rs_dict[data_index] = indices[data_index]
        ds_pt_dict = ds_ass_dict.copy()
        final_ds_dict = ds_stat_dict.copy()
        for cid, pids in cent_dict.items():
            if cid not in ds_ass and len(pids) > 0:
                for data_index in pids:
                    pt_vec = indices[data_index]
                    assigned = 0
                    tempdict = {cluster_index: get_stat_dist(pt_vec, cluster_info) for cluster_index, cluster_info in final_ds_dict.items()}
                    index = min(tempdict, key=tempdict.get)
                    distance = tempdict[index]
                    if distance < 4 * np.sqrt(d):
                        ds_pt_dict[index].append(data_index)
                        final_ds_dict[index] = update_cluster_dicts(final_ds_dict.setdefault(index, {}), data_index, pt_vec)
                        assigned = 1
                    if assigned == 0:
                        final_rs_dict[data_index] = pt_vec
                    else:
                        final_ds_dict, ds_pt_dict, final_rs_dict = process_cent_dict(cent_dict, cid, final_ds_dict, ds_pt_dict, final_rs_dict)
        final_ds_dict, ds_pt_dict, final_rs_dict = assign_points(ass_pts, final_ds_dict, ds_pt_dict, final_rs_dict, d)
        csv_columns = ['round_id','nof_cluster_discard','nof_point_discard','nof_cluster_compression','nof_point_compression','nof_point_retained']
        out_csv.write(','.join(csv_columns) + '\n')
    else:
        indices = {d[0]: d[1:] for d in rows}
        final_ds_dict, ds_pt_dict, final_rs_dict = assign_points(indices.items(), final_ds_dict, ds_pt_dict, final_rs_dict, d)
    if run_no == len(path_array):
        final_ds_dict, final_rs_dict = update_final_ds_dict(final_ds_dict, final_rs_dict, d)
    write_to_csv(run_no, final_ds_dict, cs_dict, final_rs_dict, out_csv, d)
    run_no += 1

cluster_index = 0
for cluster_id, points in ds_pt_dict.items():
    for point in points:
        final_dict[point] = cluster_index
    cluster_index += 1
rs = list(cs_pt_dict.values())
rs.append(list(final_rs_dict.keys()))
rs = sum(rs, [])
for point in rs:
    final_dict[point] = -1
with open(out_file1, 'w') as fd:
    json.dump(final_dict, fd)
    
    
