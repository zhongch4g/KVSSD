import numpy as np
import pandas as pd

def parse_log(path):
    data = []
    with open (path, "r") as f:
        for single_line in f.readlines():
            if (single_line.startswith("[Epoch]")):
                single_line = single_line.strip("[Epoch]").strip()
                data.append(single_line.split(","))
    
    # generate data to a dataframe
    data = pd.DataFrame(data, columns=['thread', 'last_epoch_done', 'total_done', 'epoch_time', 'time'], dtype=np.dtype('float'))

    time_epoch = 1
    max_n_rows = 0
    for i in range(20):
        th = data[data["thread"]==i].shape
        if (th[0] > max_n_rows):
            max_n_rows = th[0]    

    last_epoch_done_padding = np.zeros(max_n_rows)
    
    for i in range(20):
        th = data[data["thread"]==i]
        last_epoch_done = th['last_epoch_done'].values
        padding = np.pad(last_epoch_done, (0, max_n_rows-th.shape[0]), 'constant', constant_values=(0,0))
        last_epoch_done_padding += padding
    
    time = np.arange(1, len(last_epoch_done_padding) + 1)
    last_epoch_done_padding = np.array(last_epoch_done_padding)
    cumulative_last_epoch_done = np.array(last_epoch_done_padding.cumsum())
    formatted_data = pd.DataFrame({'time': time, 'last_epoch_done': last_epoch_done_padding, 'total_done': cumulative_last_epoch_done})
    
    return formatted_data
    
dataframe1 = parse_log("nvmevirt_kvssd_spdk_write_k8_v8.data")
dataframe2 = parse_log("nvmevirt_kvssd_spdk_write_k8_v32.data")
dataframe3 = parse_log("nvmevirt_kvssd_spdk_write_k8_v128.data")
dataframe4 = parse_log("nvmevirt_kvssd_spdk_write_k8_v512.data")
dataframe5 = parse_log("nvmevirt_kvssd_spdk_write_k8_v1024.data")
dataframe6 = parse_log("nvmevirt_kvssd_spdk_write_k8_v2048.data")
dataframe7 = parse_log("nvmevirt_kvssd_spdk_write_k8_v4096.data")

dataframe1 = parse_log("nvmevirt_kvssd_spdk_write_k8_v8_ufull.data")
dataframe2 = parse_log("nvmevirt_kvssd_spdk_write_k8_v32_ufull.data")
dataframe3 = parse_log("nvmevirt_kvssd_spdk_write_k8_v128_ufull.data")
dataframe4 = parse_log("nvmevirt_kvssd_spdk_write_k8_v512_ufull.data")
dataframe5 = parse_log("nvmevirt_kvssd_spdk_write_k8_v1024_ufull.data")
dataframe6 = parse_log("nvmevirt_kvssd_spdk_write_k8_v2048_ufull.data")
dataframe7 = parse_log("nvmevirt_kvssd_spdk_write_k8_v4096_ufull.data")
dataframe8 = parse_log("nvmevirt_kvssd_spdk_write_k8_v8192_ufull.data")
dataframe9 = parse_log("nvmevirt_kvssd_spdk_write_k8_v16384_ufull.data")
dataframe10 = parse_log("nvmevirt_kvssd_spdk_write_k8_v32768_ufull.data")


import matplotlib.pyplot as plt
import sys
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
# plt.rcParams["font.family"] = "arial"
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

gloyend = None
# Main
dashes=[(2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0), (2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0)]
markers = ['x', '|', '.', 'D', 'd', '', 'x', '|', '.', 'D', 'd', '']
colors = ['#001f3f', '#0074D9', '#01FF70', '#B10DC9', '#85144b', '#3D9970', '#111111', '#7FDBFF', '#FFDC00', '#FF851B']

# label = ['ARTLSM-ycsb-random', 'ARTLSM-ycsb-sequential', 'ARTLSM-ycsb-blockrandom']
label = ['8B', '32B', '128B', '512B', '1KB', '2KB', '4KB', '8KB', '16KB', '32KB']
label = ['v'+v for v in label]

# d0 d1 represents dynamic CCEH data
# f0 f1 represents fixed CCEH data

fig, ax = plt.subplots(figsize=(8, 3.6), constrained_layout=True, sharex=True, sharey=True)

sample_interval1 = 10
sample_interval2 = 20
dataframe1 = dataframe1[::1]
dataframe2 = dataframe2[::1]
dataframe3 = dataframe3[::1]
dataframe4 = dataframe4[::1]
dataframe5 = dataframe5[::1]
dataframe6 = dataframe6[::1]
dataframe7 = dataframe7[::1]
dataframe8 = dataframe8[::1]
dataframe9 = dataframe9[::1]
dataframe10 = dataframe10[::1]

x_unit = 1024*1024*1024 # GB
y_unit = 1024*1024 # MB

ax.plot(dataframe10['total_done']*32776/x_unit, dataframe10['last_epoch_done']*32776/y_unit, color=colors[9], marker=markers[2], dashes=dashes[2], label = label[9], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe9['total_done']*16392/x_unit, dataframe9['last_epoch_done']*16392/y_unit, color=colors[8], marker=markers[2], dashes=dashes[2], label = label[8], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe8['total_done']*8200/x_unit, dataframe8['last_epoch_done']*8200/y_unit, color=colors[7], marker=markers[2], dashes=dashes[2], label = label[7], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe7['total_done']*4104/x_unit, dataframe7['last_epoch_done']*4104/y_unit, color=colors[6], marker=markers[2], dashes=dashes[2], label = label[6], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe6['total_done']*2056/x_unit, dataframe6['last_epoch_done']*2056/y_unit, color=colors[5], marker=markers[2], dashes=dashes[2], label = label[5], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe5['total_done']*1032/x_unit, dataframe5['last_epoch_done']*1032/y_unit, color=colors[4], marker=markers[2], dashes=dashes[2], label = label[4], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe4['total_done']*520/x_unit, dataframe4['last_epoch_done']*520/y_unit, color=colors[3], marker=markers[2], dashes=dashes[2], label = label[3], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe3['total_done']*136/x_unit, dataframe3['last_epoch_done']*136/y_unit, color=colors[2], marker=markers[2], dashes=dashes[2], label = label[2], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe2['total_done']*40/x_unit, dataframe2['last_epoch_done']*40/y_unit, color=colors[1], marker=markers[2], dashes=dashes[2], label = label[1], alpha=0.8, fillstyle='none', markersize=8)
ax.plot(dataframe1['total_done']*16/x_unit, dataframe1['last_epoch_done']*16/y_unit, color=colors[0], marker=markers[2], dashes=dashes[2], label = label[0], alpha=0.8, fillstyle='none', markersize=8)








handles, labels = ax.get_legend_handles_labels()
order = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order], loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=5, frameon=False)

ax.grid(which='major', linestyle='--', zorder=0)
ax.grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax.xaxis.grid(False, which='both')

# ax.set_xlabel('# of Keys (Million)', fontsize=12)
ax.set_xlabel('# of KVs * KV Size (GB)', fontsize=12)
ax.set_ylabel('Throughput (MB/s)', fontsize=12)
# ax.set_ylim(0, 150)

fig.savefig("./nvmevirt_kvssd_vary_value_size_ufull_xGB_yMB.png", bbox_inches='tight', pad_inches=0.1)

