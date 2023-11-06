import matplotlib.pyplot as plt
import pandas as pd
import sys
import numpy as np

if len(sys.argv) < 3:
    print("Usage: python3 generate_figs.py path/data.csv path/output_dir")
    sys.exit()

data = pd.read_csv(sys.argv[1])
colname = data.columns[1]
result_path = sys.argv[2] if sys.argv[2][-1] == "/" else sys.argv[2] + "/" # add slash if necessary

if "fig_3" in sys.argv[1]:
    # Fig 3

    # preprocess by adding read/write ratio column
    baseline = data[data["Ratio"] == 1]
    for factor in baseline["Base Factor"].unique():
        curr_factor_tbl = baseline[baseline["Base Factor"] == factor]
        write = curr_factor_tbl[curr_factor_tbl["Operation"] == "WRITE"]
        scan = curr_factor_tbl[curr_factor_tbl["Operation"] == "SCAN"]
        plt.scatter(1/write["Time"].iloc[0], 1/scan["Time"].iloc[0], marker="x", color="black", label="1 Ratio (Baseline)")

    test = data[data["Ratio"] != 1]
    for factor in test["Base Factor"].unique():
        curr_factor_tbl = test[test["Base Factor"] == factor]
        write = curr_factor_tbl[curr_factor_tbl["Operation"] == "WRITE"]
        scan = curr_factor_tbl[curr_factor_tbl["Operation"] == "SCAN"]
        plt.scatter(1/write["Time"].iloc[0], 1/scan["Time"].iloc[0], marker="o", color="red", label="0.8 Ratio")


    # https://stackoverflow.com/questions/13588920/stop-matplotlib-repeating-labels-in-legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.title("Scan Cost vs Write Cost")
    plt.xlabel("Write Cost (s)")
    plt.ylabel("Scan Cost (s)")
    
    plt.savefig(result_path + "scan_vs_write.jpg")
    sys.exit()

# Fig 1 and 2


reads = data[data["Operation"] == "READ"]
reads_base = reads[reads["Ratio"] == 1]
reads_test = reads[reads["Ratio"] < 1]

plt.plot(reads_test.iloc[:, 1], 1/reads_test.iloc[:, -1], marker= 'o')
plt.plot(reads_base.iloc[:, 1], 1/reads_base.iloc[:, -1], marker='o')


plt.title("Point Read Latency vs Database Size")
plt.xlabel(colname)
plt.ylabel("Average Read Latency (s)")
plt.legend(["0.8 Ratio", "1 Ratio (baseline)"])




plt.savefig(result_path + "read_" + colname + ".jpg")

plt.clf()

scans = data[data["Operation"] == "SCAN"]
scans_base = scans[scans["Ratio"] == 1]
scans_test = scans[scans["Ratio"] < 1]

plt.plot(scans_test.iloc[:, 1], 1/scans_test.iloc[:, -1], marker= 'o')
plt.plot(scans_base.iloc[:, 1], 1/scans_base.iloc[:, -1], marker='o')


plt.title("Scan Latency vs Database Size")
plt.xlabel(colname)
plt.ylabel("Scan Average Latency (s)")
plt.legend(["0.8 Ratio", "1 Ratio (baseline)"])

plt.savefig(result_path + "scan_" + colname + ".jpg")

