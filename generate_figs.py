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
    data["Read/Write Ratio"] = np.nan
    for index, row in data.iterrows():
        seg1 = data[data["Operation"] == "WRITE"]
        seg2 = seg1[seg1["Base Factor"] == row["Base Factor"]]
        seg3 = seg2[np.isclose(row["Ratio"], seg2["Ratio"])]
        data.loc[index, "Read/Write Ratio"] = row["Time"] / seg3["Time"].iloc[0]

    # Read Throughput to Write Throughput Ratio
    for base in data["Base Factor"].unique():
        curr_data = data[data["Base Factor"] == base]
        curr_data = curr_data[curr_data["Operation"] == "READ"]
        plt.plot(curr_data["Ratio"], curr_data["Read/Write Ratio"], marker='o')
    plt.legend([str(i) + " Base Leveling Factor" for i in data["Base Factor"].unique()])
    plt.xlabel("Leveling Ratio Difference")
    plt.ylabel("Read Throughput / Write Throughput")
    plt.title("Read Throughput to Write Throughput Ratio")
    plt.savefig(result_path + "fig3_read_write_ratio.jpg")
    plt.clf()

    # Scan Throughput to Write Throughput Ratio
    for base in data["Base Factor"].unique():
        curr_data = data[data["Base Factor"] == base]
        curr_data = curr_data[curr_data["Operation"] == "SCAN"]
        plt.plot(curr_data["Ratio"], curr_data["Read/Write Ratio"], marker='o')
    plt.legend([str(i) + " Base Leveling Factor" for i in data["Base Factor"].unique()])
    plt.xlabel("Leveling Ratio Difference")
    plt.ylabel("Scan Throughput / Write Throughput")
    plt.title("Scan Throughput to Write Throughput Ratio")
    plt.savefig(result_path + "fig3_scan_write_ratio.jpg")
    plt.clf()

    # Read Throughput
    for base in data["Base Factor"].unique():
        curr_data = data[data["Base Factor"] == base]
        curr_data = curr_data[curr_data["Operation"] == "READ"]
        plt.plot(curr_data["Ratio"], curr_data["Time"], marker='o')
    plt.legend([str(i) + " Base Leveling Factor" for i in data["Base Factor"].unique()])
    plt.xlabel("Leveling Ratio Difference")
    plt.ylabel("Read Throughput (Op/Sec)")
    plt.title("Read Throughput")
    plt.savefig(result_path + "fig3_read_throughput.jpg")
    plt.clf()

    # Scan Throughput
    for base in data["Base Factor"].unique():
        curr_data = data[data["Base Factor"] == base]
        curr_data = curr_data[curr_data["Operation"] == "SCAN"]
        plt.plot(curr_data["Ratio"], curr_data["Time"], marker='o')
    plt.legend([str(i) + " Base Leveling Factor" for i in data["Base Factor"].unique()])
    plt.xlabel("Leveling Ratio Difference")
    plt.ylabel("Scan Throughput (Op/Sec)")
    plt.title("Scan Throughput")
    plt.savefig(result_path + "fig3_scan_throughput.jpg")
    plt.clf()

    # Write Throughput
    for base in data["Base Factor"].unique():
        curr_data = data[data["Base Factor"] == base]
        curr_data = curr_data[curr_data["Operation"] == "WRITE"]
        plt.plot(curr_data["Ratio"], curr_data["Time"], marker='o')
    plt.legend([str(i) + " Base Leveling Factor" for i in data["Base Factor"].unique()])
    plt.xlabel("Leveling Ratio Difference")
    plt.ylabel("Write Throughput (Op/Sec)")
    plt.title("Write Throughput")
    plt.savefig(result_path + "fig3_write_throughput.jpg")
    plt.clf()



    
    sys.exit()


# Fig 1 and 2


reads = data[data["Operation"] == "READ"]
reads_base = reads[reads["Ratio"] == 1]
reads_test = reads[reads["Ratio"] < 1]

plt.plot(reads_test.iloc[:, 1], reads_test.iloc[:, -1], marker= 'o')
plt.plot(reads_base.iloc[:, 1], reads_base.iloc[:, -1], marker='o')


plt.title("Point Read Throughput")
plt.xlabel(colname)
plt.ylabel("Read Throughput (Op/Sec)")
plt.legend(["2/3 Ratio", "1 Ratio (baseline)"])




plt.savefig(result_path + "read_" + colname + ".jpg")

plt.clf()

scans = data[data["Operation"] == "SCAN"]
scans_base = scans[scans["Ratio"] == 1]
scans_test = scans[scans["Ratio"] < 1]

plt.plot(scans_test.iloc[:, 1], scans_test.iloc[:, -1], marker= 'o')
plt.plot(scans_base.iloc[:, 1], scans_base.iloc[:, -1], marker='o')


plt.title("Scan Throughput")
plt.xlabel(colname)
plt.ylabel("Scan Throughput (Op/Sec)")
plt.legend(["2/3 Ratio", "1 Ratio (baseline)"])

plt.savefig(result_path + "scan_" + colname + ".jpg")

