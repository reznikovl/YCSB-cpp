import sys
import time
import os
import subprocess
import csv

if len(sys.argv) < 2:
    print("Please specify 1 if dbs need to be seeded else 0")
    sys.exit()

# open log file
timestr = time.strftime("%Y%m%d-%H%M%S")
f = open("../logs/fig_3_" + timestr + ".txt", "w+")

# add list to track results
result_fields = ["Operation", "Base Factor", "Ratio", "Time"]
results = []

# key sizes to write
base_factors = [2, 4, 8]
ratios = [1, 0.66666667, 0.55555555]
ratio_filename_strs = ["1", "2_3", "5_9"] # for easy file naming

# base_factors = [4]
# ratios = [0.55555555]
# ratio_filename_strs = ["5_9"]

if (sys.argv[1] == "1"):
    # do writes
    
    for base_factor in base_factors:
        base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
        for ratio, ratio_filename_str in zip(ratios, ratio_filename_strs):
            print("Seeding db with " + str(base_factor) + " base and " + ratio_filename_str + " ratio...")

            curr_command = base_write_args.copy()
            curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]
            curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" + str(base_factor) + "_" + str(ratio)]
            
            op_count = 2048 * 1024 * 8
            curr_command += ["-p", "operationcount=" + str(op_count)]

            r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
            f.write(str(curr_command))

            parse_location = r.stdout.find("Run throughput")
            num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
            results.append(("WRITE", base_factor, ratio, num))

            f.write("-----WRITE " + str(base_factor) + " " + str(ratio) + " -----\n")
            f.write(r.stderr)
            f.write(r.stdout)
            
            f.write("-----WRITE FINISHED-----\n")

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for base_factor in base_factors:
        for ratio, ratio_filename_str in zip(ratios, ratio_filename_strs):
            print("Forcing filters for " + str(base_factor) + " base and " + str(ratio) + " ratio...")
            r = subprocess.run(["./seed", "0", "/tmp/fig3_" + str(base_factor) + "_" + str(ratio), "1"], capture_output=True, encoding="utf-8")
            f.write("-----FILTER " + str(base_factor) + " base and " + ratio_filename_str + " ratio-----\n")
            f.write(r.stderr)
            f.write(r.stdout)
            f.write("-----FILTER FINISHED-----\n")
    os.chdir("../../YCSB-cpp")

# Perform Point Reads

for base_factor in base_factors:
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/read_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
    for ratio, ratio_filename_str in zip(ratios, ratio_filename_strs):
        print("Reading from db with " + str(base_factor) + " base and " + str(ratio) + " ratio...")
        curr_command = base_write_args.copy()
        curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" + str(base_factor) + "_" + str(ratio)]
        curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
        results.append(("READ", base_factor, ratio, num))

        f.write("-----READ  " + str(base_factor) + " base and " + str(ratio) + " ratio...-----\n")
        f.write(r.stderr)
        f.write(r.stdout)

        f.write("-----READ FINISHED-----\n")
print(results)


# Perform Range Reads
for base_factor in base_factors:
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/scan_uniform",
                       "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
    for ratio, ratio_filename_str in zip(ratios, ratio_filename_strs):
        print("Scanning from db with " + str(base_factor) +
              " base and " + str(ratio) + " ratio...")
        curr_command = base_write_args.copy()
        curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" +
                         str(base_factor) + "_" + str(ratio)]
        curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]
                     ).strip())  # magic number :(
        results.append(("SCAN", base_factor, ratio, num))

        f.write("-----SCAN  " + str(base_factor) +
                " base and " + str(ratio) + " ratio...-----\n")
        f.write(r.stderr)
        f.write(r.stdout)

        f.write("-----SCAN FINISHED-----\n")

print(results)

f.close()
with open("../logs/fig_3_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    