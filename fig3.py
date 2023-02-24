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

base_factors = [2, 4, 8]
test_factors = [2, 3, 4]
test_ratios = [0.8]
test_ratio_strs = ["0_8"] # avoid . in file names

if (sys.argv[1] == "1"): # need to seed
    # write leveling DBs
    for base_factor in base_factors:
        curr_command = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
        print("Seeding leveling db with factor " + str(base_factor) + "...")

        curr_command += ["-p", "leveldb.dbname=/tmp/fig3_leveling_" + str(base_factor)]
        
        op_count = 2048 * 1024 * 8
        curr_command += ["-p", "operationcount=" + str(op_count)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", base_factor, 1, num))

        f.write("-----WRITE LEVELING" + str(base_factor) + "-----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("-----WRITE FINISHED-----\n")

    # write test DBs
    for test_factor in test_factors:
        base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-p", "leveldb.base_scaling_factor=" + str(test_factor), "-s"]
        for ratio, ratio_filename_str in zip(test_ratios, test_ratio_strs):
            print("Seeding db with " + str(test_factor) + " base and " + str(ratio) + " ratio...")

            curr_command = base_write_args.copy()
            curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]
            curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" + str(test_factor) + "_" + ratio_filename_str]
            
            op_count = 2048 * 1024 * 8
            curr_command += ["-p", "operationcount=" + str(op_count)]

            r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
            f.write(str(curr_command))

            parse_location = r.stdout.find("Run throughput")
            num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
            results.append(("WRITE", test_factor, ratio, num))

            f.write("-----WRITE " + str(base_factor) + " " + str(ratio) + " -----\n")
            f.write(r.stderr)
            f.write(r.stdout)
            
            f.write("-----WRITE FINISHED-----\n")

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for base_factor in base_factors:
        print("Forcing filters for leveling with " + str(base_factor) + " base...")
        r = subprocess.run(["./seed", "0", "/tmp/fig3_leveling_" + str(base_factor), "1"], capture_output=True, encoding="utf-8")
        f.write("-----FILTER " + str(base_factor) + " base and " + ratio_filename_str + " ratio-----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----FILTER FINISHED-----\n")

    for test_factor in test_factors:
        for ratio, ratio_filename_str in zip(test_ratios, test_ratio_strs):
            print("Forcing filters for db with " + str(test_factor) + " base and " + str(test_factor) + "ratio...")
            r = subprocess.run(["./seed", "0", "/tmp/fig3_" + str(test_factor) + "_" + ratio_filename_str, "1"], capture_output=True, encoding="utf-8")
            f.write("-----FILTER " + str(test_factor) + " base and " + ratio_filename_str + " ratio-----\n")
            f.write(r.stderr)
            f.write(r.stdout)
            f.write("-----FILTER FINISHED-----\n")


    
    os.chdir("../../YCSB-cpp")

# Perform Point Reads

for base_factor in base_factors:
    base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/read_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
    
    print("Reading from leveling db with factor " + str(base_factor) + "...")
    curr_command = base_read_args.copy()
    curr_command += ["-p", "leveldb.dbname=/tmp/fig3_leveling_" + str(base_factor)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                ).strip())  # magic number :(
    results.append(("READ", base_factor, 1, num))

    f.write("-----READ leveling " + str(base_factor) + " base...-----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----READ FINISHED-----\n")

for test_factor in test_factors:
        base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/read_uniform", "-p", "leveldb.base_scaling_factor=" + str(test_factor), "-s"]
        for ratio, ratio_filename_str in zip(test_ratios, test_ratio_strs):
            print("Reading from db with " + str(test_factor) + " base and " + str(ratio) + " ratio...")
            curr_command = base_read_args.copy()
            curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]
            curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" + str(test_factor) + "_" + ratio_filename_str]

            r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
            f.write(str(curr_command))

            parse_location = r.stdout.find("Run throughput")
            num = float((r.stdout[parse_location + 24:]
                        ).strip())  # magic number :(
            results.append(("READ", test_factor, ratio, num))

            f.write("-----READ leveling " + str(base_factor) + " base...-----\n")
            f.write(r.stderr)
            f.write(r.stdout)

            f.write("-----READ FINISHED-----\n")

print(results)


# Perform Range Reads
for base_factor in base_factors:
    base_scan_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/scan_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s"]
    
    print("Scanning from leveling db with factor " + str(base_factor) + "...")
    curr_command = base_read_args.copy()
    curr_command += ["-p", "leveldb.dbname=/tmp/fig3_leveling_" + str(base_factor)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                ).strip())  # magic number :(
    results.append(("SCAN", base_factor, 1, num))

    f.write("-----SCAN leveling " + str(base_factor) + " base...-----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----SCAN FINISHED-----\n")

for test_factor in test_factors:
        base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/scan_uniform", "-p", "leveldb.base_scaling_factor=" + str(test_factor), "-s"]
        for ratio, ratio_filename_str in zip(test_ratios, test_ratio_strs):
            print("Scanning from db with " + str(test_factor) + " base and " + str(ratio) + " ratio...")
            curr_command = base_read_args.copy()
            curr_command += ["-p", "leveldb.ratio_diff=" + str(ratio)]
            curr_command += ["-p", "leveldb.dbname=/tmp/fig3_" + str(test_factor) + "_" + ratio_filename_str]

            r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
            f.write(str(curr_command))

            parse_location = r.stdout.find("Run throughput")
            num = float((r.stdout[parse_location + 24:]
                        ).strip())  # magic number :(
            results.append(("SCAN", test_factor, ratio, num))

            f.write("-----SCAN leveling " + str(test_factor) + " base...-----\n")
            f.write(r.stderr)
            f.write(r.stdout)

            f.write("-----SCAN FINISHED-----\n")

print(results)

f.close()
with open("../logs/fig_3_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    