import sys
import time
import os
import subprocess
import csv


# how big each DB is
num_mb = 100

# path to DB
db_path = "/tmp/"


T_values = [2,5] # leveling base ratios
K_values = [2,4] # autumn base ratios
C_value = 0.8

key_size_bytes=16
value_size_bytes=100


if len(sys.argv) < 2:
    print("Please specify 1 if dbs need to be seeded else 0")
    sys.exit()

# open log file
timestr = time.strftime("%Y%m%d-%H%M%S")
f = open("../logs/fig_3_" + timestr + ".txt", "w+")

# add list to track results
result_fields = ["Operation", "Base Factor", "Ratio", "Time"]
results = []


if (sys.argv[1] == "1"): # need to seed
    # write leveling DBs
    for base_factor in T_values:
        curr_command = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s", "-p", "recordcount=0", "-p", f"keysize={key_size_bytes}", "-p", f"fieldlength={value_size_bytes}", "-p", "leveldb.base_scaling_fator=" + str(base_factor)] # for now, we are scanning for no duplicates
        print(f"Seeding leveling db with factor {base_factor}...")

        curr_command += ["-p", "leveldb.dbname=" + db_path + "fig3_leveling_" + str(base_factor)]
        
        op_count = num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)
        curr_command += ["-p", "operationcount=" + str(op_count)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----WRITE LEVELING" + str(base_factor) + "-----\n")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", base_factor, 1, num))

        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("\n-----WRITE FINISHED-----\n")

    # write test DBs
    for test_factor in K_values:
        curr_command = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s", "-p", "recordcount=0", "-p", f"keysize={key_size_bytes}", "-p", f"fieldlength={value_size_bytes}", "-p", "leveldb.base_scaling_fator=" + str(test_factor), "-p", "leveldb.ratio_diff=" + str(C_value)] # for now, we are scanning for no duplicates
        print("Seeding db with " + str(test_factor) + " base and " + str(C_value) + " ratio...")

        curr_command = curr_command.copy()
        curr_command += ["-p", "leveldb.ratio_diff=" + str(C_value)]
        curr_command += ["-p", "leveldb.dbname=" + db_path +"fig3_" + str(test_factor) + "_" + str(C_value)]
        
        op_count = num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)
        curr_command += ["-p", "operationcount=" + str(op_count)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----WRITE " + str(test_factor) + " " + str(C_value) + " -----\n")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", test_factor, C_value, num))

        
        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("\n-----WRITE FINISHED-----\n")

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for base_factor in T_values:
        print("Forcing filters for leveling with " + str(base_factor) + " base...")
        r = subprocess.run(["./seed", "0", db_path + "fig3_leveling_" + str(base_factor), "1"], capture_output=True, encoding="utf-8")
        f.write("\n-----FILTER leveling " + str(base_factor) + " base -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("\n-----FILTER FINISHED-----\n")

    for test_factor in K_values:
        print("Forcing filters for db with " + str(test_factor) + " base and " + str(C_value) + "ratio...")
        r = subprocess.run(["./seed", "0", db_path + "fig3_" + str(test_factor) + "_" + str(C_value), "1"], capture_output=True, encoding="utf-8")
        f.write("-----FILTER " + str(test_factor) + " base and " + str(C_value) + " ratio-----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----FILTER FINISHED-----\n")


    
    os.chdir("../../YCSB-cpp")

# Perform Point Reads

for base_factor in T_values:
    base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/read_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s", "-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}", "-p", 'leveldb.filter_bits=5,5,5,5,5,5,5']
    
    print("Reading from leveling db with factor " + str(base_factor) + "...")
    curr_command = base_read_args.copy()
    curr_command += ["-p", "leveldb.dbname=" + db_path + "fig3_leveling_" + str(base_factor)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----READ leveling " + str(base_factor) + " base...-----\n")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                ).strip())  # magic number :(
    results.append(("READ", base_factor, 1, num))

    
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("\n-----READ FINISHED-----\n")

for test_factor in K_values:
        base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/read_uniform", "-p", "leveldb.base_scaling_factor=" + str(test_factor), "-s", "-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}", "-p", 'leveldb.filter_bits=5,5,5,5,5,5,5']
        print("Reading from db with " + str(test_factor) + " base and " + str(C_value) + " ratio...")
        curr_command = base_read_args.copy()
        curr_command += ["-p", "leveldb.ratio_diff=" + str(C_value)]
        curr_command += ["-p", "leveldb.dbname=" + db_path + "fig3_" + str(test_factor) + "_" + str(C_value)]

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----READ  " + str(base_factor) + " base, " + str(C_value) + " ratio...-----\n")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]
                    ).strip())  # magic number :(
        results.append(("READ", test_factor, str(C_value), num))

        f.write("\n-----READ  " + str(base_factor) + " base, " + str(C_value) + " ratio...-----\n")
        f.write(r.stderr)
        f.write(r.stdout)

        f.write("-----READ FINISHED-----\n")

print(results)


# Perform Range Reads
for base_factor in T_values:
    base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/scan_uniform", "-p", "leveldb.base_scaling_factor=" + str(base_factor), "-s", "-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    
    print("Scanning from leveling db with factor " + str(base_factor) + "...")
    curr_command = base_read_args.copy()
    curr_command += ["-p", "leveldb.dbname=" + db_path + "fig3_leveling_" + str(base_factor)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----SCAN leveling " + str(base_factor) + " base...-----\n")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                ).strip())  # magic number :(
    results.append(("SCAN", base_factor, 1, num))

    
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("\n-----SCAN FINISHED-----\n")

for test_factor in K_values:
    base_read_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/scan_uniform", "-p", "leveldb.base_scaling_factor=" + str(test_factor), "-s", "-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    
    print("Scanning from db with " + str(test_factor) + " base and " + str(C_value) + " ratio...")
    curr_command = base_read_args.copy()
    curr_command += ["-p", "leveldb.dbname=" + db_path + "fig3_" + str(test_factor) + "_" + str(C_value)]
    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----SCAN  " + str(test_factor) + " base, " + str(C_value) + " ratio...-----\n")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                ).strip())  # magic number :(
    results.append(("SCAN", test_factor, C_value, num))

    
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("\n-----SCAN FINISHED-----\n")

print(results)

f.close()
with open("../logs/fig_3_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    