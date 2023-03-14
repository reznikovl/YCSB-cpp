import sys
import time
import os
import subprocess
import csv

# mb to write
#mb_to_write = [1024*1, 1024*2, 1024*4, 1024*6, 1024*8, 1024*10, 1024 * 12, 1024 * 14]
mb_to_write = [1024*1, 1024*2, 1024*4]
db_path = "/home/ec2-user/research/mountpt/"

# T (ratio for leveling)
T = 4

# k (ratio for Autumn)
k = 3

# autumn parameter to use
c = 0.8

key_size_bytes = 16
value_size_bytes = 128

if len(sys.argv) < 2:
    print("Please specify 1 if dbs need to be seeded else 0")
    sys.exit()

# open log file
timestr = time.strftime("%Y%m%d-%H%M%S")
f = open("../logs/fig_1_" + timestr + ".txt", "w+")
f.write("Parameters: ")
f.write(f"T={T}\n")
f.write(f"k={k}\n")
f.write(f"c={c}\n")
f.write(f"key size (bytes) = {key_size_bytes}\n")
f.write(f"value size (bytes) = {value_size_bytes}\n")

# add list to track results
result_fields = ["Operation", "Size (MB)", "Ratio", "Time"]
results = []



if (sys.argv[1] == "1"):
    # do writes
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s", "-p", "recordcount=0", "-p", f"fieldlength={value_size_bytes}", "-p", f"keysize={key_size_bytes}"] # for now, we are scanning for no duplicates
    for num_mb in mb_to_write:
        print(f"Seeding db with {num_mb} mb base...")
        curr_command = base_write_args.copy()

        # set scaling factor to T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
        

        op_count = num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, 1, num))

        f.write("\n-----BASE WRITE " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("\n-----BASE WRITE FINISHED-----\n")

        curr_command = base_write_args.copy()
        curr_command += ["-p", f"leveldb.base_scaling_factor={k}"] # set scaling factor to k
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]
        print(f"Seeding db with {num_mb} mb test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r2.stdout.find("Run throughput")
        num2 = float((r2.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, c, num2))
        f.write("\n-----TEST WRITE " + str(num_mb) + "-----\n")
        f.write(r2.stderr)
        f.write(r2.stdout)
        f.write("\n-----TEST WRITE FINISHED" + str(num_mb) + "-----\n")

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for num_mb in mb_to_write:
        print(f"Forcing filters for {num_mb} mb base...")
        r = subprocess.run(["./seed", "0", f"{db_path}fig1_base_{num_mb}", "1"], capture_output=True, encoding="utf-8")
        f.write("\n-----BASE FILTER " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("\n-----BASE FILTER FINISHED-----\n")

        print(f"Forcing filters for {num_mb} mb test...")
        r = subprocess.run(["./seed", "0", f"{db_path}fig1_test_{num_mb}" + str(num_mb), "1"], capture_output=True, encoding="utf-8")
        f.write("\n-----TEST FILTER " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("\n-----TEST FILTER FINISHED-----\n")

    
    os.chdir("../../YCSB-cpp")

# Perform Point Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s", "-p", 'leveldb.filter_bits=5,5,5,5,5,5,5']
for num_mb in mb_to_write:
    print(f"Reading from db with {num_mb} mb base...")
    curr_command = base_write_args.copy()
    curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("READ", num_mb, 1, num))

    f.write("\n-----BASE READ " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("\n-----BASE READ FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"ratio_diff={c}"]
    print(f"Reading from db with {num_mb} mb test...")
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("READ", num_mb, c, num2))
    f.write("\n-----TEST READ " + str(num_mb) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)
    f.write("\n-----TEST READ FINISHED-----\n")

print(results)


# Perform Range Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/scan_uniform", "-s"]
base_write_args += ["-p", f"leveldb.base_scaling_factor={T}"]
for num_mb in mb_to_write:
    print(f"Scanning from db with {num_mb} mb base...")
    curr_command = base_write_args.copy()

    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]

    # must be last
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("SCAN", num_mb, 1, num))

    f.write("\n-----BASE SCAN " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("\n-----BASE SCAN FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"ratio_diff={c}"]
    print(f"Scanning from db with {num_mb} mb test...")
    f.write(str(curr_command))
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("SCAN", num_mb, c, num2))
    f.write("\n-----TEST SCAN " + str(num_mb) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)

    f.write("\n-----TEST SCAN FINISHED-----\n")

print(results)

f.close()
with open("../logs/fig_1_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    