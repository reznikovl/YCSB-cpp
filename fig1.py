import sys
import time
import os
import subprocess
import csv

# mb to write
# mb_to_write = [1024*1, 1024*2, 1024*4, 1024*6, 1024*8, 1024*10, 1024*12, 1024*16]
mb_to_write = [1024*3, 1024*16, 1024*29, 1024*42]

# mb_to_write = [1024*1, 1024*2, 1024*4]
db_path = "/home/ec2-user/research/mountpt/"

# T (ratio for leveling)
T = 2

# k (ratio for Autumn)
k = 2

# autumn parameter to use
c = 0.8

key_size_bytes = 16
value_size_bytes = 100

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
result_fields = ["Operation", "Size (MB)", "Ratio", "Latency", "Throughput"]
results = []

def clear_cache():
    f2 = None
    try:
        f2 = open("/proc/sys/vm/drop_caches", "a")
        r = subprocess.run(["sync"])
        r2 = subprocess.run(["echo", "3"], stdout=f2)
        if r.returncode != 0 or r2.returncode != 0:
            print("Error clearing cache...")
            f.write("\nError Clearing Cache...\n")
        else:
            print("Cleared Cache")
            f.write("\nCleared Cache Succesfully\n")
    except Exception as e:
        print("Exception when opening /proc/ file for caching:", e)
        print("Try running with sudo or double checking machine reqs")
        f.write("Exception when opening /proc/ file for caching:")
        f.write(str(e))
        f.write("Try running with sudo or double checking machine reqs")

if (sys.argv[1] == "1"):
    # do writes
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s", "-p", "recordcount=1000000", "-p", f"keysize={key_size_bytes}", "-p", f"fieldlength={value_size_bytes}",] # for now, we are scanning for no duplicates
    for num_mb in mb_to_write:
        print(f"Seeding db with {num_mb} mb base...")
        curr_command = base_write_args.copy()

        # set scaling factor to T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]

        # set sleep time (approx 10 minutes for 32 GB and scaled based on size)
        curr_command += ["-p", f"leveldb.sleep_time={num_mb // 20}"]
        

        op_count = num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last


        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----BASE WRITE " + str(num_mb) + " -----\n")
        f.write(str(curr_command))
        f.write(r.stderr)
        f.write(r.stdout)
        
        parse_location_latency = r.stdout.rfind("Avg=")
        latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
        parse_location_throughput = r.stdout.find("Run throughput")
        throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, 1, latency, throughput))
        
        f.write("\n-----BASE WRITE FINISHED-----\n")

        curr_command = base_write_args.copy()
        curr_command += ["-p", f"leveldb.base_scaling_factor={k}"] # set scaling factor to k
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]

        # set sleep time (approx 10 minutes for 32 GB and scaled based on size)
        curr_command += ["-p", f"leveldb.sleep_time={num_mb // 20}"]
        print(f"Seeding db with {num_mb} mb test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----TEST WRITE " + str(num_mb) + "-----\n")
        f.write(str(curr_command))

        parse_location_latency = r2.stdout.rfind("Avg=")
        latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
        parse_location_throughput = r2.stdout.find("Run throughput")
        throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, 0.8, latency, throughput))
        
        f.write(r2.stderr)
        f.write(r2.stdout)
        f.write("\n-----TEST WRITE FINISHED" + str(num_mb) + "-----\n")

    print(results)
clear_cache()


# Perform Short Range Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/scan_uniform", "-s", "-p", "scanlength=10", "-p", "ops_to_skip=100000"]
base_write_args += ["-p", f"leveldb.base_scaling_factor={T}"]
for num_mb in mb_to_write:
    print(f"Short Scanning from db with {num_mb} mb base...")
    curr_command = base_write_args.copy()

    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]

    # must be last
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"]
    f.write("\n-----BASE SHORT SCAN " + str(num_mb) + " -----\n")
    f.write(str(curr_command))
    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

    f.write(r.stderr)
    f.write(r.stdout)

    parse_location_latency = r.stdout.rfind("Avg=")
    latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r.stdout.find("Run throughput")
    throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("SHORT SCAN", num_mb, 1, latency, throughput))

    
    

    f.write("\n-----BASE SHORT SCAN FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"leveldb.ratio_diff={c}"]
    print(f"Short Scanning from db with {num_mb} mb test...")
    f.write("\n-----TEST SHORT SCAN " + str(num_mb) + "-----\n")
    f.write(str(curr_command))
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    f.write(r2.stderr)
    f.write(r2.stdout)

    parse_location_latency = r2.stdout.rfind("Avg=")
    latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r2.stdout.find("Run throughput")
    throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("SHORT SCAN", num_mb, 0.8, latency, throughput))
    

    f.write("\n-----TEST SHORT SCAN FINISHED-----\n")

print(results)

clear_cache()

# Perform Baseline reads no bloom filter
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s", "-p", "ops_to_skip=100000"]
for num_mb in mb_to_write:
    print(f"Reading from db with {num_mb} mb base with no filter...")
    curr_command = base_write_args.copy()
    curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----BASE READ NO FILTER" + str(num_mb) + " -----\n")
    f.write(str(curr_command))
    f.write(r.stderr)
    f.write(r.stdout)

    parse_location_latency = r.stdout.rfind("Avg=")
    latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r.stdout.find("Run throughput")
    throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("READ NO FILTER", num_mb, 1, latency, throughput))


    

    f.write("\n-----BASE READ NO FILTER FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"leveldb.ratio_diff={c}"]
    print(f"Reading from db with {num_mb} mb test...")

    f.write("\n-----TEST READ NO FILTER" + str(num_mb) + "-----\n")
    f.write(str(curr_command))

    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    
    f.write(r2.stderr)
    f.write(r2.stdout)

    parse_location_latency = r2.stdout.rfind("Avg=")
    latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r2.stdout.find("Run throughput")
    throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("READ NO FILTER", num_mb, 0.8, latency, throughput))
    
    
    f.write("\n-----TEST READ NO FILTER FINISHED-----\n")

print(results)

clear_cache()

# Perform Long Range Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/scan_uniform", "-s", "-p", "scanlength=100", "-p", "ops_to_skip=100000"]
base_write_args += ["-p", f"leveldb.base_scaling_factor={T}"]
for num_mb in mb_to_write:
    print(f"Long Scanning from db with {num_mb} mb base...")
    curr_command = base_write_args.copy()

    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]

    # must be last
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"]
    f.write("\n-----BASE LONG SCAN " + str(num_mb) + " -----\n")
    f.write(str(curr_command))
    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

    f.write(r.stderr)
    f.write(r.stdout)

    parse_location_latency = r.stdout.rfind("Avg=")
    latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r.stdout.find("Run throughput")
    throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("LONG SCAN", num_mb, 1, latency, throughput))

    
    

    f.write("\n-----BASE LONG SCAN FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"leveldb.ratio_diff={c}"]
    print(f"Long Scanning from db with {num_mb} mb test...")
    f.write("\n-----TEST LONG SCAN " + str(num_mb) + "-----\n")
    f.write(str(curr_command))
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    f.write(r2.stderr)
    f.write(r2.stdout)

    parse_location_latency = r2.stdout.rfind("Avg=")
    latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r2.stdout.find("Run throughput")
    throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("LONG SCAN", num_mb, 0.8, latency, throughput))
    

    f.write("\n-----TEST LONG SCAN FINISHED-----\n")

print(results)


# Force Filters
os.chdir("../leveldb/build")
for num_mb in mb_to_write:
    print(f"Forcing filters for {num_mb} mb base...")
    r = subprocess.run(["./seed", "0", f"{db_path}fig1_base_{num_mb}", "1", "1"], capture_output=True, encoding="utf-8")
    f.write("\n-----BASE FILTER " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)
    f.write("\n-----BASE FILTER FINISHED-----\n")

    print(f"Forcing filters for {num_mb} mb test...")
    r = subprocess.run(["./seed", "0", f"{db_path}fig1_test_{num_mb}", "1", "1"], capture_output=True, encoding="utf-8")
    f.write("\n-----TEST FILTER " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)
    f.write("\n-----TEST FILTER FINISHED-----\n")


os.chdir("../../YCSB-cpp")

clear_cache()

# Perform Point Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s", "-p", 'leveldb.filter_bits=5,5,5,5,5,5,5', "-p", "ops_to_skip=100000"]
for num_mb in mb_to_write:
    print(f"Reading from db with {num_mb} mb base...")
    curr_command = base_write_args.copy()
    curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----BASE READ " + str(num_mb) + " -----\n")
    f.write(str(curr_command))
    f.write(r.stderr)
    f.write(r.stdout)

    parse_location_latency = r.stdout.rfind("Avg=")
    latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r.stdout.find("Run throughput")
    throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("READ", num_mb, 1, latency, throughput))


    

    f.write("\n-----BASE READ FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += [f"leveldb.dbname={db_path}fig1_test_{num_mb}"]
    curr_command += ["-p", f"leveldb.ratio_diff={c}"]
    print(f"Reading from db with {num_mb} mb test...")

    f.write("\n-----TEST READ " + str(num_mb) + "-----\n")
    f.write(str(curr_command))

    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    
    f.write(r2.stderr)
    f.write(r2.stdout)

    parse_location_latency = r2.stdout.rfind("Avg=")
    latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r2.stdout.find("Run throughput")
    throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("READ", num_mb, 0.8, latency, throughput))
    
    
    f.write("\n-----TEST READ FINISHED-----\n")

print(results)



# Force Filters with No Monkey
os.chdir("../leveldb/build")
for num_mb in mb_to_write:
    print(f"Forcing filters for {num_mb} mb base no Monkey...")
    r = subprocess.run(["./seed", "0", f"{db_path}fig1_base_{num_mb}", "1", "0"], capture_output=True, encoding="utf-8")
    f.write("\n-----BASE NO MONKEY FILTER " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)
    f.write("\n-----BASE NO MONKEY FILTER FINISHED-----\n")

os.chdir("../../YCSB-cpp")

clear_cache()

# Perform Point Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s", "-p", 'leveldb.filter_bits=5,5,5,5,5,5,5', "-p", "ops_to_skip=100000"]
for num_mb in mb_to_write:
    print(f"Reading from db with {num_mb} mb base No Monkey...")
    curr_command = base_write_args.copy()
    curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
    curr_command += ["-p", f"recordcount={num_mb * 1024 * 1024 // (key_size_bytes + value_size_bytes)}"]
    curr_command += ["-p", f"leveldb.dbname={db_path}fig1_base_{num_mb}"] # must be last

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write("\n-----BASE READ NO MONKEY" + str(num_mb) + " -----\n")
    f.write(str(curr_command))
    f.write(r.stderr)
    f.write(r.stdout)

    parse_location_latency = r.stdout.rfind("Avg=")
    latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
    parse_location_throughput = r.stdout.find("Run throughput")
    throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
    results.append(("READ NO MONKEY", num_mb, 1, latency, throughput))


    

    f.write("\n-----BASE READ NO MONKEY FINISHED-----\n")

print(results)




f.close()
with open("../logs/fig_1_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)



    
    




    