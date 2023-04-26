import sys
import time
import os
import subprocess
import csv
from multiprocessing import Process



def KeepConnection(stub):
    while 1:
        time.sleep(180)
        print("continuing")

p = Process(target=KeepConnection, args=("",))
p.daemon = True
p.start()

# load 42 GB
mb_to_write = [1024*42]

db_path = "/home/ec2-user/research/mountpt/"

# T (ratio for leveling)
T = 2

# k (ratio for Autumn)
k = 2

# autumn parameter to use
c = 0.8


if len(sys.argv) < 2:
    print("Please specify 1 if dbs need to be seeded else 0")
    sys.exit()

# open log file
timestr = time.strftime("%Y%m%d-%H%M%S")
f = open("../logs/YCSB_Workloads_" + timestr + ".txt", "w+")
f.write("Parameters: ")
f.write(f"T={T}\n")
f.write(f"k={k}\n")
f.write(f"c={c}\n")



# add list to track results
result_fields = ["Operation", "Size (MB)", "Ratio", "Latency", "Throughput"]
results = []


if (sys.argv[1] == "1"):
    # load and use ycsb default params
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/ycsb_load", "-s", "-p", "recordcount=1000000",] # for now, we are scanning for no duplicates
    for num_mb in mb_to_write:
        print(f"Seeding db with {num_mb} mb base...")
        curr_command = base_write_args.copy()

        # set scaling factor to T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]

        # set sleep time (approx 10 minutes for 32 GB and scaled based on size)
        curr_command += ["-p", f"leveldb.sleep_time={num_mb // 10}"]
        

        op_count = int(num_mb * 1024 * 1024 // 1024) #ycsb default 1KB
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"leveldb.dbname={db_path}ycsb_workloads_base_{num_mb}"] # must be last


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
        curr_command += ["-p", f"leveldb.sleep_time={num_mb // 10}"]
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}ycsb_workloads_test_{num_mb}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]
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


 
for task in ["workloadc","workloade","workloadb", "workloadd", "workloadf","workloada"]:
    base_write_args = ["./ycsb", "-run", "-db",
                       "leveldb", "-P", "workloads/"+task, "-s"]
    base_write_args += ["-p", f"leveldb.base_scaling_factor={T}"]

    for num_mb in mb_to_write:
        curr_command = base_write_args.copy()
        curr_command += ["-p", f"recordcount={int(num_mb * 1024 * 1024 // 1024)}"]
        curr_command += ["-p", "operationcount=1000000"]

        curr_command += ["-p", f"leveldb.dbname={db_path}ycsb_workloads_test_{num_mb}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]

        print(f"{task} from db with {num_mb} mb test...")

        f.write("\n-----TEST WORKLOADE " + str(num_mb) + "-----\n")
        f.write(str(curr_command))
        r2 = subprocess.run(
            curr_command, capture_output=True, encoding="utf-8")
        f.write(r2.stderr)
        f.write(r2.stdout)

        parse_location_latency = r2.stdout.rfind("Avg=")
        latency = float(r2.stdout[parse_location_latency:].split()[0].split("=")[1])
        parse_location_throughput = r2.stdout.find("Run throughput")
        throughput = float((r2.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
        results.append((task, num_mb, 0.8, latency, throughput))
        

        f.write("\n-----TEST WORKLOAD FINISHED-----\n")


        curr_command.pop()
        curr_command.pop()
        curr_command.pop()
        curr_command.pop()

        curr_command += ["-p", f"leveldb.dbname={db_path}ycsb_workloads_base_{num_mb}"]
        print(f"{task} from db with {num_mb} mb BASE...")
        f.write("\n-----BASE WORKLOAD " + str(num_mb) + " -----\n")
        f.write(str(curr_command))
        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

        f.write(r.stderr)
        f.write(r.stdout)

        parse_location_latency = r.stdout.rfind("Avg=")
        latency = float(r.stdout[parse_location_latency:].split()[0].split("=")[1])
        parse_location_throughput = r.stdout.find("Run throughput")
        throughput = float((r.stdout[parse_location_throughput + 24:]).strip())  # magic number :(
        results.append((task, num_mb, 1, latency, throughput))
        f.write("\n-----BASE WORKLOAD FINISHED-----\n")

    print(results)


f.close()
with open("../logs/YCSB_Workloads_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)

exit()

