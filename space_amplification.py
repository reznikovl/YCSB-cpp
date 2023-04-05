import sys
import time
import os
import subprocess
import csv

# mb to write
mb_to_write = 8192

# mb_to_write = [1024*1, 1024*2, 1024*4]
db_path = "/home/ec2-user/research/mountpt/"

# T (ratio for leveling)
T = 2

# k (ratio for Autumn)
k = 2

# autumn parameter to use
c = 0.7

key_size_bytes = 16
value_sizes_bytes = [16, 64, 128, 1024]

if len(sys.argv) < 2:
    print("Please specify 1 if dbs need to be seeded else 0")
    sys.exit()

# open log file
timestr = time.strftime("%Y%m%d-%H%M%S")
f = open("../logs/space_amp_" + timestr + ".txt", "w+")
f.write("Space Amplification Test")
f.write("Parameters: ")
f.write(f"T={T}\n")
f.write(f"k={k}\n")
f.write(f"c={c}\n")
f.write(f"key size (bytes) = {key_size_bytes}\n")
f.write(f"Database Approx Size: {mb_to_write}")
# add list to track results
result_fields = ["Value Size", "Ratio", "Logical Size", "Actual Size"]
results = []




if (sys.argv[1] == "1"):
    # do writes
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s", "-p", f"keysize={key_size_bytes}"]
    for value_size in value_sizes_bytes:
        print(f"Seeding db with {value_size} value size base...")
        curr_command = base_write_args.copy()

        # set value size
        curr_command += ["-p", f"fieldlength={value_size}"]

        # set T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
        

        op_count = int(mb_to_write * 1024 * 1024 // (key_size_bytes + value_size))
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"recordcount=0"]
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_base_{value_size}"] # must be last

        f.write("\n-----BASE WRITE " + str(value_size) + " -----\n")
        f.write(str(curr_command))


        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        
        f.write(r.stderr)
        f.write(r.stdout)


        f.write("\n-----BASE WRITE FINISHED-----\n")

        curr_command = base_write_args.copy()
        curr_command += ["-p", f"leveldb.base_scaling_factor={k}"] # set scaling factor to k
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_test_{value_size}"]
        curr_command += ["-p", f"recordcount=0"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]
        curr_command += ["-p", f"fieldlength={value_size}"]
        print(f"Seeding db with {value_size} value size test...")
        f.write("\n-----TEST WRITE " + str(value_size) + "-----\n")
        f.write(str(curr_command))
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(r2.stdout)
        f.write(r2.stderr)

        f.write("\n-----TEST WRITE FINISHED-----\n")

    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/update_uniform", "-s", "-p", f"keysize={key_size_bytes}"]
    for value_size in value_sizes_bytes:
        print(f"Updating db with {value_size} value size base...")
        curr_command = base_write_args.copy()

        # set value size
        curr_command += ["-p", f"fieldlength={value_size}"]

        # set T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
        

        op_count = int(0.2 * mb_to_write * 1024 * 1024 // (key_size_bytes + value_size))
        record_count = mb_to_write * 1024 * 1024 // (key_size_bytes + value_size)
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"recordcount={record_count}"]
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_base_{value_size}"] # must be last

        f.write("\n-----BASE UPDATE " + str(value_size) + " -----\n")
        f.write(str(curr_command))


        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        
        f.write(r.stderr)
        f.write(r.stdout)


        f.write("\n-----BASE UPDATE FINISHED-----\n")

        curr_command = base_write_args.copy()
        curr_command += ["-p", f"leveldb.base_scaling_factor={k}"] # set scaling factor to k
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_test_{value_size}"]
        curr_command += ["-p", f"recordcount={record_count}"]
        curr_command += ["-p", f"fieldlength={value_size}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]
        print(f"Updating db with {value_size} value size test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----TEST UPDATE " + str(value_size) + "-----\n")
        f.write(str(curr_command))

        f.write("\n-----TEST UPDATE FINISHED-----\n")

    print("Finished Updating")

# calculate sizes
os.chdir("../leveldb/build")
for value_size in value_sizes_bytes:
    print(f"Evaluating Space Amplification of DB with {value_size} value size Base...")
    f.write(f"\n-----GETTING SIZE FOR {value_size} BASE-----")
    command = ["./get_space_amplification", f"{db_path}space_amp_base_{value_size}"]
    f.write(str(command))
    r = subprocess.run(command, capture_output=True, encoding="utf-8")
    f.write(r.stderr)
    result = int(r.stdout)
    results.append((value_size, 1, mb_to_write * 1024 * 1024, result))

    print(f"Evaluating Space Amplification of DB with {value_size} value size Test...")
    f.write(f"\n-----GETTING SIZE FOR {value_size} TEST-----")
    command = ["./get_space_amplification", f"{db_path}space_amp_test_{value_size}"]
    f.write(str(command))
    r = subprocess.run(command, capture_output=True, encoding="utf-8")
    f.write(r.stderr)
    result = int(r.stdout)
    results.append((value_size, c, mb_to_write * 1024 * 1024, result))
    
os.chdir("../../YCSB-cpp")
f.close()
print(results)
with open("../logs/space_amp_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    