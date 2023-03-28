import sys
import time
import os
import subprocess
import csv

# mb to write
mb_to_write = 1024 * 8

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
f = open("../logs/fig_1_" + timestr + ".txt", "w+")
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
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_update", "-s", "-p", f"keysize={key_size_bytes}"]
    for value_size in value_sizes_bytes:
        print(f"Seeding db with {value_size} value size...")
        curr_command = base_write_args.copy()

        # set value size
        curr_command += ["-p", f"leveldb.fieldlength={value_size}"]

        # set T
        curr_command += ["-p", f"leveldb.base_scaling_factor={T}"]
        

        op_count = (mb_to_write * 1024 * 1024 // (key_size_bytes + value_size)) / 0.8
        curr_command += ["-p", f"operationcount={op_count}"]
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_base_{value_size}"] # must be last


        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----BASE WRITE " + str(value_size) + " -----\n")
        f.write(str(curr_command))
        f.write(r.stderr)
        f.write(r.stdout)


        f.write("\n-----BASE WRITE FINISHED-----\n")

        curr_command = base_write_args.copy()
        curr_command += ["-p", f"leveldb.base_scaling_factor={k}"] # set scaling factor to k
        curr_command += ["-p", f"operationcount={op_count}"] # set operation count
        curr_command += ["-p", f"leveldb.dbname={db_path}space_amp_test_{value_size}"]
        curr_command += ["-p", f"leveldb.ratio_diff={c}"]
        print(f"Seeding db with {value_size} value size test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write("\n-----TEST WRITE " + str(value_size) + "-----\n")
        f.write(str(curr_command))

        f.write("\n-----TEST WRITE FINISHED-----\n")

    print("Finished Writes")

# calculate sizes
os.chdir("../leveldb/build")
for value_size in value_sizes_bytes:
    print(f"Evaluating Space Amplification of DB with {value_size} value size Base...")
    f.write(f"\n-----GETTING SIZE FOR {value_size}-----")
    command = ["./get_space_amplification", f"{db_path}space_amp_test_{value_size}"]
    f.write(str(command))
    r = subprocess.run(command, capture_output=True, encoding="utf-8")
    f.write(r.stderr)
    result = int(r.stdout)
    results.append((value_size, 1, ((mb_to_write * 1024 * 1024 // (key_size_bytes + value_size)) / 0.8), result))
    




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
        r = subprocess.run(["./seed", "0", f"{db_path}fig1_test_{num_mb}", "1"], capture_output=True, encoding="utf-8")
        f.write("\n-----TEST FILTER " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("\n-----TEST FILTER FINISHED-----\n")

    
    os.chdir("../../YCSB-cpp")

f.close()
with open("../logs/fig_1_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    