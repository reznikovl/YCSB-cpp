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
f = open("logs/fig_2_" + timestr + ".txt", "w+")

# add list to track results
result_fields = ["Operation", "Key Size (bytes)", "Ratio", "Time"]
results = []

# key sizes to write
key_sizes = [16, 64, 1024]

if (sys.argv[1] == "1"):
    # do writes
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s"]
    for key_size in key_sizes:
        print("Seeding db with " + str(key_size) + " key size base...")
        curr_command = base_write_args.copy()
        
        curr_command += ["-p", "keysize=" + str(key_size)]
        op_count = 2048 * 1024 * 1024 // key_size
        curr_command += ["-p", "operationcount=" + str(op_count)] # 1024 * 1024 / 128
        curr_command += ["-p", "leveldb.dbname=/tmp/fig2_base_" + str(key_size)] # must be last

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", key_size, 1, num))

        f.write("-----BASE WRITE " + str(key_size) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("-----BASE WRITE FINISHED-----\n")

        curr_command.pop() # pop previous db name
        curr_command += ["leveldb.dbname=/tmp/fig2_test_" + str(key_size)]
        curr_command += ["-p", "leveldb.ratio_diff=0.66666667"]
        print("Seeding db with " + str(key_size) + " key size test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r2.stdout.find("Run throughput")
        num2 = float((r2.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", key_size, 0.66666667, num2))
        f.write("-----TEST WRITE " + str(key_size) + "-----\n")
        f.write(r2.stderr)
        f.write(r2.stdout)

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for key_size in key_sizes:
        print("Forcing filters for " + str(key_size) + " key size base...")
        r = subprocess.run(["./seed", "0", "/tmp/fig2_base_" + str(key_size), "1"], capture_output=True, encoding="utf-8")
        f.write("-----BASE FILTER " + str(key_size) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----BASE FILTER FINISHED-----\n")

        print("Forcing filters for " + str(key_size) + " key size test...")
        r = subprocess.run(["./seed", "0", "/tmp/fig2_test_" + str(key_size), "1"], capture_output=True, encoding="utf-8")
        f.write("-----TEST FILTER " + str(key_size) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----TEST FILTER FINISHED-----\n")

    
    os.chdir("../../YCSB-cpp")

# Perform Point Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s"]
for key_size in key_sizes:
    print("Reading from db with " + str(key_size) + " key size base...")
    curr_command = base_write_args.copy()

    curr_command += ["-p", "keysize=" + str(key_size)]

    # must be last
    curr_command += ["-p", "leveldb.dbname=/tmp/fig2_base_" + str(key_size)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("READ", key_size, 1, num))

    f.write("-----BASE READ " + str(key_size) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----BASE READ FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += ["leveldb.dbname=/tmp/fig2_test_" + str(key_size)]
    curr_command += ["-p", "ratio_diff=0.66666667"]
    print("Reading from db with " + str(key_size) + " key size test...")
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("READ", key_size, 0.66666667, num2))
    f.write("-----TEST READ " + str(key_size) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)

print(results)


# Perform Range Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/scan_uniform", "-s"]
for key_size in key_sizes:
    print("Scanning from db with " + str(key_size) + " key size base...")
    curr_command = base_write_args.copy()

    curr_command += ["-p", "keysize=" + str(key_size)]

    # must be last
    curr_command += ["-p", "leveldb.dbname=/tmp/fig2_base_" + str(key_size)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("SCAN", key_size, 1, num))

    f.write("-----BASE SCAN " + str(key_size) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----BASE SCAN FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += ["leveldb.dbname=/tmp/fig2_test_" + str(key_size)]
    curr_command += ["-p", "ratio_diff=0.66666667"]
    print("Scanning from db with " + str(key_size) + " key size test...")
    f.write(str(curr_command))
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("SCAN", key_size, 0.66666667, num2))
    f.write("-----TEST SCAN " + str(key_size) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)

print(results)

f.close()
with open("../logs/fig_2_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    