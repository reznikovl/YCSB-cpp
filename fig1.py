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
f = open("../logs/fig_1_" + timestr + ".txt", "w+")

# add list to track results
result_fields = ["Operation", "Size (MB)", "Ratio", "Time"]
results = []

# mb to write
mb_to_write = [1024, 2048, 3076]
db_path = "/home/ec2-user/research/mountpt/"

# test ratio to use
test_ratio = 0.8

if (sys.argv[1] == "1"):
    # do writes
    base_write_args = ["./ycsb", "-run", "-db", "leveldb", "-P", "workloads/write_uniform", "-s"]
    for num_mb in mb_to_write:
        print("Seeding db with " + str(num_mb) + " mb base...")
        curr_command = base_write_args.copy()
        

        op_count = num_mb * 1024 * 8
        curr_command += ["-p", "operationcount=" + str(op_count)] # 1024 * 1024 / 128
        curr_command += ["-p", "leveldb.dbname=" + db_path + "fig1_base_" + str(num_mb)] # must be last

        r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r.stdout.find("Run throughput")
        print("Parse location: ", parse_location)
        print((r.stdout[parse_location + 24:]).strip())
        num = float((r.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, 1, num))

        f.write("-----BASE WRITE " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        
        f.write("-----BASE WRITE FINISHED-----\n")

        curr_command.pop() # pop previous db name
        curr_command += ["leveldb.dbname=" + db_path + "fig1_test_" + str(num_mb)]
        curr_command += ["-p", "leveldb.ratio_diff=" + str(test_ratio)]
        print("Seeding db with " + str(num_mb) + " mb test...")
        r2 = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
        f.write(str(curr_command))

        parse_location = r2.stdout.find("Run throughput")
        print("Parse location: ", parse_location)
        print((r.stdout[parse_location + 24:]).strip())
        num2 = float((r2.stdout[parse_location + 24:]).strip())  # magic number :(
        results.append(("WRITE", num_mb, test_ratio, num2))
        f.write("-----TEST WRITE " + str(num_mb) + "-----\n")
        f.write(r2.stderr)
        f.write(r2.stdout)

    print(results)

# add filters to db
if int(sys.argv[1]) >= 1:
    os.chdir("../leveldb/build")
    for num_mb in mb_to_write:
        print("Forcing filters for " + str(num_mb) + " mb base...")
        r = subprocess.run(["./seed", "0", db_path + "fig1_base_" + str(num_mb), "1"], capture_output=True, encoding="utf-8")
        f.write("-----BASE FILTER " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----BASE FILTER FINISHED-----\n")

        print("Forcing filters for " + str(num_mb) + " mb test...")
        r = subprocess.run(["./seed", "0", db_path + "fig1_test_" + str(num_mb), "1"], capture_output=True, encoding="utf-8")
        f.write("-----TEST FILTER " + str(num_mb) + " -----\n")
        f.write(r.stderr)
        f.write(r.stdout)
        f.write("-----TEST FILTER FINISHED-----\n")

    
    os.chdir("../../YCSB-cpp")

# Perform Point Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/read_uniform", "-s"]
for num_mb in mb_to_write:
    print("Reading from db with " + str(num_mb) + " mb base...")
    curr_command = base_write_args.copy()

    # must be last
    curr_command += ["-p", "leveldb.dbname=" + db_path + "fig1_base_" + str(num_mb)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("READ", num_mb, 1, num))

    f.write("-----BASE READ " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----BASE READ FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += ["leveldb.dbname=" + db_path + "fig1_test_" + str(num_mb)]
    curr_command += ["-p", "ratio_diff=" + str(test_ratio)]
    print("Reading from db with " + str(num_mb) + " mb test...")
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")
    f.write(str(curr_command))

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("READ", num_mb, test_ratio, num2))
    f.write("-----TEST READ " + str(num_mb) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)

print(results)


# Perform Range Reads
base_write_args = ["./ycsb", "-run", "-db",
                   "leveldb", "-P", "workloads/scan_uniform", "-s"]
for num_mb in mb_to_write:
    print("Scanning from db with " + str(num_mb) + " mb base...")
    curr_command = base_write_args.copy()

    # must be last
    curr_command += ["-p", "leveldb.dbname=" + db_path + "fig1_base_" + str(num_mb)]

    r = subprocess.run(curr_command, capture_output=True, encoding="utf-8")

    parse_location = r.stdout.find("Run throughput")
    num = float((r.stdout[parse_location + 24:]
                 ).strip())  # magic number :(
    results.append(("SCAN", num_mb, 1, num))

    f.write("-----BASE SCAN " + str(num_mb) + " -----\n")
    f.write(r.stderr)
    f.write(r.stdout)

    f.write("-----BASE SCAN FINISHED-----\n")

    curr_command.pop()  # pop previous db name
    curr_command += ["leveldb.dbname=" + db_path + "fig1_test_" + str(num_mb)]
    curr_command += ["-p", "ratio_diff=" + str(test_ratio)]
    print("Scanning from db with " + str(num_mb) + " mb test...")
    f.write(str(curr_command))
    r2 = subprocess.run(
        curr_command, capture_output=True, encoding="utf-8")

    parse_location = r2.stdout.find("Run throughput")
    num2 = float((r2.stdout[parse_location + 24:]
                  ).strip())  # magic number :(
    results.append(("SCAN", num_mb, test_ratio, num2))
    f.write("-----TEST SCAN " + str(num_mb) + "-----\n")
    f.write(r2.stderr)
    f.write(r2.stdout)

print(results)

f.close()
with open("../logs/fig_1_" + timestr + ".csv", 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(result_fields)
    csv_out.writerows(results)
    




    