import csv
import subprocess


# Define Global Variables
kafka_root = '/opt/kafka'


def run_process(args):
    proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = proc.communicate()
    if proc.returncode != 0:
        print("Error running cmd: %s", args)
        print(err)
    return out


