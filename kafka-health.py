from collections import OrderedDict
import getopt
import pickle
import platform
import os
import re
import subprocess
import sys

# Define Global Variables
script_root = sys.path[0]
script_ext = '.bat' if platform.system() == 'Windows' else '.sh'
script_sep = os.sep
kafka_root = script_root + script_sep + 'kafka'
kafka_bin = (kafka_root + script_sep + 'bin' + script_sep + 'windows') if platform.system() == 'Windows'\
             else (kafka_root + script_sep + 'bin')


# Check for supported OS
def check_os():
    if platform.system() in ('Windows', 'Linux'):
        return 0
    return 1


# Run process
def run_process(args):
    print("Args: [", end='')
    print(*args, sep=', ', end='')
    print("]")
    process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = process.communicate()
    if process.returncode != 0:
        print("Error running cmd: %s", args)
        print(err)
    return out, err


def save_variables(filename, variable):
    with open(filename, 'wb') as file:
        pickle.dump(variable, file)


def load_variables(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)


def create_list_from_output(data):
    data = [re.split('\s{1,}', x) for x in data.decode('utf-8').split('\r\n')]
    data[0].remove('')
    return data


def create_dict_from_list(data):
    data_set = OrderedDict()
    keys = []
    for x in range(len(data)):
        data_row = OrderedDict()
        if x == 0:
            for y in range(len(data[x])):
                keys.append(data[x][y])
            continue
        if len(data[x]) != len(keys):
            continue
        for y in range(len(data[x])):
            data_row[keys[y]] = data[x][y]
        data_set[x] = data_row
    return data_set


def filter_dict_by_keys(data, keys):
    data_set = OrderedDict()
    for p_key, p_value in data.items():
        data_row = OrderedDict()
        for s_key in keys:
            data_row[s_key] = p_value[s_key]
        data_set[p_key] = data_row
    return data_set


def filter_dict_drop_zero(data, key):
    return OrderedDict(filter(lambda x: ((int(x[1][key]) if x[1][key].isdigit() else int(0)) > 0), data.items()))


def sort_dict_by_key(data, key, rev):
    return OrderedDict(sorted(data.items(), key=lambda x: int(x[1][key]) if x[1][key].isdigit() else int(0),
                              reverse=rev))


def print_dict(data):
    for p_key, p_value in data.items():
        print(p_key, end=' ')
        for s_key, s_value in p_value.items():
            print(s_key + ":" + s_value, end=' ')
        print()


def print_list(data):
    for p_item in data:
        for s_item in p_item:
            print(s_item, end='\t')
        print()


def main(argv):
    if check_os():
        exit(1)

    try:
        opts, args = getopt.getopt(argv, "hs:g:", ["server=", "group="])
    except getopt.GetoptError:
        print(sys.argv[0] + ' -s <kafka server> -g <consumer group>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(sys.argv[0] + ' -s <kafka server> -g <consumer group>')
            sys.exit()
        elif opt in ("-s", "--server"):
            kafka_server = arg
        elif opt in ("-g", "--group"):
            kafka_group = arg

    cmd_get_groups = [kafka_bin + script_sep + 'kafka-consumer-groups' + script_ext, '--bootstrap-server',
                      kafka_server, '--list']
    cmd_get_lag = [kafka_bin + script_sep + 'kafka-consumer-groups' + script_ext, '--bootstrap-server',
                   kafka_server, '--group', kafka_group, '--describe']

    save_data = script_root + script_sep + "kafka_output.pkl"

    if os.path.isfile(save_data):
        output = load_variables(save_data)
        # print(output)
    else:
        (output, error) = run_process(cmd_get_lag)
        save_variables(save_data, output)
        # print(output)
        # print(error)

    my_list = create_list_from_output(output)
    my_dict = create_dict_from_list(my_list)
    my_filtered_dict = filter_dict_by_keys(my_dict, ['TOPIC', 'HOST', 'LAG'])
    my_sorted_dict = sort_dict_by_key(my_filtered_dict, 'LAG', True)

    # print(my_list)
    # print_list(my_list)
    # print_dict(my_dict)
    # print_dict(my_filtered_dict)
    # print_dict(my_sorted_dict)
    print_dict(filter_dict_drop_zero(my_sorted_dict, 'LAG'))


if __name__ == '__main__':
    main(sys.argv[1:])
