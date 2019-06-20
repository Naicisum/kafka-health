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
use_cache = False


# OS Functions
def check_os():
    if platform.system() in ('Windows', 'Linux'):
        return 0
    return 1


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


# Data caching functions
def save_variables(filename, variable):
    with open(filename, 'wb') as file:
        pickle.dump(variable, file)


def load_variables(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)


# Dictionary manipulation functions
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


def create_list_from_output(data):
    data = [re.split('\s{1,}', x) for x in data.decode('utf-8').split('\r\n')]
    if data[0] == '':
        data[0].remove('')
    else:
        data = [x for x in data if x != ['']]
    return data


def filter_dict_by_keys(data, keys):
    data_set = OrderedDict()
    for p_key, p_value in data.items():
        data_row = OrderedDict()
        for s_key in keys:
            data_row[s_key] = p_value[s_key]
        data_set[p_key] = data_row
    return data_set


def filter_dict_where_key_equal_value(data, key, value):
    data_set = OrderedDict()
    for k, v in data.items():
        if v[key] == value:
            data_set[k] = v
    return data_set


def filter_dict_where_key_value_gt_zero(data, key):
    return OrderedDict(filter(lambda x: ((int(x[1][key]) if x[1][key].isdigit() else int(0)) > 0), data.items()))


def sort_dict_by_key(data, key, rev):
    return OrderedDict(sorted(data.items(), key=lambda x: int(x[1][key]) if x[1][key].isdigit() else int(0),
                              reverse=rev))


def sum_dict_value_by_key(data, key):
    key_sum = int()
    for k, v in data.items():
        if isinstance(v, dict):
            key_sum += sum_dict_value_by_key(v, key)
        else:
            if k == key:
                key_sum += int(v)
    return key_sum


def count_dict_value_by_key(data, key):
    key_count = int()
    for k, v in data.items():
        if isinstance(v, dict):
            key_count += sum_dict_value_by_key(v, key)
        else:
            if k == key:
                key_count += 1
    return key_count


def unique_dict_values_by_key(data, key):
    data_set = list()
    for k, v in data.items():
        if v[key] not in data_set:
            data_set.append(v[key])
    return data_set


def unique_dict_keys(data):
    data_set = list()
    for k, v in data.items():
        for sk, sv in v.items():
            if sk not in data_set:
                data_set.append(sk)
        break
    return data_set


# Utility Functions
def print_dict(data):
    for p_key, p_value in data.items():
        print(p_key, end=' ')
        for s_key, s_value in p_value.items():
            print(s_key + ":" + s_value, end=' ')
        print()


def print_list(data):
    index = 0
    if isinstance(data[0], list):
        for p_item in data:
            print(str(index) + " : ", end=' ')
            for s_item in p_item:
                print(s_item, end=' ')
            print()
            index += 1
    else:
        for p_item in data:
            print(str(index) + " : " + p_item)
            index += 1


# Health Check Functions
def get_consumer_groups_list(kafka_server):
    cmd_get_groups = [kafka_bin + script_sep + 'kafka-consumer-groups' + script_ext, '--bootstrap-server',
                      kafka_server, '--list']
    save_data = script_root + script_sep + "kafka_groups.pkl"
    if os.path.isfile(save_data) and use_cache:
        output = load_variables(save_data)
    else:
        (output, error) = run_process(cmd_get_groups)
        save_variables(save_data, output)
    return create_list_from_output(output)


def get_consumer_groups_detail(kafka_server, kafka_group):
    cmd_get_lag = [kafka_bin + script_sep + 'kafka-consumer-groups' + script_ext, '--bootstrap-server',
                   kafka_server, '--group', kafka_group, '--describe']

    save_data = script_root + script_sep + "kafka_detail_"+kafka_group+".pkl"

    if os.path.isfile(save_data) and use_cache:
        output = load_variables(save_data)
    else:
        (output, error) = run_process(cmd_get_lag)
        save_variables(save_data, output)
    return create_list_from_output(output)


# Main Entry
def main(argv):
    if check_os():
        print("Unsupported OS, exiting...")
        exit(2)

    try:
        opts, args = getopt.getopt(argv, "hcs:g:", ["server=", "group=", "cache"])
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
        elif opt in ("-c", "--cache"):
            use_cache = True

    # my_list = create_list_from_output(output)
    # my_dict = create_dict_from_list(my_list)
    # my_filtered_dict = filter_dict_by_keys(my_dict, ['TOPIC', 'HOST', 'LAG'])
    # my_sorted_dict = sort_dict_by_key(my_filtered_dict, 'LAG', True)
    # my_zero_dict = filter_dict_where_key_value_gt_zero(my_sorted_dict, 'LAG')
    # my_value_filtered_dict = filter_dict_where_key_equal_value(my_zero_dict, 'TOPIC', 'OpenNMS.Sink.Telemetry-Netflow-5')
    # my_unique_topics = unique_dict_values_by_key(my_dict, 'TOPIC')
    # my_unique_hosts = unique_dict_values_by_key(my_dict, 'HOST')
    # my_unique_keys = unique_dict_keys(my_dict)

    kafka_topics = get_consumer_groups_list(kafka_server)
    kafka_data = get_consumer_groups_detail(kafka_server, kafka_group)

if __name__ == '__main__':
    main(sys.argv[1:])
