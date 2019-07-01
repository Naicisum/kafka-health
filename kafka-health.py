from collections import OrderedDict
import getopt
import pandas as pd
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
debug = False

# OS Functions
def check_os():
    if platform.system() in ('Windows', 'Linux'):
        return 0
    return 1


def run_process(args):
    if debug:
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
    if isinstance(data[0], list):
        data[0] = list(filter(None, data[0]))
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
            key_sum += int(sum_dict_value_by_key(v, key))
        else:
            if k == key:
                key_sum += int(v) if v.isdigit() else int(0)
    return str(key_sum)


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
    if data.__len__() != 0:
        sort = unique_dict_keys(data)[-1]
        data_frame = pd.DataFrame.from_dict(data, orient='index', columns=unique_dict_keys(data))
        data_frame[sort] = pd.to_numeric(data_frame[sort])
        print(data_frame.sort_values(sort).to_string(index=False))

        # for p_key, p_value in data.items():
        #    print(p_key, end=' ')
        #    for s_key, s_value in p_value.items():
        #       print(s_key + ":" + str(s_value), end=' ')
        #    print()
    else:
        print("No lags greater than 0")


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
    global use_cache
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
    global use_cache
    cmd_get_lag = [kafka_bin + script_sep + 'kafka-consumer-groups' + script_ext, '--bootstrap-server',
                   kafka_server, '--group', kafka_group, '--describe']

    save_data = script_root + script_sep + "kafka_detail_"+kafka_group+".pkl"

    if os.path.isfile(save_data) and use_cache:
        output = load_variables(save_data)
    else:
        (output, error) = run_process(cmd_get_lag)
        save_variables(save_data, output)
    return create_list_from_output(output)


def report_lag_per_host(data):
    data_set = OrderedDict()
    iter = int(0)
    key_filter = ['HOST', 'TOPIC', 'LAG']
    data_x = filter_dict_where_key_value_gt_zero(filter_dict_by_keys(data, key_filter), 'LAG')
    unique_hosts = unique_dict_values_by_key(data_x, 'HOST')
    for host in unique_hosts:
        data_y = filter_dict_where_key_equal_value(data_x, 'HOST', host)
        unique_topics = unique_dict_values_by_key(data_y, 'TOPIC')
        for topic in unique_topics:
            data_z = sum_dict_value_by_key(filter_dict_where_key_equal_value(data_y, 'TOPIC', topic), 'LAG')
            data_set[iter] = {'HOST': host, 'TOPIC': topic, 'LAG': data_z}
            iter += 1
    return sort_dict_by_key(data_set, 'LAG', True)


def report_lag_per_topic(data):
    data_set = OrderedDict()
    iter = int(0)
    key_filter = ['TOPIC', 'LAG']
    data_x = filter_dict_where_key_value_gt_zero(filter_dict_by_keys(data, key_filter), 'LAG')
    unique_topics = unique_dict_values_by_key(data_x, 'TOPIC')
    for topic in unique_topics:
        data_y = sum_dict_value_by_key(filter_dict_where_key_equal_value(data_x, 'TOPIC', topic), 'LAG')
        data_set[iter] = {'TOPIC': topic, 'LAG': data_y}
        iter += 1
    return sort_dict_by_key(data_set, 'LAG', True)


# Main Entry
def main(argv):
    global use_cache
    global debug
    use_topic = False
    use_host = False
    kafka_group = list()

    if check_os():
        print("Unsupported OS, exiting...")
        exit(2)

    try:
        opts, args = getopt.getopt(argv, "?cs:g:th", ["server=", "group=", "cache", "by-topic", "by-host"])
    except getopt.GetoptError:
        print(sys.argv[0] + ' -s <kafka server> -g <consumer group> [--by-topic or --by-host]')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-?':
            print(sys.argv[0] + ' -s <kafka server> -g <consumer group> [--by-topic or --by-host]')
            sys.exit()
        elif opt in ("-s", "--server"):
            kafka_server = arg
        elif opt in ("-g", "--group"):
            kafka_group = arg
        elif opt in ("-c", "--cache"):
            use_cache = True
        elif opt in ("-t", "--by-topic"):
            use_topic = True
        elif opt in ("-h", "--by-host"):
            use_host = True
        elif opt in ("-d", "--debug"):
            debug = True

    if kafka_group == "" or kafka_group is None:
        kafka_consumers = get_consumer_groups_list(kafka_server)
    else:
        kafka_consumers = list()
        kafka_consumers.append([kafka_group])

    if use_host is True or use_topic is True:
        for consumer in kafka_consumers:
            print("Processing Consumer: " + consumer[0])
            kafka_data = create_dict_from_list(get_consumer_groups_detail(kafka_server, consumer[0]))
            if use_host:
                print_dict(report_lag_per_host(kafka_data))
            if use_topic:
                print_dict(report_lag_per_topic(kafka_data))
    else:
        print("Error: Output by Topic or Host not specified")


if __name__ == '__main__':
    main(sys.argv[1:])
