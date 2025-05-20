from pandas import read_csv

tests = [
    (pq, sq, d, s, inst)
    for pq   in [0, 1, 2]
    for sq   in [0, 1, 2]
    for d    in [0, 100]
    for s    in [0, 1000, 4000]
    for inst in [1, 5, 10]
]

percentage_losses = {}



def get_total_messages_sent_for_config(test):
    total_msgs = 0
    str_config = '-'.join([str(setting) for setting in test])
    for publisher_id in range(1, 11):
        publisher_logs = read_csv(f"publisher_logs/{publisher_id}.csv", index_col=0)
        relevant_publisher_logs = publisher_logs[publisher_logs['config']==str_config]
        total_msgs += relevant_publisher_logs['cont'].sum()
    return total_msgs

def get_total_messages_received_for_config(test):
    total_msgs = 0
    str_config = '-'.join([str(setting) for setting in test])
    received_logs = read_csv(f"analyser_logs/{str_config}.csv", index_col=0)
    return received_logs.shape[0]

for test in tests:
    str_config = '-'.join([str(setting) for setting in test])
    total_msgs_sent_for_test = get_total_messages_sent_for_config(test)
    total_msgs_received_for_test = get_total_messages_received_for_config(test)
    percentage_losses[str_config] = 100 * (1 - total_msgs_received_for_test/total_msgs_sent_for_test)

    