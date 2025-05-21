from pandas import read_csv
from os.path import exists
from pandas import DataFrame
from pandas import concat
from pprint import pprint
from pandas import Series
import numpy as np

tests = [
    (pq, sq, d, s, inst)
    for pq   in [0, 1, 2]
    for sq   in [0, 1, 2]
    for d    in [0, 100]
    for s    in [0, 1000, 4000]
    for inst in [1, 5, 10]
]

receive_rate_per_test = {}
mean_loss_per_test = {}
mean_out_of_order_rate_per_test = {}
mean_duplicate_msg_rate_per_test = {}
mean_gap_per_test = {}
std_gap_per_test = {}

summary_rows = []

publisher_dfs = [
    read_csv(f"publisher_logs/{p}.csv") for p in range(1, 11)
]

def get_total_messages_sent_for_config(test):
    total_msgs = 0
    str_config = '-'.join([str(setting) for setting in test])
    for publisher_id in range(1, 11):
        if exists(f"publisher_logs/{publisher_id}.csv"):
            publisher_logs = publisher_dfs[publisher_id-1]
            relevant_publisher_logs = publisher_logs[publisher_logs['config']==str_config]
            total_msgs += relevant_publisher_logs['count'].sum()
    return total_msgs

def get_total_messages_received_for_config(df_rec):
    return df_rec.shape[0]

def get_messages_sent_per_publisher(test):
    str_config = '-'.join([str(setting) for setting in test])
    sent_per_publisher = []
    for publisher in range(1, 11):
        if exists(f"publisher_logs/{publisher}.csv"):
            df = publisher_dfs[publisher - 1]
            msgs = df[df['config'] == str_config]
            count = msgs['count'].sum()
            if count > 0:
                sent_per_publisher += [{'publisher': publisher, 'sent': count}]
    return sent_per_publisher

def get_messages_received_per_publisher(df):
    received_per_publisher = []
    for publisher in range(1, test[-1] + 1):
        msgs = df[df['id'] == publisher]
        if msgs.shape[0] > 0:
            received_per_publisher += [{'publisher': publisher, 'received': msgs.shape[0]}]
    return received_per_publisher

def get_mean_loss_per_publisher_for_test(test, df_rec):
    sent_per_publisher = get_messages_sent_per_publisher(test)
    received_per_publisher = get_messages_received_per_publisher(df_rec)
    df_sent = DataFrame.from_records(sent_per_publisher, index='publisher')
    df_received = DataFrame.from_records(received_per_publisher, index='publisher')
    df = concat([df_sent, df_received], axis=1)
    df['loss'] = 100 * (1 - df['received']/df['sent'])
    return float(df['loss'].mean())

def get_mean_out_order_rate_for_test(df):
    df['counter'] = df["msg"].str.extract(r"(\d+):")
    df["counter"] = df["counter"].astype(int)

    def compute_out_of_order(group):
        counters = group["counter"].tolist()
        out_of_order = sum(
            1 for i in range(1, len(counters)) if counters[i] < counters[i - 1]
        )
        total = len(counters)
        out_of_order_pct = (out_of_order / (total - 1)) * 100 if total > 1 else 0.0
        return Series({
            "total_msgs": total,
            "out_of_order_count": out_of_order,
            "out_of_order_pct": out_of_order_pct
        })
   
    per_publisher_df = df.groupby("id").apply(compute_out_of_order).reset_index()
    mean_out_of_order_pct = per_publisher_df["out_of_order_pct"].mean()
    return float(mean_out_of_order_pct)

def get_mean_duplicate_rate_per_test(df):
    def compute_duplicates(group):
        messages = group["msg"]
        total = len(messages)
        duplicate_count = messages.duplicated().sum()
        duplicate_pct = (duplicate_count / total) * 100 if total > 0 else 0.0
        return Series({
            "total_msgs": total,
            "duplicate_count": duplicate_count,
            "duplicate_pct": duplicate_pct
        })
    
    per_publisher_dupes = df.groupby("id").apply(compute_duplicates).reset_index()
    mean_duplicate_pct = per_publisher_dupes["duplicate_pct"].mean()
    return mean_duplicate_pct

def get_average_timestamp_mean_and_std_for_test(df):
    df[["counter", "timestamp"]] = df["msg"].str.extract(r"(\d+):(\d+)")
    df["counter"] = df["counter"].astype(int)
    df["timestamp"] = df["timestamp"].astype(np.int64)

    def compute_gaps(group):
        group_sorted = group.sort_values("counter").reset_index(drop=True)
        consecutive = group_sorted["counter"].diff().eq(1)
        valid_indices = consecutive[consecutive].index

        if len(valid_indices) < 1:
            return Series({
                "mean_gap_ms": None,
                "std_gap_ms": None,
                "valid_gap_count": 0
            })

        gaps = group_sorted.loc[valid_indices, "timestamp"].values - group_sorted.loc[valid_indices - 1, "timestamp"].values

        return Series({
            "mean_gap_ms": gaps.mean(),
            "std_gap_ms": gaps.std(ddof=1),
        })
    per_publisher_gap_stats = df.groupby("id").apply(compute_gaps).reset_index()
    return per_publisher_gap_stats['mean_gap_ms'].mean(), per_publisher_gap_stats['std_gap_ms'].mean()


for i, test in enumerate(tests):
    str_config = '-'.join([str(setting) for setting in test])
    df_rec = read_csv(f"analyser_logs/{str_config}.csv", index_col=0)
    total_msgs_received_for_test = get_total_messages_received_for_config(df_rec)
    mean_loss_per_test[str_config] = get_mean_loss_per_publisher_for_test(test, df_rec)

    mean_out_of_order_rate_per_test[str_config] = get_mean_out_order_rate_for_test(df_rec)
    mean_duplicate_msg_rate_per_test[str_config] = get_mean_duplicate_rate_per_test(df_rec)

    mean, std = get_average_timestamp_mean_and_std_for_test(df_rec)
    mean_gap_per_test[str_config] = mean
    std_gap_per_test[str_config] = std

    summary_rows.append({
        "config": str_config,
        "pub_qos": test[0],
        "sub_qos": test[1],
        "delay": test[2],
        "msg_size": test[3],
        "instance_count": test[4],
        "mean_loss_pct": mean_loss_per_test.get(str_config),
        "mean_out_of_order_pct": mean_out_of_order_rate_per_test.get(str_config),
        "mean_duplicate_pct": mean_duplicate_msg_rate_per_test.get(str_config),
        "mean_gap_ms": mean_gap_per_test.get(str_config),
        "std_gap_ms": std_gap_per_test.get(str_config),
    })

summary_df = DataFrame(summary_rows)
summary_df.to_csv("summary_results.csv", index=False)
# print("✅ Saved summary to summary_results.csv")

