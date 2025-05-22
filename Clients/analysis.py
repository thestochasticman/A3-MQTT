from collections import Counter
from pandas import DataFrame
from os.path import dirname
from pprint import pprint
import numpy as np

def get_rate_of_receive(rec_logs: dict):
    return round(
        (sum([len(records) for pub_id, records in rec_logs.items()])/30), 
        2
    )

def get_message_loss_per_publisher(pub_summary: dict, rec_summary: dict):
    losses = []
    for pub_id in pub_summary:
        total_sent = pub_summary[pub_id]
        total_received = len(rec_summary[pub_id])
        losses += [100 * (1 - (total_received/total_sent))]
    return round(sum(losses) / len(losses), 2)

def get_out_of_order_rate_per_publisher(rec_logs: dict):
    def out_of_order_pct(counter_list):
        if len(counter_list) < 2:
            return 0.0                     # nothing to compare
        ooo = sum(
            1 for i in range(1, len(counter_list))
            if counter_list[i] < counter_list[i-1]
        )
        return 100 * ooo / (len(counter_list) - 1)
    pcts = []
    for records in rec_logs.values():
        counts = [record['counter'] for record in records]
        pcts += [out_of_order_pct(counts)]
    return round(sum(pcts)/len(pcts), 2)


def get_mean_and_std_of_avg_msg_gap_per_publisher(rec_logs: dict):

    def get_per_publisher_gap_stats(rec_logs: dict):
        """
        rec_logs: dict[int, List[dict]]  
            Mapping publisher_id → list of records, each record must have:
            { 'counter': int, 'timestamp': int }

        Returns:
            pd.DataFrame with index=publisher_id and columns [mean_gap_ms, std_gap_ms]
        """
        stats = []
        for pub_id, records in rec_logs.items():
            # Sort by counter
            recs = sorted(records, key=lambda r: r['counter'])
            counters   = [r['counter']   for r in recs]
            timestamps = [r['ts_rec'] for r in recs]

            # Compute only truly consecutive gaps
            gaps = []
            for i in range(len(counters)-1):
                if counters[i+1] == counters[i] + 1:
                    gaps.append(timestamps[i+1] - timestamps[i])

            if gaps:
                stats.append({
                    'publisher_id': pub_id,
                    'avg_gap_ms': np.mean(gaps),
                })
            else:
                # no valid gaps
                stats.append({
                    'publisher_id': pub_id,
                    'avg_gap_ms': np.nan,
                })

        df = DataFrame(stats).set_index('publisher_id')
        return df

    df = get_per_publisher_gap_stats(rec_logs)
    return float(round(df['avg_gap_ms'].mean(), 2)), float(round(df['avg_gap_ms'].std(), 2))

def get_duplication_rate(rec_logs: dict):
    pcts = []
    for records in rec_logs.values():
        counts = [record['counter'] for record in records]
        total = len(counts)
        freq = Counter(counts)
        duplicate_count=  sum((n - 1) for n in freq.values() if n > 1)
        pct = 100 * duplicate_count / total if total else 0.0
        pcts += [pct]
    return round(sum(pcts)/len(pcts), 2)
    
def analyse(str_config: str, pub_logs: dict, rec_logs: dict):
    results = {}
    results['str_config'] = str_config
    results['rate_of_receive'] = get_rate_of_receive(rec_logs)
    results['mean_msg_loss'] = get_message_loss_per_publisher(pub_logs, rec_logs)
    results['out_of_order'] = get_out_of_order_rate_per_publisher(rec_logs)
    results['duplication_rate'] = get_duplication_rate(rec_logs) 
    results['mean_gap_ms'], results['std_gap_ms'] = get_mean_and_std_of_avg_msg_gap_per_publisher(rec_logs)
    pprint(results)
