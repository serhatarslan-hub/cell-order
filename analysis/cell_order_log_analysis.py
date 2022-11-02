import argparse
import numpy as np
import re
import ast

CELL_ORDER_LOG_PATTERN = ".*ts_ms:(?P<ts>\d*) slice_metrics:(?P<metrics_dict>.*)"
CELL_ORDER_CONF_PATTERN = ".*Cell-Order configuration: ((?P<conf_dict>{.*}))"
CELL_ORDER_UE_LOG_PATTERN = ".*ts_ms:(?P<ts>\d*) stream:(?P<stream_dict>.*)"
CELL_ORDER_UE_SLICE_PATTERN = ".*slice_id:(?P<slice_id>.*)"

def read_cell_order_log(filename, ts_start=None):
    retval = {}
    with open(filename,'r') as f:
        for line in f:
            if ('ts_ms' not in line):
                if ('slice-delay-budget-msec' in line):
                    conf = re.match(CELL_ORDER_CONF_PATTERN, line)
                    conf_dict = ast.literal_eval(conf.group('conf_dict'))
                    slice_delay_budget_msec = conf_dict['slice-delay-budget-msec']
                continue

            log = re.match(CELL_ORDER_LOG_PATTERN, line)
            ts = float(log.group('ts')) / 1000. # in sec
            metrics_dict = ast.literal_eval(log.group('metrics_dict'))

            if not ts_start:
                ts_start = ts
            elif ts < ts_start:
                continue

            for s_idx, metrics in metrics_dict.items():
                if s_idx not in list(retval):
                    retval[s_idx] = {'raw_ts_sec':[], 
                                     'raw_lat_msec': [], 
                                     'raw_n_rbgs': [],
                                     'raw_tx_mbps': [], 
                                     'raw_buf_bytes': [],
                                     'raw_mcs': [],
                                     'raw_cqi': []}

                retval[s_idx]['raw_ts_sec'].append( ts - ts_start )
                retval[s_idx]['raw_lat_msec'].append( float(metrics['dl_latency [msec]']) )
                # retval[s_idx]['raw_n_rbgs'].append( int(metrics['new_num_rbgs']) )
                retval[s_idx]['raw_n_rbgs'].append( int(metrics['cur_slice_mask'].count('1')) )
                retval[s_idx]['raw_tx_mbps'].append( float(metrics['tx_brate downlink [Mbps]']) )
                retval[s_idx]['raw_buf_bytes'].append( float(metrics['dl_buffer [bytes]']) )
                if ('dl_mcs' in metrics.keys()):
                    retval[s_idx]['raw_mcs'].append( float(metrics['dl_mcs']) )
                if ('dl_cqi' in metrics.keys()):
                    retval[s_idx]['raw_cqi'].append( float(metrics['dl_cqi']) )

    for s_idx, s_data in retval.items():
        for metric_type, metric_vals in s_data.items():
            retval[s_idx][metric_type] = np.array(metric_vals)

    print("Data for {} seconds has been extracted".format(max(retval[s_idx]['raw_ts_sec'])))
    return retval, slice_delay_budget_msec, ts_start

def read_cell_order_ue_log(filename, ts_start=None):
    retval = {'raw_ts_sec':[],
              'raw_mbps':[],
              'raw_cwnd_bytes':[],
              'raw_bytes':[],
              'raw_rtt_msec':[],
              'raw_rttvar_msec':[],
              'raw_n_rtx':[]}

    with open(filename,'r') as f:
        for line in f:
            if ('ts_ms' not in line):
                if ('slice_id' in line):
                    slice_id_log = re.match(CELL_ORDER_UE_SLICE_PATTERN, line)
                    slice_id = int(slice_id_log.group('slice_id'))
                continue

            log = re.match(CELL_ORDER_UE_LOG_PATTERN, line)
            ts = float(log.group('ts')) / 1000. # in sec
            stream_dict = ast.literal_eval(log.group('stream_dict'))

            if not ts_start:
                ts_start = ts
            elif ts < ts_start:
                continue

            retval['raw_ts_sec'].append( ts - ts_start )
            retval['raw_mbps'].append( float(stream_dict['bits_per_second']) / 1e6)
            retval['raw_cwnd_bytes'].append( float(stream_dict['snd_cwnd']) )
            retval['raw_bytes'].append( float(stream_dict['bytes']) )
            retval['raw_rtt_msec'].append( float(stream_dict['rtt']) / 1e3)
            retval['raw_rttvar_msec'].append( float(stream_dict['rttvar']) / 1e3)
            retval['raw_n_rtx'].append( int(stream_dict['retransmits']) )

    for key, val in retval.items():
        retval[key] = np.array(val)

    print("UE Data for {} seconds has been extracted".format(max(retval['raw_ts_sec'])))
    return retval, slice_id, ts_start

def summarize_over_sla_period(raw_data, sla_period, outlier_percentile = 5):

    for s_idx, s_data in raw_data.items():
        if (sla_period == 0):
            for raw_key, val in raw_data[s_idx].items():
                if ('raw_' in raw_key):
                    new_key = raw_key.replace('raw_', '')
                    raw_data[s_idx][new_key] = raw_data[s_idx][raw_key]
            continue

        assert 'raw_ts_sec' in raw_data[s_idx].keys(), "Raw Data must have timestamps!"
        cur_ts = raw_data[s_idx]['raw_ts_sec'][0]

        raw_keys = list(raw_data[s_idx].keys())

        # Summarize the metrics over the sla period
        while (cur_ts <= raw_data[s_idx]['raw_ts_sec'][-1]):
            cur_idx_filter = np.logical_and(raw_data[s_idx]['raw_ts_sec'] >= cur_ts, 
                                            raw_data[s_idx]['raw_ts_sec'] < cur_ts + sla_period)
            if (cur_idx_filter.any()):
                for raw_key in raw_keys:
                    assert 'raw' in raw_key

                    new_key = raw_key.replace('raw_', '')
                    if (new_key not in raw_data[s_idx].keys()):
                        # Initialize summarized metrics
                        raw_data[s_idx][new_key] = []

                    cur_vals = raw_data[s_idx][raw_key][cur_idx_filter]
                    if ('ts_sec' in new_key):
                        raw_data[s_idx]['ts_sec'].append(cur_ts + sla_period)

                    elif ('msec' in new_key): # Latency or RTT
                        outlier_filter = np.logical_and(cur_vals <= np.percentile(cur_vals, 100 - outlier_percentile),
                                                        cur_vals >= np.percentile(cur_vals, outlier_percentile))
                        if (outlier_filter.any()):
                            raw_data[s_idx][new_key].append(np.mean(cur_vals[outlier_filter]))
                        else:
                            raw_data[s_idx][new_key].append(0.)

                    else:
                        raw_data[s_idx][new_key].append(np.mean(cur_vals))

            cur_ts += sla_period

        for s_idx, s_data in raw_data.items():
            for key, val in s_data.items():
                if ('raw_' not in key):
                    raw_data[s_idx][key] = np.array(val)

def print_latency_stats(data, start_time, end_time, slice_delay_budget_msec):
    for s_idx, metrics in data.items():
        stat_filter = np.logical_and(metrics['ts_sec'] >= start_time,
                                     metrics['ts_sec'] <= end_time)
        lat_stat_metrics = metrics['lat_msec'][stat_filter]
        log_str = "\n\tLatency values for slice {}:".format(s_idx)
        for margin in [0, 5, 10]:
            budget_lo = max(0, slice_delay_budget_msec[s_idx][0] - margin)
            budget_hi = max(0, slice_delay_budget_msec[s_idx][1] + margin)
            filter = np.logical_and(lat_stat_metrics >= budget_lo,
                                    lat_stat_metrics <= budget_hi)
            success_ratio = 100. * len(lat_stat_metrics[filter]) / len(lat_stat_metrics)
            log_str += " ([{},{}]: {:.2f}%)".format(budget_lo, budget_hi, success_ratio)
        
        low_latency_filter = lat_stat_metrics <= slice_delay_budget_msec[s_idx][1]
        low_latency_ratio = 100. * len(lat_stat_metrics[low_latency_filter]) / len(lat_stat_metrics)
        log_str += " (Low-Lat Rate: {:.2f}%)".format(low_latency_ratio)

        print("{}\n\n{}".format(log_str, lat_stat_metrics))

def print_stats(data, metric, start_time, end_time):
    for s_idx, metrics in data.items():
        stat_filter = np.logical_and(metrics['ts_sec'] >= start_time,
                                     metrics['ts_sec'] <= end_time)
        stat_metrics = metrics[metric][stat_filter]
        print("\n\t{} stats for slice {}:".format(metric.upper(), s_idx))

        print(stat_metrics)
            
if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-file', type=str, default='/logs/cell-order.log', 
                        help='Log file to parse.')
    parser.add_argument('--start-time', type=float, default=0, 
                        help='Time after the first log to start the analysis (sec)')
    parser.add_argument('--end-time', type=float, default=10000, 
                        help='Time after the first log to end the analysis (sec)')
    parser.add_argument('--sla-period', type=float, default=30, 
                        help='Seconds over which the SLAs are negotiated')
    parser.add_argument('--outlier-percentile', type=float, default=0, 
                        help='Percentile to clip-off from both ends before calculating SLA')
    parser.add_argument('--print-cqi', action='store_true',
                        help='Whether to print CQI values with the latency stats')
    parser.add_argument('--print-tx-rate', action='store_true',
                        help='Whether to print TX rates with the latency stats')
    args = parser.parse_args()

    data, slice_delay_budget_msec = read_cell_order_log(args.log_file)
    summarize_over_sla_period(data, args.sla_period, args.outlier_percentile)

    print_latency_stats(data, args.start_time, args.end_time, slice_delay_budget_msec)

    if (args.print_cqi):
        print_stats(data, 'cqi', args.start_time, args.end_time)

    if (args.print_tx_rate):
        print_stats(data, 'tx_mbps', args.start_time, args.end_time)