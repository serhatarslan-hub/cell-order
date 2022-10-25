import argparse
import numpy as np
import re
import ast

CELL_ORDER_LOG_PATTERN = ".*ts_ms:(?P<ts>\d*) slice_metrics:(?P<metrics_dict>.*)"
CELL_ORDER_CONF_PATTERN = ".*Cell-Order configuration: ((?P<conf_dict>{.*}))"

def read_cell_order_log(filename):
    retval = {}
    ts_min = None
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

            if not ts_min:
                ts_min = ts

            for s_idx, metrics in metrics_dict.items():
                if s_idx not in list(retval):
                    retval[s_idx] = {'raw_ts_sec':[], 
                                     'raw_lat_msec': [], 
                                     'raw_n_rbgs': [],
                                     'raw_tx_mbps': [], 
                                     'raw_buf_bytes': [],
                                     'raw_mcs': [],
                                     'raw_cqi': []}

                retval[s_idx]['raw_ts_sec'].append( ts - ts_min )
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
    return retval, slice_delay_budget_msec

def summarize_over_sla_period(raw_data, sla_period, outlier_percentile = 5):

    for s_idx, s_data in raw_data.items():
        if (sla_period == 0):
            raw_data[s_idx]['ts_sec'] = raw_data[s_idx]['raw_ts_sec']
            raw_data[s_idx]['lat_msec'] = raw_data[s_idx]['raw_lat_msec']
            raw_data[s_idx]['tx_mbps'] = raw_data[s_idx]['raw_tx_mbps']
            raw_data[s_idx]['buf_bytes'] = raw_data[s_idx]['raw_buf_bytes']
            if ('dl_cqi' in s_data.keys()):
                    raw_data[s_idx]['cqi'] = raw_data[s_idx]['raw_cqi']
            continue

        cur_ts = raw_data[s_idx]['raw_ts_sec'][0]
        raw_data[s_idx]['ts_sec'] = []
        raw_data[s_idx]['lat_msec'] = []
        raw_data[s_idx]['tx_mbps'] = []
        raw_data[s_idx]['buf_bytes'] = []
        raw_data[s_idx]['cqi'] = []
        while (cur_ts <= raw_data[s_idx]['raw_ts_sec'][-1]):
            cur_idx_filter = np.logical_and(raw_data[s_idx]['raw_ts_sec'] >= cur_ts, 
                                            raw_data[s_idx]['raw_ts_sec'] < cur_ts + sla_period)
            if (cur_idx_filter.any()):
                raw_data[s_idx]['ts_sec'].append(cur_ts + sla_period)

                lat_filter = np.logical_and(raw_data[s_idx]['raw_lat_msec'][cur_idx_filter] <= np.percentile(raw_data[s_idx]['raw_lat_msec'][cur_idx_filter], 100 - outlier_percentile),
                                            raw_data[s_idx]['raw_lat_msec'][cur_idx_filter] >= np.percentile(raw_data[s_idx]['raw_lat_msec'][cur_idx_filter], outlier_percentile))
                if (lat_filter.any()):
                    raw_data[s_idx]['lat_msec'].append(np.mean(raw_data[s_idx]['raw_lat_msec'][cur_idx_filter][lat_filter]))
                else:
                    raw_data[s_idx]['lat_msec'].append(0.)
                raw_data[s_idx]['tx_mbps'].append(np.mean(raw_data[s_idx]['raw_tx_mbps'][cur_idx_filter]))
                raw_data[s_idx]['buf_bytes'].append(np.mean(raw_data[s_idx]['raw_buf_bytes'][cur_idx_filter]))
                raw_data[s_idx]['cqi'].append(np.mean(raw_data[s_idx]['raw_cqi'][cur_idx_filter]))
            cur_ts += sla_period

        raw_data[s_idx]['ts_sec'] = np.array(raw_data[s_idx]['ts_sec'])
        raw_data[s_idx]['lat_msec'] = np.array(raw_data[s_idx]['lat_msec'])
        raw_data[s_idx]['tx_mbps'] = np.array(raw_data[s_idx]['tx_mbps'])
        raw_data[s_idx]['buf_bytes'] = np.array(raw_data[s_idx]['buf_bytes'])
        raw_data[s_idx]['cqi'] = np.array(raw_data[s_idx]['cqi'])

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

def print_cqi_stats(data, start_time, end_time):
    for s_idx, metrics in data.items():
        stat_filter = np.logical_and(metrics['ts_sec'] >= start_time,
                                     metrics['ts_sec'] <= end_time)
        cqi_stat_metrics = metrics['cqi'][stat_filter]
        print("\n\tCQI stats for slice {}:".format(s_idx))

        print(cqi_stat_metrics)
            
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
    parser.add_argument('--print-cqi', type=bool, default=False, 
                        help='Whether to print CQI values with the latency stats')
    args = parser.parse_args()

    data, slice_delay_budget_msec = read_cell_order_log(args.log_file)
    summarize_over_sla_period(data, args.sla_period, args.outlier_percentile)

    print_latency_stats(data, args.start_time, args.end_time, slice_delay_budget_msec)

    print_cqi_stats(data, args.start_time, args.end_time)