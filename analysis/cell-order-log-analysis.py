import numpy as np
import re
import ast

CELL_ORDER_LOG_PATTERN = ".*ts_ms:(?P<ts>\d*) slice_metrics:(?P<metrics_dict>.*)"
CELL_ORDER_CONF_PATTERN = ".*Cell-Order configuration: ((?P<conf_dict>{.*}))"

cell_order_log_filename = '/logs/cell-order.log'

SLA_PERIOD = 30 # seconds over which the SLAs are negotiated

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
                                     'raw_buf_bytes': []}

                retval[s_idx]['raw_ts_sec'].append( ts - ts_min )
                retval[s_idx]['raw_lat_msec'].append( float(metrics['dl_latency [msec]']) )
                # retval[s_idx]['raw_n_rbgs'].append( int(metrics['new_num_rbgs']) )
                retval[s_idx]['raw_n_rbgs'].append( int(metrics['cur_slice_mask'].count('1')) )
                retval[s_idx]['raw_tx_mbps'].append( float(metrics['tx_brate downlink [Mbps]']) )
                retval[s_idx]['raw_buf_bytes'].append( float(metrics['dl_buffer [bytes]']) )

    for s_idx, s_data in retval.items():
        retval[s_idx]['raw_ts_sec'] = np.array(s_data['raw_ts_sec'])
        retval[s_idx]['raw_lat_msec'] = np.array(s_data['raw_lat_msec'])
        retval[s_idx]['raw_n_rbgs'] = np.array(s_data['raw_n_rbgs'])
        retval[s_idx]['raw_tx_mbps'] = np.array(s_data['raw_tx_mbps'])
        retval[s_idx]['raw_buf_bytes'] = np.array(s_data['raw_buf_bytes'])

    return retval, slice_delay_budget_msec

def summarize_over_sla_period(raw_data, period):

    for s_idx, s_data in raw_data.items():
        if (period == 0):
            raw_data[s_idx]['ts_sec'] = raw_data[s_idx]['raw_ts_sec']
            raw_data[s_idx]['lat_msec'] = raw_data[s_idx]['raw_lat_msec']
            raw_data[s_idx]['tx_mbps'] = raw_data[s_idx]['raw_tx_mbps']
            raw_data[s_idx]['buf_bytes'] = raw_data[s_idx]['raw_buf_bytes']
            continue

        cur_ts = raw_data[s_idx]['raw_ts_sec'][0]
        raw_data[s_idx]['ts_sec'] = []
        raw_data[s_idx]['lat_msec'] = []
        raw_data[s_idx]['tx_mbps'] = []
        raw_data[s_idx]['buf_bytes'] = []
        while (cur_ts <= raw_data[s_idx]['raw_ts_sec'][-1]):
            cur_idx_filter = np.logical_and(raw_data[s_idx]['raw_ts_sec'] >= cur_ts, 
                                            raw_data[s_idx]['raw_ts_sec'] < cur_ts + period)
            if (cur_idx_filter.any()):
                raw_data[s_idx]['ts_sec'].append(cur_ts + period)
                raw_data[s_idx]['lat_msec'].append(np.mean(raw_data[s_idx]['raw_lat_msec'][cur_idx_filter]))
                raw_data[s_idx]['tx_mbps'].append(np.mean(raw_data[s_idx]['raw_tx_mbps'][cur_idx_filter]))
                raw_data[s_idx]['buf_bytes'].append(np.mean(raw_data[s_idx]['raw_buf_bytes'][cur_idx_filter]))
            cur_ts += period

        raw_data[s_idx]['ts_sec'] = np.array(raw_data[s_idx]['ts_sec'])
        raw_data[s_idx]['lat_msec'] = np.array(raw_data[s_idx]['lat_msec'])
        raw_data[s_idx]['tx_mbps'] = np.array(raw_data[s_idx]['tx_mbps'])
        raw_data[s_idx]['buf_bytes'] = np.array(raw_data[s_idx]['buf_bytes'])
            
if __name__ == '__main__':

    data, slice_delay_budget_msec = read_cell_order_log(cell_order_log_filename)
    summarize_over_sla_period(data, SLA_PERIOD)

    for s_idx, metrics in data.items():
        log_str = "\n\tLatency values for slice {}:".format(s_idx)
        for margin in [0, 5, 10]:
            budget_lo = max(0, slice_delay_budget_msec[s_idx][0] - margin)
            budget_hi = max(0, slice_delay_budget_msec[s_idx][1] + margin)
            filter = np.logical_and(metrics['lat_msec'] >= budget_lo,
                                    metrics['lat_msec'] <= budget_hi)
            success_ratio = 100. * len(metrics['lat_msec'][filter]) / len(metrics['lat_msec'])
            log_str += " ([{},{}]: {:.2f}%)".format(budget_lo, budget_hi, success_ratio)
        
        low_latency_filter = metrics['lat_msec'] <= slice_delay_budget_msec[s_idx][1]
        low_latency_ratio = 100. * len(metrics['lat_msec'][low_latency_filter]) / len(metrics['lat_msec'])
        log_str += " (Low-Lat Rate: {:.2f}%)".format(low_latency_ratio)

        print("{}\n\n{}".format(log_str, metrics['lat_msec']))