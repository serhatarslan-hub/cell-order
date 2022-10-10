import argparse
import ast
import collections
import json
import logging
import os
import time

import constants
from scope_start import read_metrics, get_metric_value, get_slice_users,\
    read_slice_scheduling, read_slice_mask, write_tenant_slicing_mask,\
    write_slice_scheduling, average_metric, avg_slice_metrics
import mcs_mapper

NUM_SLICE_USERS_KEYWORD = 'num_slice_users'
DL_BUFFER_KEYWORD = 'dl_buffer [bytes]'
DL_THP_KEYWORD = 'tx_brate downlink [Mbps]'
DL_LAT_KEYWORD = 'dl_latency [msec]'
DL_MCS_KEYWORD = 'dl_mcs'


# Calculates the latency for each imsi at each timestep in msec
def calculate_dl_latency_metric(metrics_db: dict) -> None:
    # {imsi->{ts->{metric_name->val}}}
    for imsi, ts_val in metrics_db.items():
        for ts, metrics in ts_val.items():
            queue_size_bits = float(metrics[DL_BUFFER_KEYWORD]) * 8.
            tx_rate_kbps = float(metrics[DL_THP_KEYWORD]) * 1e3
            if (tx_rate_kbps > 0):
                metrics_db[imsi][ts][DL_LAT_KEYWORD] = (queue_size_bits / tx_rate_kbps) # in msec
            else:
                metrics_db[imsi][ts][DL_LAT_KEYWORD] = 0.

def readjust_rbgs_to_capacity(slice_metrics: dict, tot_num_rbg_rqstd: int) -> None:
    logging.info('requested_rbg:{}'.format(tot_num_rbg_rqstd))

    # # Decrease the number of requested RBGs one by one starting with the last slice (largest budget)
    # cur_s_idx_to_readjust = len(list(slice_metrics)) -1
    # while tot_num_rbg_rqstd > constants.MAX_RBG:
    #     cur_s_key = list(slice_metrics)[cur_s_idx_to_readjust]

    #     tot_num_rbg_rqstd -= slice_metrics[cur_s_key]['new_num_rbgs']
    #     slice_metrics[cur_s_key]['new_num_rbgs'] = max(slice_metrics[cur_s_key]['new_num_rbgs'] - 1, 1)
    #     tot_num_rbg_rqstd += slice_metrics[cur_s_key]['new_num_rbgs']

    #     cur_s_idx_to_readjust = (cur_s_idx_to_readjust - 1) % len(list(slice_metrics))

    # Decrease the number of requested RBGs one by one starting with the slice that has the most RBGs
    while tot_num_rbg_rqstd > constants.MAX_RBG:
        cur_s_key = 0
        cur_max_num_rbgs = 0
        for s_key, s_val in slice_metrics.items():
            if s_val['new_num_rbgs'] > cur_max_num_rbgs:
                cur_max_num_rbgs = s_val['new_num_rbgs']
                cur_s_key = s_key

        tot_num_rbg_rqstd -= slice_metrics[cur_s_key]['new_num_rbgs']
        slice_metrics[cur_s_key]['new_num_rbgs'] = max(slice_metrics[cur_s_key]['new_num_rbgs'] - 1, 1)
        tot_num_rbg_rqstd += slice_metrics[cur_s_key]['new_num_rbgs']

# Implement Delay Target aware resource allocation policy
# in this case, assign more resources to slices/users if dl_buffer is above threshold 
# if the current latency is below some lower bound, deallocate resources to reduce resource consumption
def cell_order_for_delay_target(cell_order_config: dict, metrics_db: dict, slice_users: dict, iter_cnt: int) -> dict:

    # Add the latency in milliseconds into the metrics_db
    calculate_dl_latency_metric(metrics_db)

    # get some metric averages {imsi->metric_mean_val}
    avg_dl_lat_msec = average_metric(metrics_db, DL_LAT_KEYWORD)
    avg_dl_buffer_bytes = average_metric(metrics_db, DL_BUFFER_KEYWORD)
    avg_dl_thr_mbps = average_metric(metrics_db, DL_THP_KEYWORD)
    avg_dl_mcs = average_metric(metrics_db, DL_MCS_KEYWORD)

    # sum metrics over slice
    slice_metrics = dict()
    for key, val in slice_users.items():
        slice_metrics[key] = {NUM_SLICE_USERS_KEYWORD: len(val)}

    # average slice metrics into dict {slice_idx->metric_name->metric_mean_val}
    avg_slice_metrics(slice_metrics, slice_users, avg_dl_lat_msec, DL_LAT_KEYWORD)
    avg_slice_metrics(slice_metrics, slice_users, avg_dl_buffer_bytes, DL_BUFFER_KEYWORD)
    avg_slice_metrics(slice_metrics, slice_users, avg_dl_thr_mbps, DL_THP_KEYWORD)
    avg_slice_metrics(slice_metrics, slice_users, avg_dl_mcs, DL_MCS_KEYWORD)

    mask_to_write = False
    tot_num_rbg_rqstd = 0
    for s_key, s_val in slice_metrics.items():
        # get current slicing mask
        slice_metrics[s_key]['cur_slice_mask'] = read_slice_mask(s_key) # string
        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')
        slice_metrics[s_key]['new_num_rbgs'] = cur_num_rbgs

        curr_tx_rate_budget_low = cell_order_config['slice-tx-rate-budget-Mbps'][s_key][0]
        curr_tx_rate_budget_hi = cell_order_config['slice-tx-rate-budget-Mbps'][s_key][1]
        req_n_prbs = mcs_mapper.calculate_n_prbs(curr_tx_rate_budget_hi, 
                                                 round(s_val[DL_MCS_KEYWORD]))

        if (iter_cnt < 1):
            # Make sure to start with a fair allocation
            if (cell_order_config['delay-budget-enabled']):
                slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1
            else:
                slice_metrics[s_key]['new_num_rbgs'] = int(constants.MAX_RBG / len(list(slice_metrics)))

        elif cell_order_config['delay-budget-enabled']:

            curr_lo_delay_budget = cell_order_config['slice-delay-budget-msec'][s_key][0]
            curr_hi_delay_budget = cell_order_config['slice-delay-budget-msec'][s_key][1]

            if s_val[DL_LAT_KEYWORD] > curr_hi_delay_budget or (s_val[DL_THP_KEYWORD] < curr_tx_rate_budget_low and s_val[DL_LAT_KEYWORD] != 0.0):
                # Allocate more resources to this slice
                # slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + 1, req_n_prbs + 2)
                if (cur_num_rbgs > req_n_prbs):
                    slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 2
                    # slice_metrics[s_key]['new_num_rbgs'] = slice_metrics[s_key]['new_num_rbgs'] + 1
                else:
                    slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

                # slice_metrics[s_key]['new_num_rbgs'] = min(req_n_prbs + 1, constants.MAX_RBG)
                # slice_metrics[s_key]['new_num_rbgs'] = min(slice_metrics[s_key]['new_num_rbgs'] + 1, constants.MAX_RBG)
            elif s_val[DL_LAT_KEYWORD] < curr_lo_delay_budget:
                # De-allocate resources from this slice
                if (cur_num_rbgs > req_n_prbs):
                    slice_metrics[s_key]['new_num_rbgs'] = max(req_n_prbs - 1, 1)
                else:
                    slice_metrics[s_key]['new_num_rbgs'] = max(cur_num_rbgs - 1, 1)

                # slice_metrics[s_key]['new_num_rbgs'] = max(slice_metrics[s_key]['new_num_rbgs'] - 1, 1)
                # slice_metrics[s_key]['new_num_rbgs'] = max(req_n_prbs - 1, 1)
            else:
                # Try to maintain the current latency 
                slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

        mask_to_write = mask_to_write or (slice_metrics[s_key]['new_num_rbgs'] != cur_num_rbgs)
        tot_num_rbg_rqstd += slice_metrics[s_key]['new_num_rbgs']

    # get timestamp for logging purposes
    timestamp_ms = int(time.time() * 1000)
    if (iter_cnt > 0):
        # Don't log the first iteration because cell-order hasn't kicked in yet
        logging.info('ts_ms:' + str(timestamp_ms) + ' slice_metrics:' + str(slice_metrics))

    if mask_to_write:

        # Readjust number of RBGs if the total number exceeds the availability
        if tot_num_rbg_rqstd > constants.MAX_RBG:
            readjust_rbgs_to_capacity(slice_metrics, tot_num_rbg_rqstd)

        # Write slice masks for each slice on file
        rbg_idx_to_start = 0
        for s_key, s_val in slice_metrics.items():
            new_mask = '0' * rbg_idx_to_start
            new_mask += '1' * s_val['new_num_rbgs']
            rbg_idx_to_start = len(new_mask)
            new_mask += '0' * (constants.MAX_RBG - rbg_idx_to_start)

            # assemble config parameters dictionary and write mask
            # tenant_number needs to be there but is not used in this case
            config_params = {'network_slicing_enabled': True, 'tenant_number': 1, 'slice_allocation': new_mask}
            write_tenant_slicing_mask(config_params, True, s_key)

        if (iter_cnt < 1):
            # Wait for the initial allocations to stabilize
            time.sleep(5)

    return slice_metrics


# get cell-order parameters from configuration file
def parse_cell_order_config_file(filename: str) -> dict:

    logging.info('Parsing ' + filename + ' configuration file')

    with open(filename, 'r') as file:
        config = json.load(file)

    for param_key, param_val in config.items():
        # convert to right types
        if param_val.lower() in ['true', 'false']:
            config[param_key] = bool(param_val == 'True')
        elif param_key in ['slice-delay-budget-msec', 'slice-tx-rate-budget-Mbps']:
            # Convert some config to python dictionary
            config[param_key] = ast.literal_eval(param_val)
        elif param_key in ['reallocation-period-sec']:
            config[param_key] = float(param_val)

    return config


if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', type=str, required=True, help='Configuration file to parse.')
    parser.add_argument('--t', type=float, default=0, help='Seconds to run for (0 for unlimited)')
    args = parser.parse_args()

    # configure logger and console output
    logging.basicConfig(level=logging.DEBUG, filename='/logs/cell-order.log', filemode='a+',
        format='%(asctime)-15s %(levelname)-8s %(message)s')
    formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    filename = os.path.expanduser('~/radio_api/')
    filename = filename + args.config_file

    # parse heuristic config file
    cell_order_config = parse_cell_order_config_file(filename)
    logging.info('Cell-Order configuration: ' + str(cell_order_config))

    # Scope exposes telemetry every 250 msec
    telemetry_lines_to_read = int(4 * cell_order_config['reallocation-period-sec'])

    tot_iter =  args.t / cell_order_config['reallocation-period-sec']

    iter = -1
    while iter < tot_iter or args.t == 0:

        iter += 1
        logging.info('Starting round ' + str(iter))

        # read metrics database {imsi->{ts->{metric_name->val}}}
        metrics_db = read_metrics(lines_num=telemetry_lines_to_read)

        # get slicing associations {slice_id->(imsi)}
        slice_users = get_slice_users(metrics_db)

        cell_order_for_delay_target(cell_order_config, metrics_db, slice_users, iter)

        time.sleep(cell_order_config['reallocation-period-sec'])