import asyncio
import logging
import time

import constants
import mcs_mapper
from scope_start import (average_metric, avg_slice_metrics, get_metric_value,
                         get_slice_users, read_metrics, read_slice_mask,
                         read_slice_scheduling, write_slice_scheduling,
                         write_tenant_slicing_mask)


class CellOrderServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
        """
        Args:
            loop: the associated event loop registered to the OS
            config: the configuration to run the server with
        """
        self.loop = loop
        self.config = config
        self.telemetry_lines_to_read = int(4 * self.config['reallocation-period-sec'])
        self.connected_users = {}

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))

        # TODO: Start resource allocation right after receiving the first Consume message
        if (not self.connected_users):
            self.loop.call_later(5, lambda: self.reallocate_resources())

        # TODO: Determine slice ID with a more elegant way
        slice_id = peername[1] - constants.DEFAULT_CELL_ORDER_PORT - 3
        
        self.connected_users[slice_id] = {'transport': transport,
                                          'service_type': 'best_effort',
                                          'negotiated': False,
                                          'sla_end_time': 0}

    def data_received(self, data):
        # TODO: Fill in here

        message = data.decode().strip()
        print('Data received: {!r}'.format(message))

        sla_end_time = time.time() \
                        + self.config['sla-period-sec'] \
                        + self.config['sla-grace-period-sec']

    def calculate_dl_latency_metric(self, metrics_db: dict) -> None:
        # Calculates the latency for each imsi at each timestep in msec

        # {imsi->{ts->{metric_name->val}}}
        for imsi, ts_val in metrics_db.items():
            for ts, metrics in ts_val.items():
                queue_size_bits = float(metrics[constants.DL_BUFFER_KEYWORD]) * 8.
                tx_rate_kbps = float(metrics[constants.DL_THP_KEYWORD]) * 1e3
                if (tx_rate_kbps > 0):
                    metrics_db[imsi][ts][constants.DL_LAT_KEYWORD] = \
                        (queue_size_bits / tx_rate_kbps) # in msec
                else:
                    metrics_db[imsi][ts][constants.DL_LAT_KEYWORD] = 0.

    def provide_latency_service(self, slice_metrics: dict, s_key: int) -> None:
        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')

        # TODO: Dynamically collect budgets from the negotiations
        curr_tx_rate_budget_lo = self.config['slice-tx-rate-budget-Mbps'][s_key][0]
        curr_tx_rate_budget_hi = self.config['slice-tx-rate-budget-Mbps'][s_key][1]
        req_n_prbs = mcs_mapper.calculate_n_prbs(curr_tx_rate_budget_hi, 
                                                    round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))

        curr_lo_delay_budget = self.config['slice-delay-budget-msec'][s_key][0]
        curr_hi_delay_budget = self.config['slice-delay-budget-msec'][s_key][1]

        if (slice_metrics[s_key][constants.DL_LAT_KEYWORD] > curr_hi_delay_budget \
            or (slice_metrics[s_key][constants.DL_THP_KEYWORD] < curr_tx_rate_budget_lo \
                and slice_metrics[s_key][constants.DL_LAT_KEYWORD] != 0.0)):
            # Allocate more resources to this slice
            double_n_prbs = mcs_mapper.calculate_n_prbs(2*curr_tx_rate_budget_hi, 
                                                        round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
            slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + 2, double_n_prbs)
        elif slice_metrics[s_key][constants.DL_LAT_KEYWORD] < curr_lo_delay_budget:
            # De-allocate resources from this slice
            slice_metrics[s_key]['new_num_rbgs'] = max(min(cur_num_rbgs, req_n_prbs) - 2, 1)
        else:
            # Try to maintain the current latency 
            slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

    def readjust_rbgs_to_capacity(self, slice_metrics: dict, tot_num_rbg_rqstd: int) -> None:
        logging.info('requested_rbg:{}'.format(tot_num_rbg_rqstd))

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

    def reallocate_resources(self):

        if (self.connected_users):
            self.loop.call_later(self.config['reallocation-period-sec'], 
                                 lambda: self.reallocate_resources())

        # Read metrics database {imsi->{ts->{metric_name->val}}}
        metrics_db = read_metrics(lines_num = self.telemetry_lines_to_read)
        # Add the latency in milliseconds into the metrics_db
        self.calculate_dl_latency_metric(metrics_db)
        # Get slicing associations {slice_id->(imsi)}
        slice_users = get_slice_users(metrics_db)

        # Create slice metrics to sum metrics over slice
        slice_metrics = dict()
        for key, val in slice_users.items():
            slice_metrics[key] = {constants.NUM_SLICE_USERS_KEYWORD: len(val)}
        metric_keywords_to_ave = [constants.DL_LAT_KEYWORD, 
                                  constants.DL_BUFFER_KEYWORD, 
                                  constants.DL_THP_KEYWORD, 
                                  constants.DL_MCS_KEYWORD, 
                                  constants.DL_CQI_KEYWORD]
        for metric_keyword in metric_keywords_to_ave:
            # get metric averages {imsi->metric_mean_val}
            metric_avg = average_metric(metrics_db, metric_keyword)
            # average slice metrics into dict {slice_idx->metric_name->metric_mean_val}
            avg_slice_metrics(slice_metrics, slice_users, metric_avg, metric_keyword)

        tot_num_rbg_rqstd = 0
        # Allocate RBGs to each user based on their service type and budgets
        for s_key, s_val in slice_metrics.items():
            # get current slicing mask
            slice_metrics[s_key]['cur_slice_mask'] = read_slice_mask(s_key) # string

            if (s_val[constants.DL_CQI_KEYWORD] < 0.5):
                # It is not feasible to allocate good resources for this UE anyway
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (s_key not in self.connected_users):
                # The user has not negotiated for a service yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (not self.connected_users[s_key]['negotiated']):
                # The user has not negotiated for a service yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (self.connected_users[s_key]['service_type'] == 'best_effort'):
                # Assign RBGs that are left after assigning others
                # TODO: Fix allocation
                slice_metrics[s_key]['new_num_rbgs'] = constants.MAX_RBG

            elif (self.connected_users[s_key]['service_type'] == 'latency'):
                self.provide_latency_service(slice_metrics, s_key)

            elif (self.connected_users[s_key]['service_type'] == 'throughput'):
                # TODO: Fix allocation
                slice_metrics[s_key]['new_num_rbgs'] = constants.MAX_RBG

            else:
                logging.error('User {} requests an unknown service! ({})'\
                                  .format(s_key, 
                                          self.connected_users[s_key]['service_type']))
                self.connected_users[s_key]['transport'].close()
                del self.connected_users[s_key]
                slice_metrics[s_key]['new_num_rbgs'] = 1

            tot_num_rbg_rqstd += slice_metrics[s_key]['new_num_rbgs']

        # Readjust number of RBGs if the total number exceeds the availability
        if tot_num_rbg_rqstd > constants.MAX_RBG:
            self.readjust_rbgs_to_capacity(slice_metrics, tot_num_rbg_rqstd)

        timestamp_ms = int(time.time() * 1000)
        logging.info('ts_ms:' + str(timestamp_ms) + ' slice_metrics:' + str(slice_metrics))

        self.write_slice_masks(slice_metrics)

    def write_slice_masks(self, slice_metrics: dict) -> None:
         # Write slice masks for each slice on file
        rbg_idx_to_start = 0
        for s_key, s_val in slice_metrics.items():
            new_mask = '0' * rbg_idx_to_start
            new_mask += '1' * s_val['new_num_rbgs']
            rbg_idx_to_start = len(new_mask)
            new_mask += '0' * (constants.MAX_RBG - rbg_idx_to_start)

            if (new_mask != s_val['cur_slice_mask']):
                # assemble config parameters dictionary and write mask
                # tenant_number needs to be there but is not used in this case
                config_params = {'network_slicing_enabled': True, 
                                    'tenant_number': 1, 
                                    'slice_allocation': new_mask}
                write_tenant_slicing_mask(config_params, True, s_key)

class CellOrderClientProtocol(asyncio.Protocol):
    # TODO: Fill in here
    pass