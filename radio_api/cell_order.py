import ast
import subprocess
import asyncio
import logging
import time
import json
import numpy as np

import constants
import mcs_mapper
from scope_start import (average_metric, avg_slice_metrics, get_metric_value,
                         get_slice_users, read_metrics, read_slice_mask,
                         read_slice_scheduling, write_slice_scheduling,
                         write_tenant_slicing_mask)
from support_functions import start_iperf_client, kill_process_using_port

DEFAULT_CELL_ORDER_FIELDS = {
    'msg_type': None,
    'client_ip': None,
    'client_port': constants.DEFAULT_CELL_ORDER_PORT,
    'nid': None,
    'service_type': 'best_effort',
    'start_time': 0,
    'sla_period': 30,
    'budgets': {
        constants.DL_LAT_KEYWORD: [0., 5000.], 
        constants.DL_THP_KEYWORD: [0., 1000.]
    },
    'price': None
}

# get cell-order parameters from configuration file
def parse_cell_order_config_file(filename: str) -> dict:

    logging.info('Parsing ' + filename + ' configuration file')

    with open(filename, 'r') as file:
        config = json.load(file)

    dict_var_keys = ['slice-delay-budget-msec', 'slice-tx-rate-budget-Mbps',
                     'slice-service-type']
    float_var_keys = ['duration-sec', 'sla-period-sec', 'sla-grace-period-sec',
                      'reallocation-period-sec', 'outlier-percentile']

    for param_key, param_val in config.items():
        # convert to right types
        if param_val.lower() in ['true', 'false']:
            config[param_key] = bool(param_val == 'True')
        elif param_key in dict_var_keys:
            # Convert some config to python dictionary
            config[param_key] = ast.literal_eval(param_val)
        elif param_key in float_var_keys:
            config[param_key] = float(param_val)

    return config

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
        self.clients = {}
        self.next_nid = 0
        self.negotiations = {}
        self.reallocation_handle = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        logging.info('Connection from {}'.format(peername))

        uid = self.get_uid(peername[1])
        self.clients[uid] = {'transport': transport,
                             'active_nid': None,
                             'sla_end_time': 0,
                             'cancel_handle': None,
                             'stats':{}}

    def get_uid(self, client_port: int) -> int:
        """
        Determine the user ID for the client which is equivalent to slice ID
        Args:
            client_port: The port number which the client uses for its traffic
        """
        
        # TODO: Determine user ID (slice ID) with a more elegant way
        return (client_port - constants.DEFAULT_CELL_ORDER_PORT) % constants.SLICE_NUM

    def data_received(self, data):

        msg = data.decode().strip()
        logging.info("Received Message:{}".format(msg))

        cell_order_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        try:
            cell_order_msg.update(json.loads(msg))
        except Exception as e:
            print("==> Internal error")
            print(e)
            return # TODO: Do we want to ignore the received message here?

        uid = self.get_uid(cell_order_msg['client_port'])
        client_name = (cell_order_msg['client_ip'], cell_order_msg['client_port'])
        if (uid in self.clients.keys()):
            peername = self.clients[uid]['transport'].get_extra_info('peername')
            if (peername != client_name):
                logging.error("Cell-Order Server couldn't find the state for {}!".format(client_name))
                return

            if (cell_order_msg['msg_type'] == 'request'):
                self.handle_request(uid, cell_order_msg)

            elif (cell_order_msg['msg_type'] == 'consume'):
                self.handle_consume(uid, cell_order_msg)

            elif (cell_order_msg['msg_type'] == 'dispute'):
                self.handle_dispute(uid, cell_order_msg)

            else:
                logging.error("Cell-Order Server received a message of unknown type: {}".format(cell_order_msg))

        else:
            logging.error("Cell-Order Server has not established a session with the client {}!".format(client_name))

    def handle_request(self, uid: int, request_msg: dict) -> None:
        
        assert request_msg['msg_type'] == 'request'

        if (self.is_feasible(uid, request_msg)):
            price = 10 # TODO: Look-up from a pricing table

            nid = self.next_nid
            self.negotiations[nid] = {
                'client_ip': request_msg['client_ip'],
                'client_port': request_msg['client_port'],
                'service_type': request_msg['service_type'],
                'sla_period': request_msg['sla_period'],
                'budgets': request_msg['budgets'],
                'price': price
            }
            self.next_nid += 1

            if (self.clients[uid]['cancel_handle']):
                self.clients[uid]['cancel_handle'].cancel()
            self.clients[uid]['cancel_handle'] = \
                self.loop.call_later(self.config['sla-grace-period-sec'], 
                                     lambda: self.send_cancel(uid, nid, price=0))

        else:
            price = 9999 # Basically infinity
            nid = -1

        self.send_response(uid, request_msg, nid, price)

    def is_feasible(self, uid: int, request_msg: dict) -> bool:

        # TODO: Determine if the request is feasible
        return True

    def handle_consume(self, uid: int, consume_msg: dict) -> None:
        
        assert consume_msg['msg_type'] == 'consume'

        nid = consume_msg['nid']
        if (nid not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for consume msg: {}".format(consume_msg))
            return

        negotiator = (self.negotiations[nid]['client_ip'],
                      self.negotiations[nid]['client_port'])
        client_name = (consume_msg['client_ip'], consume_msg['client_port'])
        if (negotiator != client_name):
            logging.error("Cell-Order Server received a consume for a different negotiation: {}".format(negotiator))
            return

        expected_payment = self.negotiations[nid]['price']
        if (consume_msg['price'] < expected_payment):
            logging.error("Cell-Order Server received insufficient payment: " +\
                          "{} (expected: {})".format(consume_msg['price'], expected_payment))
            return

        if (self.is_feasible(uid, self.negotiations[nid])):
            self.send_supply(uid, consume_msg, nid, price=expected_payment)
        else:
            self.send_cancel(uid, nid, price=expected_payment)

    def get_avg_stats(self, uid: int, nid: int, sla_keyword: str) -> float:
        stats = self.clients[uid]['stats']

        if (not stats):
            logging.error("Average stats calculated without any measurements!")
            return self.negotiations[nid]['budgets'][sla_keyword][1]

        now = time.time()
        time_from_prev_sla = now \
                             - self.negotiations[nid]['sla_period'] \
                             - self.config['sla-grace-period-sec']
        sla_stats = []
        for ts_sec, metrics in stats.items():
            if (ts_sec < time_from_prev_sla):
                continue
            sla_stats.append(metrics[sla_keyword])

        if (not sla_stats):
            logging.error("Average stats calculated without any measurements!")
            return self.negotiations[nid]['budgets'][sla_keyword][1]

        sla_stats = np.array(sla_stats)
        outlier_percentile = self.config['outlier-percentile']
        outlier_filter = np.logical_and(sla_stats <= np.percentile(sla_stats, 100 - outlier_percentile),
                                        sla_stats >= np.percentile(sla_stats, outlier_percentile))
        return np.mean(sla_stats[outlier_filter])

    def evaluate_dispute(self, uid: int, nid: int):

        service_type = self.negotiations[nid]['service_type']
        if (service_type == 'latency'):
            sla_keyword = constants.DL_LAT_KEYWORD

        elif (service_type == 'throughput'):
            sla_keyword = constants.DL_THP_KEYWORD

        else:
            # Either best effort, or un-recognized service type
            return False, 0 # Can not be disputed, no refunds given

        avg_stat = self.get_avg_stats(uid, nid, sla_keyword)
        if (avg_stat >= self.negotiations[nid]['budgets'][sla_keyword][0] and \
            avg_stat <= self.negotiations[nid]['budgets'][sla_keyword][1]):
            return False, 0 # Unlawful dispute, no refunds given

        # TODO: Calculate refund
        refund = 0

        return True, refund

    def handle_dispute(self, uid: int, dispute_msg: dict) -> None:
        
        assert dispute_msg['msg_type'] == 'dispute'

        nid = dispute_msg['nid']
        if (nid not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for dispute msg: {}".format(dispute_msg))
            return

        negotiator = (self.negotiations[nid]['client_ip'],
                      self.negotiations[nid]['client_port'])
        client_name = (dispute_msg['client_ip'], dispute_msg['client_port'])
        if (negotiator != client_name):
            logging.error("Cell-Order Server received a dispute for a different negotiation: {}".format(negotiator))
            return

        is_disputable, refund = self.evaluate_dispute(uid, nid)
        if (is_disputable):
            # Tell the client to continue without paying for the next SLA period
            self.send_supply(uid, dispute_msg, nid, price=0)
        else:
            self.send_cancel(uid, nid, price=refund)


    def send_response(self, uid: int, request_msg: dict, 
                            nid: int, price: float) -> None:

        response_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        response_msg['msg_type'] = 'response'
        response_msg['client_ip'] = request_msg['client_ip']
        response_msg['client_port'] = request_msg['client_port']
        response_msg['nid'] = nid
        response_msg['service_type'] = request_msg['service_type']
        response_msg['budgets'] = request_msg['budgets']
        response_msg['price'] = price
        self.clients[uid]['transport'].write(json.dumps(response_msg).encode())
        logging.info("Sent Message:{}".format(response_msg))

    def send_supply(self, uid: int, msg: dict, 
                          nid: int, price: float) -> None:

        if (self.clients[uid]['active_nid'] != nid):
            # Overwrite the active negotiation id and garbage collect
            old_nid = self.clients[uid]['active_nid']
            try:
                self.negotiations.pop(old_nid)
            except:
                pass
            self.clients[uid]['active_nid'] = nid

        time_to_next_sla = self.negotiations[nid]['sla_period'] \
                            + self.config['sla-grace-period-sec']
        self.clients[uid]['sla_end_time'] = time.time() + time_to_next_sla

        self.clients[uid]['stats'] = {} # Refresh stored measurements for a new sla

        if (not self.reallocation_handle):
            self.reallocation_handle = self.loop.call_soon(lambda: self.reallocate_resources())

        if (self.clients[uid]['cancel_handle']):
            self.clients[uid]['cancel_handle'].cancel()
        self.clients[uid]['cancel_handle'] = \
            self.loop.call_later(time_to_next_sla, 
                                 lambda: self.send_cancel(uid, nid, price=0))

        supply_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        supply_msg['msg_type'] = 'supply'
        supply_msg['client_ip'] = msg['client_ip']
        supply_msg['client_port'] = msg['client_port']
        supply_msg['nid'] = nid
        supply_msg['service_type'] = self.negotiations[nid]['service_type']
        supply_msg['start_time'] = msg['start_time']
        supply_msg['sla_period'] = self.negotiations[nid]['sla_period']
        supply_msg['budgets'] = self.negotiations[nid]['budgets']
        supply_msg['price'] = price
        self.clients[uid]['transport'].write(json.dumps(supply_msg).encode())
        logging.info("Sent Message:{}".format(supply_msg))

    def send_cancel(self, uid: int, nid: int, price: float) -> None:
        
        cancel_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        cancel_msg['msg_type'] = 'cancel'
        cancel_msg['client_ip'] = self.negotiations[nid]['client_ip']
        cancel_msg['client_port'] = self.negotiations[nid]['client_port']
        cancel_msg['nid'] = nid
        cancel_msg['service_type'] = self.negotiations[nid]['service_type']
        cancel_msg['sla_period'] = self.negotiations[nid]['sla_period']
        cancel_msg['budgets'] = self.negotiations[nid]['budgets']
        cancel_msg['price'] = price
        self.clients[uid]['transport'].write(json.dumps(cancel_msg).encode())
        logging.info("Sent Message:{}".format(cancel_msg))

        try:
            self.negotiations.pop(nid)
        except:
            pass

        if (self.clients[uid]['cancel_handle']):
            self.clients[uid]['cancel_handle'].cancel()
        self.clients[uid]['cancel_handle'] = None
        self.clients[uid]['active_nid'] = None
        self.clients[uid]['sla_end_time'] = 0
        self.clients[uid]['stats'] = {} 

    def calculate_dl_latency_metric(self, metrics_db: dict) -> None:
        """
        Calculates the latency for each imsi at each timestep in msec
        """

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

    def reallocate_resources(self):

        now = time.time()
        for user in self.clients:
            if (user['sla_end_time'] >= now + self.config['reallocation-period-sec']):
                self.reallocation_handle = self.loop.call_later(self.config['reallocation-period-sec'], 
                                                                lambda: self.reallocate_resources())
                break
            self.reallocation_handle = None

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

            elif (s_key not in self.clients):
                # The user client has not established a session yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (self.clients[s_key]['sla_end_time'] < now or \
                  not self.clients[s_key]['active_nid']):
                # The user has not negotiated for new a service yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            else:
                nid = self.clients[s_key]['active_nid']
                service_type = self.negotiations[nid]['service_type']

                if (service_type == 'best_effort'):
                    # Force readjust_rbgs_to_capacity() to run which 
                    # assigns RBGs that are left after assigning others
                    slice_metrics[s_key]['new_num_rbgs'] = constants.MAX_RBG

                elif (service_type == 'latency'):
                    self.provide_latency_service(slice_metrics, s_key, nid)

                elif (service_type == 'throughput'):
                    self.provide_throughput_service(slice_metrics, s_key, nid)

                else:
                    logging.error('User {} requests an unknown service! ({})'.format(s_key, service_type))
                    self.send_cancel(s_key, nid, price=0)
                    slice_metrics[s_key]['new_num_rbgs'] = 1

            tot_num_rbg_rqstd += slice_metrics[s_key]['new_num_rbgs']

        # Readjust number of RBGs if the total number exceeds the availability
        if tot_num_rbg_rqstd > constants.MAX_RBG:
            self.readjust_rbgs_to_capacity(slice_metrics, tot_num_rbg_rqstd)

        timestamp_ms = int(now * 1000)
        logging.info('ts_ms:' + str(timestamp_ms) + ' slice_metrics:' + str(slice_metrics))

        # Record metrics to decide on feasibility and handle disputes in the future
        for s_key, s_val in slice_metrics.items():
            self.clients[s_key]['stats'][now] = s_val

        self.write_slice_masks(slice_metrics)

    def provide_latency_service(self, slice_metrics: dict, s_key: int, nid: int) -> None:

        curr_tx_rate_budget_lo = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][0]
        curr_tx_rate_budget_hi = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][1]
        req_n_prbs = mcs_mapper.calculate_n_prbs(curr_tx_rate_budget_hi, 
                                                    round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))

        curr_lo_delay_budget = self.negotiations[nid]['budgets'][constants.DL_LAT_KEYWORD][0]
        curr_hi_delay_budget = self.negotiations[nid]['budgets'][constants.DL_LAT_KEYWORD][1]

        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')

        if (slice_metrics[s_key][constants.DL_LAT_KEYWORD] > curr_hi_delay_budget \
            or (slice_metrics[s_key][constants.DL_THP_KEYWORD] < curr_tx_rate_budget_lo \
                and slice_metrics[s_key][constants.DL_LAT_KEYWORD] != 0.0)):
            # Allocate more resources to this slice
            double_n_prbs = mcs_mapper.calculate_n_prbs(2 * curr_tx_rate_budget_hi, 
                                                        round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
            slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + 2, double_n_prbs)
        elif slice_metrics[s_key][constants.DL_LAT_KEYWORD] < curr_lo_delay_budget:
            # De-allocate resources from this slice
            slice_metrics[s_key]['new_num_rbgs'] = max(min(cur_num_rbgs, req_n_prbs) - 2, 1)
        else:
            # Try to maintain the current latency 
            slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

    def provide_throughput_service(self, slice_metrics: dict, s_key: int, nid: int) -> None:

        curr_tx_rate_budget_lo = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][0]
        curr_tx_rate_budget_hi = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][1]
        req_n_prbs = mcs_mapper.calculate_n_prbs(curr_tx_rate_budget_hi, 
                                                    round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))

        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')

        if (slice_metrics[s_key][constants.DL_THP_KEYWORD] < curr_tx_rate_budget_lo \
            and slice_metrics[s_key][constants.DL_LAT_KEYWORD] != 0.0):
            # Allocate more resources to this slice
            double_n_prbs = mcs_mapper.calculate_n_prbs(2 * curr_tx_rate_budget_hi, 
                                                        round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
            slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + 1, double_n_prbs)
        elif slice_metrics[s_key][constants.DL_THP_KEYWORD] > curr_tx_rate_budget_hi:
            # De-allocate resources from this slice
            slice_metrics[s_key]['new_num_rbgs'] = max(min(cur_num_rbgs, req_n_prbs) - 1, 1)
        else:
            # Try to maintain the current latency 
            slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

    def readjust_rbgs_to_capacity(self, slice_metrics: dict, tot_num_rbg_rqstd: int) -> None:

        logging.info('requested_rbg:{}'.format(tot_num_rbg_rqstd))

        # Isolate best effort slices
        best_effort_users = []
        for s_key, s_val in slice_metrics.items():
            nid = self.clients[s_key]['active_nid']
            if (not nid or self.negotiations[nid]['service_type'] =='best_effort'):
                best_effort_users.append(s_key)
                tot_num_rbg_rqstd -= slice_metrics[s_key]['new_num_rbgs']
                slice_metrics[s_key]['new_num_rbgs'] = 1
                tot_num_rbg_rqstd += 1

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

        # Distribute the remaining rbgs to best effort userrs
        while best_effort_users and tot_num_rbg_rqstd < constants.MAX_RBG:
            for s_key in best_effort_users:
                slice_metrics[s_key]['new_num_rbgs'] += 1
                tot_num_rbg_rqstd += 1
                if tot_num_rbg_rqstd >= constants.MAX_RBG:
                    break

    def write_slice_masks(self, slice_metrics: dict) -> None:
        """
        Write slice masks for each slice on file
        """

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
    
    def __init__(self, loop, config, client_ip, iperf_target_rate, iperf_udp):
        """
        Args:
            loop: the associated event loop registered to the OS
            config: the configuration to run the server with
            client_ip: the IP address for the UE that is running this client
            iperf_target_rate: target bitrate in bps for iperf [KMG] (O for unlimited)
            iperf_udp: whether to use UDP traffic for iperf3
        """
        self.loop = loop
        self.config = config
        self.client_ip = client_ip
        self.iperf_target_rate = iperf_target_rate
        self.iperf_udp = iperf_udp

        port_offset = int(client_ip.split('.')[-1])
        self.client_port = constants.DEFAULT_CELL_ORDER_PORT + port_offset
        self.iperf_port = constants.DEFAULT_IPERF_PORT + port_offset

        # TODO: Determine slice ID with a more elegant way
        self.slice_id = port_offset % constants.SLICE_NUM
        logging.info('slice_id:' + str(self.slice_id))

        self.active_nid = None
        self.request_handle = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        logging.info('Connected to {}'.format(peername))
        self.transport = transport

        self.loop.call_soon(lambda: self.wait_until_iperf_established())

    def wait_until_iperf_established(self):
        """
        Run in loop until UE is actually connected. Not negotiated with cell-order
        """
        start_iperf_client(self.client_ip, self.iperf_port, 
                           iperf_target_rate=self.iperf_target_rate, 
                           iperf_udp=self.iperf_udp,
                           reversed=False, duration=5, loop=True)

        self.start_time = time.time()
        self.request_handle = self.loop.call_soon(lambda: self.request_sla())

    def data_received(self, data):

        msg = data.decode().strip()
        logging.info("Received Message:{}".format(msg))

        cell_order_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        try:
            cell_order_msg.update(json.loads(msg))
        except Exception as e:
            print("==> Internal error")
            print(e)
            return # TODO: Do we want to ignore the received message here?

        incoming_client_info = (cell_order_msg['client_ip'], cell_order_msg['client_port'])
        if (incoming_client_info != (self.client_ip, self.client_port)):
            logging.error("Cell-Order Client received a message for a different client {}!".format(incoming_client_info))
            return

        if (cell_order_msg['msg_type'] == 'response'):
            self.handle_response(cell_order_msg)

        elif (cell_order_msg['msg_type'] == 'supply'):
            self.handle_supply(cell_order_msg)

        elif (cell_order_msg['msg_type'] == 'cancel'):
            self.handle_cancel(cell_order_msg)

        else:
            logging.error("Cell-Order Client received a message of unknown type: {}".format(cell_order_msg))


    def connection_lost(self, exc):

        logging.info('The server closed the connection, stopping the event loop')
        kill_process_using_port(self.iperf_port)
        self.loop.stop()

    def request_sla(self):

        # TODO: Send a request for an SLA and set a retransmission timeout if no negatioation occurs
        if (self.active_nid):
            # Already negotiated, no need for a new request
            self.request_handle = None
            return
        
        request_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        request_msg['msg_type'] = 'request'
        request_msg['client_ip'] = self.client_ip
        request_msg['client_port'] = self.client_port
        request_msg['nid'] = -1
        request_msg['service_type'] = self.config['slice-service-type'][self.slice_id]
        request_msg['sla_period'] = self.config['sla-period-sec']
        request_msg['budgets'] = {
            constants.DL_LAT_KEYWORD: self.config['slice-delay-budget-msec'][self.slice_id], 
            constants.DL_THP_KEYWORD: self.config['slice-tx-rate-budget-Mbps'][self.slice_id]
        }
        request_msg['price'] = 0
        self.transport.write(json.dumps(request_msg).encode())
        logging.info("Sent Message:{}".format(request_msg))

        self.request_handle = self.loop.call_later(self.config['sla-grace-period-sec'], 
                                                   lambda: self.request_sla())

    def handle_response(self, response_msg: dict) -> None:
        
        # TODO: IF response is valid, cancel request_handle, and note the active nid

        # TODO: Send consume with the negotiated terms if the price is acceptable
        pass

    def handle_supply(self, supply_msg: dict) -> None:
        
        # TODO: Check the supplied service is what is requested in the first place

        # TODO: Start the iperf traffic

        # TODO: Measure the performance and send consume/dispute accordingly
        pass

    def handle_cancel(self, cancel_msg: dict) -> None:
        
        # TODO: Flush negotiatian related state (ie. active nid)

        # TODO: If there isnt already request handle, send a new request after grace period

        # TODO: Collect refund if any
        pass