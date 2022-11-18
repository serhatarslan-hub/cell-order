import asyncio
import logging
import time
import json

import constants
import mcs_mapper
from scope_start import (average_metric, avg_slice_metrics, get_metric_value,
                         get_slice_users, read_metrics, read_slice_mask,
                         read_slice_scheduling, write_slice_scheduling,
                         write_tenant_slicing_mask)

DEFAULT_CELL_ORDER_FIELDS = {
    'msg_type': None,
    'client_ip': None,
    'client_port': constants.DEFAULT_CELL_ORDER_PORT,
    'negotiation_id': None,
    'service_type': 'best_effort',
    'start_time': 0,
    'sla_period': 30,
    'budgets': {'lat_msec': [0., 5000.], 'thp_Mbps': [0., 1000.]},
    'price': None
}

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
        self.next_negotiation_id = 0
        self.negotiations = {}
        self.reallocation_handle = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        logging.info('Connection from {}'.format(peername))

        slice_id = self.get_slice_id(peername[1])
        self.connected_users[slice_id] = {'transport': transport,
                                          'active_negotiation_id': None,
                                          'sla_end_time': 0,
                                          'cancel_handle': None}

    def get_slice_id(self, client_port: int) -> int:
        
        # TODO: Determine slice ID with a more elegant way
        return client_port - constants.DEFAULT_CELL_ORDER_PORT - 3

    def data_received(self, data):

        msg = data.decode().strip()
        logging.info("Received Message: {}".format(msg))

        cell_order_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        try:
            cell_order_msg.update(json.loads(msg))
        except Exception as e:
            print("==> Internal error")
            print(e)
            # TODO: Do we really want to ignore the received message here?
            return

        slice_id = self.get_slice_id(cell_order_msg['client_port'])
        client_name = (cell_order_msg['client_ip'], cell_order_msg['client_port'])
        if (slice_id in self.connected_users.keys()):
            peername = self.connected_users[slice_id]['transport'].get_extra_info('peername')
            if (peername != client_name):
                logging.error("Cell-Order Server couldn't find the state for {}!".format(client_name))
                return

            if (cell_order_msg['msg_type'] == 'request'):
                self.handle_request(slice_id, cell_order_msg)

            elif (cell_order_msg['msg_type'] == 'consume'):
                self.handle_consume(slice_id, cell_order_msg)

            elif (cell_order_msg['msg_type'] == 'dispute'):
                self.handle_dispute(slice_id, cell_order_msg)

            else:
                logging.error("Cell-Order Server received a message of unknown type: {}".format(cell_order_msg))

        else:
            logging.error("Cell-Order Server has not established a session with the client {}!".format(client_name))

    def handle_request(self, slice_id: int, request_msg: dict) -> None:
        
        assert request_msg['msg_type'] == 'request'

        if (self.is_feasible(slice_id, request_msg)):
            price = 10 # TODO: Look-up from a pricing table

            negotiation_id = self.next_negotiation_id
            self.negotiations[negotiation_id] = {
                'client_ip': request_msg['client_ip'],
                'client_port': request_msg['client_port'],
                'service_type': request_msg['service_type'],
                'sla_period': request_msg['sla_period'],
                'budgets': request_msg['budgets'],
                'price': price
            }
            self.next_negotiation_id += 1

            if (self.connected_users[slice_id]['cancel_handle']):
                self.connected_users[slice_id]['cancel_handle'].cancel()
            self.connected_users[slice_id]['cancel_handle'] = \
                self.loop.call_later(self.config['sla-grace-period-sec'], 
                                     lambda: self.send_cancel(slice_id, negotiation_id, price=0))

        else:
            price = 9999 # Basically infinity
            negotiation_id = -1

        self.send_response(slice_id, request_msg, negotiation_id, price)

    def is_feasible(self, slice_id: int, request_msg: dict) -> bool:

        # TODO: Determine if the request is feasible
        return True

    def handle_consume(self, slice_id: int, consume_msg: dict) -> None:
        
        assert consume_msg['msg_type'] == 'consume'

        negotiation_id = consume_msg['negotiation_id']
        if (negotiation_id not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for consume msg: {}".format(consume_msg))
            return

        negotiator = (self.negotiations[negotiation_id]['client_ip'],
                      self.negotiations[negotiation_id]['client_port'])
        client_name = (consume_msg['client_ip'], consume_msg['client_port'])
        if (negotiator != client_name):
            logging.error("Cell-Order Server received a consume for a different negotiation: {}".format(negotiator))
            return

        expected_payment = self.negotiations[negotiation_id]['price']
        if (consume_msg['price'] < expected_payment):
            logging.error("Cell-Order Server received insufficient payment: " +\
                          "{} (expected: {})".format(consume_msg['price'], expected_payment))
            return

        if (self.connected_users[slice_id]['cancel_handle']):
            self.connected_users[slice_id]['cancel_handle'].cancel()

        if (self.is_feasible(slice_id, self.negotiations[negotiation_id])):
            self.send_supply(slice_id, consume_msg, negotiation_id, price=expected_payment)
        else:
            self.send_cancel(slice_id, negotiation_id, price=expected_payment)

    def is_disputable(self, slice_id: int, negotiation_id: int) -> bool:

        # TODO: Determine if the request is feasible
        return True

    def handle_dispute(self, slice_id: int, dispute_msg: dict) -> None:
        
        assert dispute_msg['msg_type'] == 'dispute'

        negotiation_id = dispute_msg['negotiation_id']
        if (negotiation_id not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for dispute msg: {}".format(dispute_msg))
            return

        negotiator = (self.negotiations[negotiation_id]['client_ip'],
                      self.negotiations[negotiation_id]['client_port'])
        client_name = (dispute_msg['client_ip'], dispute_msg['client_port'])
        if (negotiator != client_name):
            logging.error("Cell-Order Server received a dispute for a different negotiation: {}".format(negotiator))
            return

        if (self.connected_users[slice_id]['cancel_handle']):
            self.connected_users[slice_id]['cancel_handle'].cancel()

        if (self.is_disputable(slice_id, negotiation_id)):
            # Tell the client to continue without paying for the next SLA period
            self.send_supply(slice_id, dispute_msg, negotiation_id, price=0)

        else:
            # TODO: Calculate refund
            refund = 0

            self.send_cancel(slice_id, negotiation_id, price=refund)


    def send_response(self, slice_id: int, request_msg: dict, 
                            negotiation_id: int, price: float) -> None:

        response_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        response_msg['msg_type'] = 'response'
        response_msg['client_ip'] = request_msg['client_ip']
        response_msg['client_port'] = request_msg['client_port']
        response_msg['negotiation_id'] = negotiation_id
        response_msg['service_type'] = request_msg['service_type']
        response_msg['budgets'] = request_msg['budgets']
        response_msg['price'] = price
        self.connected_users[slice_id]['transport'].write(json.dumps(response_msg))
        logging.info("Sent Message: {}".format(response_msg))

    def send_supply(self, slice_id: int, msg: dict, 
                          negotiation_id: int, price: float) -> None:

        if (self.connected_users[slice_id]['active_negotiation_id'] != negotiation_id):
            # Overwrite the active negotiation id and garbage collect
            old_negotiation_id = self.connected_users[slice_id]['active_negotiation_id']
            try:
                self.negotiations.pop(old_negotiation_id)
            except:
                pass
            self.connected_users[slice_id]['active_negotiation_id'] = negotiation_id

        time_to_next_sla = self.negotiations[negotiation_id]['sla_period'] \
                            + self.config['sla-grace-period-sec']
        self.connected_users[slice_id]['sla_end_time'] = time.time() + time_to_next_sla

        if (not self.reallocation_handle):
            self.reallocation_handle = self.loop.call_soon(lambda: self.reallocate_resources())

        self.connected_users[slice_id]['cancel_handle'] = \
            self.loop.call_later(time_to_next_sla, 
                                 lambda: self.send_cancel(slice_id, negotiation_id, price=0))

        supply_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        supply_msg['msg_type'] = 'supply'
        supply_msg['client_ip'] = msg['client_ip']
        supply_msg['client_port'] = msg['client_port']
        supply_msg['negotiation_id'] = negotiation_id
        supply_msg['service_type'] = self.negotiations[negotiation_id]['service_type']
        supply_msg['start_time'] = msg['start_time']
        supply_msg['sla_period'] = self.negotiations[negotiation_id]['sla_period']
        supply_msg['budgets'] = self.negotiations[negotiation_id]['budgets']
        supply_msg['price'] = price
        self.connected_users[slice_id]['transport'].write(json.dumps(supply_msg))
        logging.info("Sent Message: {}".format(supply_msg))

    def send_cancel(self, slice_id: int, negotiation_id: int, price: float) -> None:
        
        cancel_msg = DEFAULT_CELL_ORDER_FIELDS.copy()
        cancel_msg['msg_type'] = 'cancel'
        cancel_msg['client_ip'] = self.negotiations[negotiation_id]['client_ip']
        cancel_msg['client_port'] = self.negotiations[negotiation_id]['client_port']
        cancel_msg['negotiation_id'] = negotiation_id
        cancel_msg['service_type'] = self.negotiations[negotiation_id]['service_type']
        cancel_msg['sla_period'] = self.negotiations[negotiation_id]['sla_period']
        cancel_msg['budgets'] = self.negotiations[negotiation_id]['budgets']
        cancel_msg['price'] = price
        self.connected_users[slice_id]['transport'].write(json.dumps(cancel_msg))
        logging.info("Sent Message: {}".format(cancel_msg))

        try:
            self.negotiations.pop(negotiation_id)
        except:
            pass
        self.connected_users[slice_id]['cancel_handle'] = None
        self.connected_users[slice_id]['active_negotiation_id'] = None
        self.connected_users[slice_id]['sla_end_time'] = 0

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
        for user in self.connected_users:
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

            elif (s_key not in self.connected_users):
                # The user client has not established a session yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (self.connected_users[s_key]['sla_end_time'] < now or \
                  self.connected_users[s_key]['active_negotiation_id'] is None):
                # The user has not negotiated for new a service yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            else:
                negotiation_id = self.connected_users[s_key]['active_negotiation_id']
                service_type = self.negotiations[negotiation_id]['service_type']

                if (service_type == 'best_effort'):
                    # Assign RBGs that are left after assigning others
                    # TODO: Fix allocation
                    slice_metrics[s_key]['new_num_rbgs'] = constants.MAX_RBG

                elif (service_type == 'latency'):
                    self.provide_latency_service(slice_metrics, s_key)

                elif (service_type == 'throughput'):
                    # TODO: Fix allocation
                    slice_metrics[s_key]['new_num_rbgs'] = constants.MAX_RBG

                else:
                    logging.error('User {} requests an unknown service! ({})'.format(s_key, service_type))
                    self.connected_users[s_key]['transport'].close()
                    self.connected_users.pop(s_key)
                    slice_metrics[s_key]['new_num_rbgs'] = 1

            tot_num_rbg_rqstd += slice_metrics[s_key]['new_num_rbgs']

        # Readjust number of RBGs if the total number exceeds the availability
        if tot_num_rbg_rqstd > constants.MAX_RBG:
            self.readjust_rbgs_to_capacity(slice_metrics, tot_num_rbg_rqstd)

        timestamp_ms = int(now * 1000)
        logging.info('ts_ms:' + str(timestamp_ms) + ' slice_metrics:' + str(slice_metrics))

        self.write_slice_masks(slice_metrics)

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
    # TODO: Fill in here
    pass