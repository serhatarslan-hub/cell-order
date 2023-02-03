import asyncio
import logging
import time
import json
import numpy as np

import constants
import mcs_mapper
import cell_order
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
        self.clients = {}
        self.next_nid = 0
        self.negotiations = {}
        self.reallocation_handle = None

        self.lat_budget_offset_ms = 28 # TODO: Dynamically determine this

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

        messages = data.decode().strip()
        for msg in messages.split('\n'):
            logging.info("Received Message:{}".format(msg))
            self.loop.call_soon(lambda: self.msg_received(msg))

    def msg_received(self, msg: str) -> None:

        cell_order_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
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

    def is_feasible(self, uid: int, request_msg: dict) -> bool:

        # Read the MCS values of users to calculate required number of RBGs each
        # Read metrics database {imsi->{ts->{metric_name->val}}}
        metrics_db = read_metrics(lines_num = self.telemetry_lines_to_read)
        # Get slicing associations {slice_id->(imsi)}
        slice_users = get_slice_users(metrics_db)
        # Create slice metrics to sum metrics over slice
        slice_metrics = dict()
        for key, val in slice_users.items():
            slice_metrics[key] = {constants.NUM_SLICE_USERS_KEYWORD: len(val)}
        metric_keywords_to_ave = [constants.DL_MCS_KEYWORD, constants.DL_CQI_KEYWORD]
        for metric_keyword in metric_keywords_to_ave:
            # get metric averages {imsi->metric_mean_val}
            metric_avg = average_metric(metrics_db, metric_keyword)
            # average slice metrics into dict {slice_idx->metric_name->metric_mean_val}
            avg_slice_metrics(slice_metrics, slice_users, metric_avg, metric_keyword)

        tot_reserved_rbg = 0
        for cur_uid, client_state in self.clients.items():
            if (cur_uid == uid or client_state['active_nid'] is None):
                continue

            active_negotiation = self.negotiations[client_state['active_nid']]
            if (active_negotiation['service_type'] == 'best_effort'):
                tot_reserved_rbg += 1
                continue

            agreed_max_thp = active_negotiation['budgets'][constants.DL_THP_KEYWORD][1]
            cur_cqi = slice_metrics[cur_uid][constants.DL_CQI_KEYWORD]
            cur_mcs = slice_metrics[cur_uid][constants.DL_MCS_KEYWORD]
            if (cur_cqi >= self.config['min-acceptable-cqi']):
                tot_reserved_rbg += mcs_mapper.calculate_n_rbgs(agreed_max_thp, round(cur_mcs))

        # Calculate how many RBG would be required for the requester
        if (request_msg['service_type'] == 'best_effort'):
            n_requested_rbg = 1
        else:
            requested_max_thp = request_msg['budgets'][constants.DL_THP_KEYWORD][1]
            cur_cqi = slice_metrics[uid][constants.DL_CQI_KEYWORD]
            cur_mcs = slice_metrics[uid][constants.DL_MCS_KEYWORD]
            n_requested_rbg = mcs_mapper.calculate_n_rbgs(requested_max_thp, round(cur_mcs))

        if (cur_cqi < self.config['min-acceptable-cqi']):
            logging.info("Service is not feasible because of the " + \
                         "bad channel conditions. (CQI: {})".format(cur_cqi))
            return False
        elif (tot_reserved_rbg + n_requested_rbg > constants.MAX_RBG):
            n_available_rbg = constants.MAX_RBG - tot_reserved_rbg
            logging.info("Service is not feasible because the " + \
                         "physical resources are already booked. " + \
                         "(Requested: {}, Available: {}, MCS: {})".format(n_requested_rbg, n_available_rbg, cur_mcs))
            return False
        else:
            return  True 

    def handle_request(self, uid: int, request_msg: dict) -> None:
        
        assert request_msg['msg_type'] == 'request'

        if (self.is_feasible(uid, request_msg)):
            price = 10 # TODO: Look-up from a pricing table

            nid = self.next_nid
            self.negotiations[nid] = {
                'client_ip': request_msg['client_ip'],
                'client_port': request_msg['client_port'],
                'service_type': request_msg['service_type'],
                'sla_period': self.config['sla-period-sec'],
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

    def handle_consume(self, uid: int, consume_msg: dict) -> None:
        
        assert consume_msg['msg_type'] == 'consume'

        nid = consume_msg['nid']
        if (nid not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for consume msg: {}".format(consume_msg))
            self.send_cancel(uid, nid, price = self.negotiations[nid]['price'])
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
            self.send_cancel(uid, nid, price=consume_msg['price'])
            return

        if (self.is_feasible(uid, self.negotiations[nid])):
            self.send_supply(uid, consume_msg, nid, price=expected_payment)
        else:
            self.send_cancel(uid, nid, price=expected_payment)

    def get_avg_stats(self, uid: int, nid: int, sla_keywords: list) -> list:
        stats = self.clients[uid]['stats']

        if (not stats):
            logging.error("Average stats calculated without any measurements!")
            return [self.negotiations[nid]['budgets'][sla_keyword][1] for sla_keyword in sla_keywords]

        now = time.time()
        time_from_prev_sla = now \
                             - self.negotiations[nid]['sla_period'] \
                             - self.config['sla-grace-period-sec']             
        sla_stats = [[] for _ in range(len(sla_keywords))]
        for ts_sec, metrics in stats.items():
            if (ts_sec < time_from_prev_sla):
                continue
            for i in range(len(sla_keywords)):
                sla_stats[i].append(metrics[sla_keywords[i]])

        if (not sla_stats[0]):
            logging.error("Average stats calculated without any measurements!")
            return [self.negotiations[nid]['budgets'][sla_keyword][1] for sla_keyword in sla_keywords]

        sla_stats = [np.array(keyword_stats) for keyword_stats in sla_stats]
        outlier_percentile = self.config['outlier-percentile']
        retval = []
        for keyword_stats in sla_stats:
            outlier_filter = \
                np.logical_and(keyword_stats <= np.percentile(keyword_stats, 
                                                              100 - outlier_percentile),
                               keyword_stats >= np.percentile(keyword_stats, 
                                                              outlier_percentile))
            retval.append(np.mean(keyword_stats[outlier_filter]))
        return retval

    def evaluate_dispute(self, uid: int, nid: int, accepted_payment: float):

        service_type = self.negotiations[nid]['service_type']
        if (service_type == 'latency'):
            sla_keywords = [constants.LAT_KEYWORD, constants.DL_THP_KEYWORD]

        elif (service_type == 'throughput'):
            sla_keywords = [constants.DL_THP_KEYWORD]

        else:
            # Either best effort, or un-recognized service type
            return False, 0 # Can not be disputed, no refunds given

        avg_stats = self.get_avg_stats(uid, nid, sla_keywords) 
        logging.info("Average {}: {}".format(sla_keywords, avg_stats))

        for i in range(len(sla_keywords)):
            lower_bound = self.negotiations[nid]['budgets'][sla_keywords[i]][0]
            upper_bound = self.negotiations[nid]['budgets'][sla_keywords[i]][1]
            if (sla_keywords[i] == constants.LAT_KEYWORD):
                # TODO: Are we not okay with latency lower than the lower bound?
                lower_bound -= self.lat_budget_offset_ms
                upper_bound -= self.lat_budget_offset_ms
            elif (sla_keywords[i] == constants.DL_THP_KEYWORD):
                upper_bound = np.Inf

            if (avg_stats[i] < lower_bound or avg_stats[i] > upper_bound):
                # Rightful dispute
                # TODO: Also calculate partial refund
                partial_refund = self.negotiations[nid]['price']
                is_disputable = \
                    accepted_payment >= self.negotiations[nid]['price'] - partial_refund

                return is_disputable, self.negotiations[nid]['price'] - accepted_payment

        return False, 0 # Unlawful dispute, no refunds given

    def handle_dispute(self, uid: int, dispute_msg: dict) -> None:
        
        assert dispute_msg['msg_type'] == 'dispute'

        nid = dispute_msg['nid']
        if (nid not in self.negotiations.keys()):
            logging.error("Cell-Order Server cannot find the negotiation for dispute msg!")
            return

        negotiator = (self.negotiations[nid]['client_ip'],
                      self.negotiations[nid]['client_port'])
        client_name = (dispute_msg['client_ip'], dispute_msg['client_port'])
        if (negotiator != client_name):
            logging.error("Cell-Order Server received a dispute for a different negotiation: {}".format(negotiator))
            return

        is_disputable, refund = self.evaluate_dispute(uid, nid, dispute_msg['price'])
        if (is_disputable and self.is_feasible(uid, self.negotiations[nid])):
            # Tell the client to continue with the price they accept to pay for the next SLA period
            self.send_supply(uid, dispute_msg, nid, price=dispute_msg['price'])
        else:
            self.send_cancel(uid, nid, price=refund)

    def send_response(self, uid: int, request_msg: dict, 
                            nid: int, price: float) -> None:

        response_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
        response_msg['msg_type'] = 'response'
        response_msg['client_ip'] = request_msg['client_ip']
        response_msg['client_port'] = request_msg['client_port']
        response_msg['nid'] = nid
        response_msg['service_type'] = request_msg['service_type']
        response_msg['sla_period'] = self.config['sla-period-sec']
        response_msg['budgets'] = request_msg['budgets']
        response_msg['price'] = price
        response_str = json.dumps(response_msg) + '\n'
        self.clients[uid]['transport'].write(response_str.encode())
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

        supply_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
        supply_msg['msg_type'] = 'supply'
        supply_msg['client_ip'] = msg['client_ip']
        supply_msg['client_port'] = msg['client_port']
        supply_msg['nid'] = nid
        supply_msg['service_type'] = self.negotiations[nid]['service_type']
        supply_msg['start_time'] = msg['start_time']
        supply_msg['sla_period'] = self.negotiations[nid]['sla_period']
        supply_msg['budgets'] = self.negotiations[nid]['budgets']
        supply_msg['price'] = price
        supply_str = json.dumps(supply_msg) + '\n'
        self.clients[uid]['transport'].write(supply_str.encode())
        logging.info("Sent Message:{}".format(supply_msg))

    def send_cancel(self, uid: int, nid: int, price: float) -> None:
        
        if (not self.clients[uid]['transport'].is_closing()):
            cancel_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
            cancel_msg['msg_type'] = 'cancel'
            cancel_msg['client_ip'] = self.negotiations[nid]['client_ip']
            cancel_msg['client_port'] = self.negotiations[nid]['client_port']
            cancel_msg['nid'] = nid
            cancel_msg['service_type'] = self.negotiations[nid]['service_type']
            cancel_msg['sla_period'] = self.negotiations[nid]['sla_period']
            cancel_msg['budgets'] = self.negotiations[nid]['budgets']
            cancel_msg['price'] = price
            cancel_str = json.dumps(cancel_msg) + '\n'
            self.clients[uid]['transport'].write(cancel_str.encode())
            logging.info("Sent Message:{}".format(cancel_msg))
        else:
            logging.info("Removing client {}! Connection closed.".format(uid))

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

    def calculate_latency_metric(self, metrics_db: dict) -> None:
        """
        Calculates the latency for each imsi at each timestep for both UL/DL in msec
        """
        # {imsi->{ts->{metric_name->val}}}
        for imsi, ts_val in metrics_db.items():
            for ts, metrics in ts_val.items():
                dl_queue_size_bits = float(metrics[constants.DL_BUFFER_KEYWORD]) * 8.
                tx_rate_kbps = float(metrics[constants.DL_THP_KEYWORD]) * 1e3
                if (tx_rate_kbps > 0):
                    dl_latency = (dl_queue_size_bits / tx_rate_kbps) # in msec
                else:
                    dl_latency = 0.

                ul_queue_size_bits = float(metrics[constants.UL_BUFFER_KEYWORD]) * 8.
                rx_rate_kbps = float(metrics[constants.UL_THP_KEYWORD]) * 1e3
                if (rx_rate_kbps > 0):
                    ul_latency = (ul_queue_size_bits / rx_rate_kbps) # in msec
                else:
                    ul_latency = 0.
                
                metrics_db[imsi][ts][constants.DL_LAT_KEYWORD] = dl_latency
                metrics_db[imsi][ts][constants.UL_LAT_KEYWORD] = ul_latency
                metrics_db[imsi][ts][constants.LAT_KEYWORD] = dl_latency + ul_latency

    def reallocate_resources(self):

        now = time.time()
        for _, client_state in self.clients.items():
            if (client_state['sla_end_time'] >= now + self.config['reallocation-period-sec']):
                self.reallocation_handle = self.loop.call_later(self.config['reallocation-period-sec'], 
                                                                lambda: self.reallocate_resources())
                break
            self.reallocation_handle = None

        # Read metrics database {imsi->{ts->{metric_name->val}}}
        metrics_db = read_metrics(lines_num = self.telemetry_lines_to_read)
        # Add the latency in milliseconds into the metrics_db
        self.calculate_latency_metric(metrics_db)
        # Get slicing associations {slice_id->(imsi)}
        slice_users = get_slice_users(metrics_db)

        # Create slice metrics to sum metrics over slice
        slice_metrics = dict()
        for key, val in slice_users.items():
            slice_metrics[key] = {constants.NUM_SLICE_USERS_KEYWORD: len(val)}
        metric_keywords_to_ave = [constants.LAT_KEYWORD, 
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

            if (s_val[constants.DL_CQI_KEYWORD] < self.config['min-acceptable-cqi']):
                # It is not feasible to allocate good resources for this UE anyway
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (s_key not in self.clients):
                # The user client has not established a session yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            elif (self.clients[s_key]['sla_end_time'] < now or \
                  self.clients[s_key]['active_nid'] is None):
                # The user has not negotiated for new a service yet
                slice_metrics[s_key]['new_num_rbgs'] = 1

            else:
                nid = self.clients[s_key]['active_nid']
                service_type = self.negotiations[nid]['service_type']

                if (service_type == 'best-effort'):
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
            if (s_key in self.clients):
                self.clients[s_key]['stats'][now] = s_val

        self.write_slice_masks(slice_metrics)

    def provide_latency_service(self, slice_metrics: dict, s_key: int, nid: int) -> None:

        curr_tx_rate_budget_lo = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][0]
        curr_tx_rate_budget_hi = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][1]
        req_n_prbs = mcs_mapper.calculate_n_rbgs(curr_tx_rate_budget_hi, 
                                                 round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
        step = 1 + int(float(req_n_prbs) * self.config['reallocation-step-coeff'])

        curr_lo_delay_budget = self.negotiations[nid]['budgets'][constants.LAT_KEYWORD][0] - self.lat_budget_offset_ms
        curr_hi_delay_budget = self.negotiations[nid]['budgets'][constants.LAT_KEYWORD][1] - self.lat_budget_offset_ms

        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')

        if (slice_metrics[s_key][constants.LAT_KEYWORD] > curr_hi_delay_budget \
            or (slice_metrics[s_key][constants.DL_THP_KEYWORD] < curr_tx_rate_budget_lo \
                and slice_metrics[s_key][constants.LAT_KEYWORD] != 0.0)):
            # Allocate more resources to this slice
            double_n_prbs = mcs_mapper.calculate_n_rbgs(2 * curr_tx_rate_budget_hi, 
                                                        round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
            slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + step, double_n_prbs)
        elif slice_metrics[s_key][constants.LAT_KEYWORD] < curr_lo_delay_budget:
            # De-allocate resources from this slice
            # slice_metrics[s_key]['new_num_rbgs'] = max(min(cur_num_rbgs, req_n_prbs) - step, 1)
            slice_metrics[s_key]['new_num_rbgs'] = max(cur_num_rbgs - step, 1)
        else:
            # Try to maintain the current latency 
            slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs + 1

    def provide_throughput_service(self, slice_metrics: dict, s_key: int, nid: int) -> None:

        curr_tx_rate_budget_lo = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][0]
        curr_tx_rate_budget_hi = self.negotiations[nid]['budgets'][constants.DL_THP_KEYWORD][1]
        req_n_prbs = mcs_mapper.calculate_n_rbgs(curr_tx_rate_budget_hi, 
                                                    round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
        step = 1 + int(float(req_n_prbs) * self.config['reallocation-step-coeff'])

        cur_num_rbgs = slice_metrics[s_key]['cur_slice_mask'].count('1')

        if (slice_metrics[s_key][constants.DL_THP_KEYWORD] < curr_tx_rate_budget_lo \
            and slice_metrics[s_key][constants.LAT_KEYWORD] != 0.0):
            # Allocate more resources to this slice
            double_n_prbs = mcs_mapper.calculate_n_rbgs(2 * curr_tx_rate_budget_hi, 
                                                        round(slice_metrics[s_key][constants.DL_MCS_KEYWORD]))
            slice_metrics[s_key]['new_num_rbgs'] = min(max(cur_num_rbgs, req_n_prbs) + step, double_n_prbs)
        elif slice_metrics[s_key][constants.DL_THP_KEYWORD] > curr_tx_rate_budget_hi:
            # De-allocate resources from this slice
            slice_metrics[s_key]['new_num_rbgs'] = max(cur_num_rbgs - step, 1)
        else:
            slice_metrics[s_key]['new_num_rbgs'] = req_n_prbs

    def readjust_rbgs_to_capacity(self, slice_metrics: dict, tot_num_rbg_rqstd: int) -> None:

        logging.info('requested_rbg:{}'.format(tot_num_rbg_rqstd))

        # Isolate best effort slices
        best_effort_users = []
        for s_key, s_val in slice_metrics.items():
            nid = None
            is_passive_user = True
            if (s_key in self.clients):
                nid = self.clients[s_key]['active_nid']
                if (nid != None and self.negotiations[nid]['service_type'] !='best_effort'):
                    is_passive_user = False
            
            if (is_passive_user):
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

        next_rbg_idx_l = 0
        next_rbg_idx_r = constants.MAX_RBG 
        mask_towards_right = True
        for s_key, s_val in slice_metrics.items():
            if (mask_towards_right):
                new_mask = '0' * next_rbg_idx_l
                new_mask += '1' * s_val['new_num_rbgs']
                next_rbg_idx_l += s_val['new_num_rbgs']
                new_mask += '0' * (constants.MAX_RBG + constants.N_SAFETY_RBG - next_rbg_idx_l)
            else:
                next_rbg_idx_r -= s_val['new_num_rbgs']
                new_mask = '0' * next_rbg_idx_r
                new_mask += '1' * s_val['new_num_rbgs']
                new_mask += '0' * (constants.MAX_RBG + constants.N_SAFETY_RBG - len(new_mask))
            mask_towards_right = not mask_towards_right

            if (new_mask != s_val['cur_slice_mask']):
                # assemble config parameters dictionary and write mask
                # tenant_number needs to be there but is not used in this case
                config_params = {'network_slicing_enabled': True, 
                                    'tenant_number': 1, 
                                    'slice_allocation': new_mask}
                write_tenant_slicing_mask(config_params, True, s_key)
