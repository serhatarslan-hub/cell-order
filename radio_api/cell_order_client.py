import subprocess
import asyncio
import asyncssh
import nest_asyncio
import logging
import time
import json
import numpy as np

import constants
import cell_order
from support_functions import kill_process_using_port

nest_asyncio.apply() # Needed to create ssh coroutines within the client's event loop

# TODO: Find a better way to connect to the remote when starting iperf traffic
REMOTE_UNAME = 'ali'
REMOTE_PASSWORD = 'sardar'

class CellOrderClientProtocol(asyncio.Protocol):
    
    def __init__(self, loop, config, client_public_ip, client_ip, dst_ip, iperf_target_rate, iperf_udp):
        """
        Args:
            loop: the associated event loop registered to the OS
            config: the configuration to run the server with
            client_public_ip: the public IP address for the UE that is running this client
            client_ip: the IP address for the UE that is running this client
            dst_ip: the IP address for the remote server that traffic will come from
            iperf_target_rate: target bitrate in bps for iperf [KMG] (O for unlimited)
            iperf_udp: whether to use UDP traffic for iperf3
        """
        self.loop = loop

        # State required to run the client
        self.config = config
        self.client_ip = client_ip
        self.dst_ip = dst_ip
        self.client_public_ip = client_public_ip
        self.iperf_target_rate = iperf_target_rate
        self.iperf_udp = iperf_udp

        port_offset = int(client_ip.split('.')[-1])
        self.client_port = constants.DEFAULT_CELL_ORDER_PORT + port_offset
        self.iperf_port = constants.DEFAULT_IPERF_PORT + port_offset

        # TODO: Determine slice ID with a more elegant way
        self.slice_id = port_offset % constants.SLICE_NUM
        logging.info('slice_id:' + str(self.slice_id))

        self.active_nid = None
        self.sla_period = 0
        self.negotiated_budgets = {}
        self.sla_price = self.config['max-sla-price']
        self.request_handle = None
        self.request_rtx_cnt = 0
        self.consume_handle = None
        self.consume_rtx_cnt = 0

        # State to collect statistics about the client
        self.client_start_time = time.time()
        self.client_close_time = 0
        self.stats = {
            'n_sla': 0,
            'success_cnt': 0,
            'tot_payment': 0,
            'measurement_type': '',
            'measurements': []
        }

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        logging.info('Connected to {}'.format(peername))
        self.transport = transport

        self.loop.call_soon(lambda: self.wait_until_iperf_established())

    def connection_lost(self, exc):

        logging.info('The connection is closed, stopping the event loop')
        self.loop.stop()

        # Display some statistics before shutting down
        print("--------------------")
        print("Client Time: {:.2f} sec".format(time.time() - self.client_start_time))
        if (self.stats['n_sla'] == 0):
            success_rate = 0
        else:
            success_rate = self.stats['success_cnt'] / self.stats['n_sla'] * 100
        print("Success Rate: {:.2f}% ({}/{})".format(success_rate, 
                                                     self.stats['success_cnt'], 
                                                     self.stats['n_sla']))
        print("Total Payments: {}".format(self.stats['tot_payment']))
        rounded_measurements = [[round(e,2) for e in m] for m in self.stats['measurements']]
        print("Measurements {}:\n\t{}".format(self.stats['measurement_type'],
                                              rounded_measurements))
        print("--------------------")

    def stop_client(self) -> None:

        kill_process_using_port(self.iperf_port)
        self.transport.close() # Calls self.connection_lost(None)

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

        incoming_client_info = (cell_order_msg['client_ip'], cell_order_msg['client_port'])
        if (incoming_client_info != self.transport.get_extra_info('sockname')):
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

    async def run_ssh_for_iperf_from_remote(self, iperf_server_ip: str, 
                                                  iperf_server_port: int,
                                                  iperf_target_rate: str,
                                                  iperf_udp: bool,
                                                  iperf_duration: float):

        logging.info("Starting iperf traffic from the remote host ...")
        function_call = "start_iperf_client("
        function_call += "server_ip='{}', ".format(iperf_server_ip)
        function_call += "port={}, ".format(iperf_server_port)
        function_call += "iperf_target_rate='{}', ".format(iperf_target_rate)
        function_call += "iperf_udp={}, ".format(iperf_udp)
        function_call += "duration={}, ".format(iperf_duration)
        function_call += "reversed=False, loop=False, json=True)"
        program = "from support_functions import start_iperf_client; {}".format(function_call)
        cmd = 'cd /home/ali/cell-order/radio_api; python3 -c "{}"'.format(program)

        async with asyncssh.connect(self.dst_ip, username=REMOTE_UNAME, password=REMOTE_PASSWORD, known_hosts=None) as conn:
            return await conn.run(cmd, check=True)

    def budgets_match_service_type(self):

        service_type = self.config['slice-service-type'][self.slice_id]
        if (self.negotiated_budgets):
            budgets = self.negotiated_budgets
        else:
            budgets = {
                constants.LAT_KEYWORD: self.config['slice-delay-budget-msec'][self.slice_id], 
                constants.DL_THP_KEYWORD: self.config['slice-tx-rate-budget-Mbps'][self.slice_id]
            }

        if (service_type == 'latency'):
            # Any given budget works
            return True
        elif (service_type == 'best_effort' and budgets != cell_order.BUDGET_WILDCARD):
            logging.info("Best effort service can not specify requested budget! " + \
                         "Please check your configuration for budgets to request. " + \
                         "(Expected: {}, Configured {})".format(cell_order.BUDGET_WILDCARD, 
                                                                budgets))
            return False
        elif (service_type == 'throughput' \
              and budgets[constants.LAT_KEYWORD] != cell_order.LAT_BUDGET_WILDCARD):
            logging.info("Throughput service can not specify requested latency! " + \
                         "Please check your configuration for budgets to request. " + \
                         "(Expected: {}, Configured {})".format(cell_order.LAT_BUDGET_WILDCARD, 
                                                                budgets[constants.LAT_KEYWORD]))
            return False

        return True

    def wait_until_iperf_established(self):
        """
        Run in loop until UE is actually connected. Not negotiated with cell-order
        """
        if (not self.budgets_match_service_type()):
            self.stop_client()
            return
            
        ssh_loop = asyncio.new_event_loop()
        try:
            error_output = ssh_loop.run_until_complete(
                self.run_ssh_for_iperf_from_remote(self.client_public_ip, self.iperf_port,
                                                   iperf_target_rate = '', iperf_udp = False,
                                                   iperf_duration = 5)).stderr.strip()
        except (OSError, asyncssh.Error) as exc:
            logging.error('SSH connection failed: ' + str(exc))
            self.stop_client()
        finally:
            ssh_loop.close()

        if (error_output):
            logging.error(error_output)
        else:
            logging.info("... Client ready to negotiate!")

        self.client_start_time = time.time() # Negotiated traffic will start now
        if (self.config['duration-sec'] != 0):
            self.client_close_time = self.client_start_time + self.config['duration-sec']

        if (self.request_handle):
            self.request_handle.cancel()
        if (not self.loop.is_closed()):
            self.request_handle = self.loop.call_soon(lambda: self.send_request())

    def send_request(self):

        if (self.request_handle):
            self.request_handle.cancel()

        if (self.active_nid is not None):
            # Already negotiated, no need for a new request
            self.request_handle = None
            return

        self.request_rtx_cnt += 1
        if (self.request_rtx_cnt > self.config['max-rtx']):
            # Request retransmission timeout
            logging.info('Request retransmitted {} times.'.format(self.config['max-rtx']) + \
                         ' Stopping negotiations!')
            self.stop_client()
            return
        
        request_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
        request_msg['msg_type'] = 'request'
        request_msg['client_ip'] = self.transport.get_extra_info('sockname')[0]
        request_msg['client_port'] = self.transport.get_extra_info('sockname')[1]
        request_msg['nid'] = -1
        request_msg['service_type'] = self.config['slice-service-type'][self.slice_id]
        request_msg['sla_period'] = 0
        request_msg['budgets'] = {
            constants.LAT_KEYWORD: self.config['slice-delay-budget-msec'][self.slice_id], 
            constants.DL_THP_KEYWORD: self.config['slice-tx-rate-budget-Mbps'][self.slice_id]
        }
        request_msg['price'] = 0
        request_str = json.dumps(request_msg) + '\n'
        self.transport.write(request_str.encode())
        logging.info("Sent Message:{}".format(request_msg))

        self.request_handle = self.loop.call_later(self.config['sla-grace-period-sec'], 
                                                   lambda: self.send_request())

    def sla_as_requested(self, msg: dict) -> bool:

        if (msg['nid'] == -1):
            logging.info("The request has been denied by the server.")
            return False

        if (msg['service_type'] != self.config['slice-service-type'][self.slice_id]):
            logging.info("The response ({}) is not for ".format(msg['service_type']) +\
                "the requested type of service ({})!".format(self.config['slice-service-type'][self.slice_id]))
            return False

        if (msg['price'] >= self.config['max-sla-price']):
            logging.info("The price ({}) for the service ".format(msg['price']) +\
                "is higher than acceptable cost ({})!".format(self.config['max-sla-price']))
            return False

        if (msg['service_type'] == 'latency' \
            and (msg['budgets'][constants.LAT_KEYWORD] != \
                    self.config['slice-delay-budget-msec'][self.slice_id])):
            logging.info("The responded latency budget ({}) ".format(msg['budgets'][constants.LAT_KEYWORD]) +\
                "is not as requested ({})!".format(self.config['slice-delay-budget-msec'][self.slice_id]))
            return False

        if (msg['service_type'] in ['latency', 'throughput'] \
            and (msg['budgets'][constants.DL_THP_KEYWORD] != \
                    self.config['slice-tx-rate-budget-Mbps'][self.slice_id])):
            logging.info("The responded throughput budget ({}) ".format(msg['budgets'][constants.DL_THP_KEYWORD]) +\
                "is not as requested ({})!".format(self.config['slice-tx-rate-budget-Mbps'][self.slice_id]))
            return False

        return True

    def flush_state_and_restart(self, restart_delay: float) -> None:

        self.active_nid = None
        self.sla_period = 0
        self.negotiated_budgets = {}
        self.sla_price = self.config['max-sla-price']
        self.request_rtx_cnt = 0
        if (self.consume_handle):
            self.consume_handle.cancel()
        self.consume_handle = None
        self.consume_rtx_cnt = 0

        logging.info("The state for client is flushed. " + \
            "Will re-negotiate in {} sec.".format(restart_delay))
        if (self.request_handle):
            self.request_handle.cancel()
        self.request_handle = self.loop.call_later(restart_delay, 
                                                    lambda: self.send_request())

        if (restart_delay != 0):
            ssh_loop = asyncio.new_event_loop()
            try:
                error_output = ssh_loop.run_until_complete(
                    self.run_ssh_for_iperf_from_remote(self.client_public_ip, self.iperf_port,
                                                       iperf_target_rate = self.iperf_target_rate, 
                                                       iperf_udp = self.iperf_udp,
                                                       iperf_duration = restart_delay)).stderr.strip()
            except (OSError, asyncssh.Error) as exc:
                logging.error('SSH connection failed: ' + str(exc))
                self.stop_client()
            finally:
                ssh_loop.close()
            
            if (error_output):
                logging.error(error_output)
            elif (not self.loop.closed()):
                logging.info("... Client ready to re-negotiate!")

    def handle_response(self, response_msg: dict) -> None:

        assert response_msg['msg_type'] == 'response'
        
        if (not self.sla_as_requested(response_msg)):
            self.flush_state_and_restart(self.config['sla-grace-period-sec'])
            return

        self.active_nid = response_msg['nid']
        self.sla_period = response_msg['sla_period']
        self.negotiated_budgets = response_msg['budgets']
        self.sla_price = response_msg['price']
        if (self.request_handle):
            self.request_handle.cancel()
            self.request_handle = None 
        self.request_rtx_cnt = 0

        self.send_consume_or_dispute(disputed_price=0)

    def handle_supply(self, supply_msg: dict) -> None:

        assert supply_msg['msg_type'] == 'supply'

        if (self.client_close_time != 0 and time.time() > self.client_close_time):
            self.stop_client()
            return

        if (supply_msg['nid'] != self.active_nid):
            logging.error("Cell-Order Client received a supply " + \
                          "for an unknown negotiation: {} ".format(supply_msg['nid']) + \
                          "(expected: {})".format(self.active_nid))
            return

        if (supply_msg['start_time'] < time.time() - supply_msg['sla_period']):
            # This is an old supply message, ignore it
            return

        if (self.consume_handle):
            self.consume_handle.cancel()
        self.consume_handle = None
        self.consume_rtx_cnt = 0
        
        if (not self.sla_as_requested(supply_msg)):
            self.flush_state_and_restart(self.config['sla-grace-period-sec'])
            return

        self.stats['tot_payment'] += supply_msg['price'] # Agreed upon payment

        self.loop.call_soon(lambda: self.start_traffic_and_measurements())

    def handle_cancel(self, cancel_msg: dict) -> None:

        assert cancel_msg['msg_type'] == 'cancel'

        if (cancel_msg['nid'] != self.active_nid):
            logging.error("Cell-Order Client received a cancel " + \
                          "for an unknown negotiation: {} ".format(cancel_msg['nid']) + \
                          "(expected: {})".format(self.active_nid))
            return

        refund = cancel_msg['price']
        self.stats['tot_payment'] -= refund

        if (self.client_close_time != 0 and time.time() > self.client_close_time):
            self.stop_client()
            return

        restart_delay = self.config['sla-grace-period-sec'] if refund == 0 else 0
        self.flush_state_and_restart(restart_delay)

    def get_avg_stats(self, iperf_output: dict, sla_keywords: list) -> list:

        iperf_start_time_ms = iperf_output['start']['timestamp']['timesecs'] * 1000

        sla_stats = [[] for _ in range(len(sla_keywords))]
        for interval_data in iperf_output['intervals']:
            assert len(interval_data['streams']) == 1 

            stream_data = interval_data['streams'][0]
            assert stream_data['sender'], "Iperf's RTT can only be displayed if sender!"

            stream_data[constants.LAT_KEYWORD] = float(stream_data['rtt']) / 1e3
            stream_data[constants.DL_THP_KEYWORD] = float(stream_data['bits_per_second']) / 1e6

            ts_ms = int(stream_data['end'] * 1000) + iperf_start_time_ms
            logging.info('ts_ms:' + str(ts_ms) + ' stream:' + str(stream_data))

            for i in range(len(sla_keywords)):
                sla_stats[i].append(stream_data[sla_keywords[i]])

        if (not sla_stats[0]):
            logging.error("Clients stats calculated without any measurements!")
            return [self.negotiated_budgets[sla_keyword][1] for sla_keyword in sla_keywords]

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

    def get_price_to_dispute(self, iperf_output_file: str='', 
                                   iperf_output_dict: dict=None) -> float:
        """
        Evaluate the service received and compare against the negotiated SLA.
        Return 0 if the service is satisfactory.
        Args:
            iperf_output_file: Path to the file that contains measurements
        """
        service_type = self.config['slice-service-type'][self.slice_id]
        if (service_type == 'latency'):
            sla_keywords = [constants.LAT_KEYWORD, constants.DL_THP_KEYWORD]

        elif (service_type == 'throughput'):
            sla_keywords = [constants.DL_THP_KEYWORD]

        else:
            # Either best effort, or un-recognized service type
            return 0 # Can not be disputed, no refunds given
        
        if (iperf_output_file != ''):
            with open(iperf_output_file, 'r') as f:
                iperf_output = json.load(f)
                if (not iperf_output):
                    logging.info("Client couldn't reconcile measurements. Will not dispute.")
                    return 0
        elif (iperf_output_dict):
            iperf_output = iperf_output_dict
        else:
            logging.error("get_price_to_dispute() was called without any arguments. Can not dispute!")
            return 0

        avg_stats = self.get_avg_stats(iperf_output, sla_keywords)
        logging.info("Average {}: {}".format(sla_keywords, avg_stats))
        # Record stats
        self.stats['measurement_type'] = sla_keywords
        self.stats['measurements'].append(avg_stats)

        for i in range(len(sla_keywords)):
            lower_bound = self.negotiated_budgets[sla_keywords[i]][0]
            upper_bound = self.negotiated_budgets[sla_keywords[i]][1]
            if (sla_keywords[i] == constants.LAT_KEYWORD):
                lower_bound = 0 # okay for latency lower than the lower bound
            elif (sla_keywords[i] == constants.DL_THP_KEYWORD):
                upper_bound = np.Inf

            if (avg_stats[i] < lower_bound or avg_stats[i] > upper_bound):
                # TODO: Calculate partial dispute
                partial_dispute = self.sla_price
                return partial_dispute

        return 0 # No need to dispute

    def start_traffic_and_measurements(self) -> None:

        ssh_loop = asyncio.new_event_loop()
        try:
            ssh_output = ssh_loop.run_until_complete(
                self.run_ssh_for_iperf_from_remote(self.client_public_ip, self.iperf_port,
                                                   iperf_target_rate = self.iperf_target_rate, 
                                                   iperf_udp = self.iperf_udp,
                                                   iperf_duration = self.sla_period))
        except (OSError, asyncssh.Error) as exc:
            logging.error('SSH connection failed: ' + str(exc))
            self.stop_client()
        finally:
            ssh_loop.close()
        
        error_output = ssh_output.stderr.strip()
        if (error_output):
            logging.error(error_output)
            disputed_price = 0
        else:
            iperf_output_dict = json.loads(ssh_output.stdout.strip())
            disputed_price = self.get_price_to_dispute(iperf_output_dict=iperf_output_dict)

        # Record stats
        self.stats['n_sla'] += 1
        self.stats['success_cnt'] += (disputed_price == 0)

        if (not self.loop.is_closed()):
            self.send_consume_or_dispute(disputed_price)

    def send_consume_or_dispute(self, disputed_price: float) -> None:

        now = time.time()
        if (self.client_close_time != 0 and now > self.client_close_time \
            and disputed_price == 0):
            self.stop_client()
            return

        self.consume_rtx_cnt += 1
        if (self.consume_rtx_cnt > self.config['max-rtx']):
            # Consume retransmission timeout
            logging.info('Consume retransmitted {} times.'.format(self.config['max-rtx']) + \
                         ' Restarting negotiations!')
            self.flush_state_and_restart(self.config['sla-grace-period-sec'])
            return
        
        consume_msg = cell_order.DEFAULT_CELL_ORDER_FIELDS.copy()
        consume_msg['msg_type'] = 'consume' if disputed_price == 0 else 'dispute'
        consume_msg['client_ip'] = self.transport.get_extra_info('sockname')[0]
        consume_msg['client_port'] = self.transport.get_extra_info('sockname')[1]
        consume_msg['nid'] = self.active_nid
        consume_msg['service_type'] = self.config['slice-service-type'][self.slice_id]
        consume_msg['start_time'] = now
        consume_msg['sla_period'] = self.sla_period
        consume_msg['budgets'] = self.negotiated_budgets
        consume_msg['price'] = self.sla_price - disputed_price
        consume_str = json.dumps(consume_msg) + '\n'
        self.transport.write(consume_str.encode())
        logging.info("Sent Message:{}".format(consume_msg))

        if (self.consume_handle):
            self.consume_handle.cancel()
        self.consume_handle = self.loop.call_later(self.config['sla-grace-period-sec'], 
                                                   lambda: self.send_consume_or_dispute(disputed_price))