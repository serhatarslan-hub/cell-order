import argparse
import json
import logging
import os
import time
import asyncio

import constants
import cell_order
from support_functions import start_iperf_client, kill_process_using_port

MULTI_FLOW_ERR = "Cell-Order has not been implemented for multiple flows between the same end-points yet!"

# Get stats from the iperf output & run the logic for SLA verification
def cell_order_ue_delay_measurements(iperf_start_time_ms:int, iperf_output_file:str) -> None:

    with open(iperf_output_file,'r') as f:
        iperf_output = json.load(f)
        if (not iperf_output):
            return

    for interval_data in iperf_output['intervals']:
        assert len(interval_data['streams']) == 1, MULTI_FLOW_ERR

        stream_data = interval_data['streams'][0]
        assert stream_data['sender'], "Iperf's RTT can only be displayed if sender!"

        ts_ms = int(stream_data['end'] * 1000) + iperf_start_time_ms
        logging.info('ts_ms:' + str(ts_ms) + ' stream:' + str(stream_data))

        # rtt = stream_data['rtt']


if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', type=str, required=True, 
                        help='Configuration file to parse.')
    parser.add_argument('--server-ip', type=str, required=True, 
                        help='IP address of the provider.')
    parser.add_argument('--client-ip', type=str, required=True, 
                        help='IP address of the UE.')
    parser.add_argument('--iperf-target-rate', type=str, 
                        help='target bitrate in bps for iperf [KMG] (O for unlimited)')
    parser.add_argument('--iperf-udp', 
                        help='Use UDP traffic for iperf3', action='store_true')
    args = parser.parse_args()

    # Determine UE id
    colosseum_node_id = int(args.client_ip.split('.')[-1]) - 1 
    client_port = constants.DEFAULT_CELL_ORDER_PORT + colosseum_node_id + 1

    # configure logger and console output
    log_filename = '/logs/cell-order-ue{}.log'.format(colosseum_node_id)
    logging.basicConfig(level=logging.DEBUG, filename=log_filename, filemode='a+',
                        format='%(asctime)-15s %(levelname)-8s %(message)s')
    formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    filename = os.path.expanduser('~/radio_api/')
    filename = filename + args.config_file

    kill_process_using_port(client_port)
    time.sleep(1)  # Give OS time to free up the PORT usage

    # parse heuristic config file
    cell_order_config = cell_order.parse_cell_order_config_file(filename)
    logging.info('Cell-Order configuration: ' + str(cell_order_config))

    loop = asyncio.get_event_loop()

    cell_order_client = cell_order.CellOrderClientProtocol(loop, 
                                                           cell_order_config,
                                                           args.client_ip,
                                                           args.iperf_target_rate,
                                                           args.iperf_udp)
 
    coro = loop.create_connection(lambda: cell_order_client, 
                                  args.server_ip, constants.DEFAULT_CELL_ORDER_PORT, 
                                  local_addr = ('127.0.0.1', client_port))
    loop.run_until_complete(coro)    

    loop.run_forever()
    loop.close()         



    
    
    

    iperf_output_file = '/logs/iperf-ue{}.json'.format(colosseum_node_id)
    if (os.path.isfile(iperf_output_file)):
        # remove json file so that program reads file of current execution
        os.system('rm ' + iperf_output_file)

    start_time = time.time()
    cur_time = start_time
    while cur_time + args.sla_period - start_time <= args.t or args.t == 0:

        cur_time = time.time()

        # Run traffic for analysis
        iperf_start_time_ms = int(cur_time * 1000)
        start_iperf_client(args.ue_ip, client_port, 
                           iperf_target_rate=args.iperf_target_rate, 
                           iperf_udp=args.iperf_udp, 
                           reversed=False, duration=args.sla_period, loop=False,
                           json_filename=iperf_output_file)

        # Run the analysis itself
        cell_order_ue_delay_measurements(iperf_start_time_ms, iperf_output_file)

        if (os.path.isfile(iperf_output_file)):
            os.system('rm ' + iperf_output_file)

        