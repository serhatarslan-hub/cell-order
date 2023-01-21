import argparse
import logging
import os
import time
import asyncio

import constants
import cell_order, cell_order_client
from support_functions import kill_process_using_port


if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', type=str, required=True, 
                        help='Configuration file to parse.')
    parser.add_argument('--server-ip', type=str, required=True, 
                        help='IP address of the provider.')
    parser.add_argument('--client-ip', type=str, required=True, 
                        help='IP address of the UE.')
    parser.add_argument('--dst-ip', type=str, required=True, 
                        help='IP address that the user wants to communicate with.')
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

    cell_order_client = cell_order_client.CellOrderClientProtocol(loop, 
                                                           cell_order_config,
                                                           args.client_ip,
                                                           args.dst_ip,
                                                           args.iperf_target_rate,
                                                           args.iperf_udp)
 
    coro = loop.create_connection(lambda: cell_order_client, 
                                  args.server_ip, constants.DEFAULT_CELL_ORDER_PORT, 
                                  local_addr = (args.client_ip, client_port))
    loop.run_until_complete(coro)    

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        cell_order_client.stop_client()
    loop.close()