import argparse
import logging
import os
import asyncio
import time

import constants
import cell_order, cell_order_server
from support_functions import kill_process_using_port


if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', type=str, required=True, 
                        help='Configuration file to parse.')
    parser.add_argument('--server-ip', type=str, required=True, 
                        help='IP address of the provider.')
    args = parser.parse_args()

    # configure logger and console output
    logging.basicConfig(level=logging.DEBUG, filename='/logs/cell-order.log', 
        filemode='a+', format='%(asctime)-15s %(levelname)-8s %(message)s')
    formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    filename = os.path.expanduser('~/radio_api/')
    filename = filename + args.config_file

    kill_process_using_port(constants.DEFAULT_CELL_ORDER_PORT)
    time.sleep(1)  # Give OS time to free up the PORT usage

    # parse heuristic config file
    cell_order_config = cell_order.parse_cell_order_config_file(filename)
    logging.info('Cell-Order configuration: ' + str(cell_order_config))

    loop = asyncio.get_event_loop()

    cell_order_server = cell_order_server.CellOrderServerProtocol(loop = loop, 
                                                                  config = cell_order_config)

    coro = loop.create_server(lambda: cell_order_server, 
                              args.server_ip, constants.DEFAULT_CELL_ORDER_PORT)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    logging.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()