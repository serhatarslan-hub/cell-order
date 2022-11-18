import argparse
import ast
import collections
import json
import logging
import os
import time
import asyncio

import constants
import cell_order

# get cell-order parameters from configuration file
def parse_cell_order_config_file(filename: str) -> dict:

    logging.info('Parsing ' + filename + ' configuration file')

    with open(filename, 'r') as file:
        config = json.load(file)

    dict_var_keys = ['slice-delay-budget-msec', 'slice-tx-rate-budget-Mbps']
    float_var_keys = ['reallocation-period-sec', 'sla-period-sec', 
                      'sla-grace-period-sec', 'outlier-percentile']

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


if __name__ == '__main__':

    # Define command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', type=str, required=True, help='Configuration file to parse.')
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

    loop = asyncio.get_event_loop()

    cell_order_server = cell_order.CellOrderServerProtocol(loop = loop, 
                                                           config = cell_order_config)

    coro = loop.create_server(lambda: cell_order_server, 
                              '127.0.0.1', constants.DEFAULT_CELL_ORDER_PORT)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()