import ast
import logging
import json

import constants

LAT_BUDGET_WILDCARD = [0, 9999]
THP_BUDGET_WILDCARD = [0, 0]
BUDGET_WILDCARD = {
    constants.LAT_KEYWORD: LAT_BUDGET_WILDCARD, 
    constants.DL_THP_KEYWORD: THP_BUDGET_WILDCARD
}
DEFAULT_CELL_ORDER_FIELDS = {
    'msg_type': None,
    'client_ip': None,
    'client_port': constants.DEFAULT_CELL_ORDER_PORT,
    'nid': None,
    'service_type': 'best_effort',
    'start_time': 0,
    'sla_period': 30,
    'budgets': BUDGET_WILDCARD,
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
                      'reallocation-period-sec', 'outlier-percentile', 
                      'max-rtx', 'max-sla-price', 'min-acceptable-cqi',
                      'reallocation-step-coeff']

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
