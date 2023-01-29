SCOPE_CONFIG = '../radio_code/scope_config/'
COLOSSEUM_CONFIG = '../radio_code/srsLTE/config_files/colosseum_config/'
GENERIC_CONFIG = '../radio_code/srsLTE/config_files/general_config/'
RUNNING_CONFIG = '../radio_code/srslte_config/'
SLICE_NUM = 3
MAX_RBG = 17
DEFAULT_IPERF_PORT = 5201
DEFAULT_CELL_ORDER_PORT = 9701
assert DEFAULT_IPERF_PORT % SLICE_NUM == DEFAULT_CELL_ORDER_PORT % SLICE_NUM

NUM_SLICE_USERS_KEYWORD = 'num_slice_users'
DL_BUFFER_KEYWORD = 'dl_buffer [bytes]'
UL_BUFFER_KEYWORD = 'ul_buffer [bytes]'
DL_THP_KEYWORD = 'tx_brate downlink [Mbps]'
UL_THP_KEYWORD = 'rx_brate uplink [Mbps]'
DL_LAT_KEYWORD = 'dl_latency [msec]'
UL_LAT_KEYWORD = 'ul_latency [msec]'
DL_MCS_KEYWORD = 'dl_mcs'
DL_CQI_KEYWORD = 'dl_cqi'
LAT_KEYWORD = 'rtt_wireless [msec]'