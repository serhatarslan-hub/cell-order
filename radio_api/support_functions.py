import subprocess
import logging
import os


# run tmux command in the specified session
def run_tmux_command(cmd: str, session_name: str) -> None:

    # define tmux commands
    tmux_create_session = 'tmux new-session -d -s ' + session_name
    tmux_split_window = 'tmux split-window -h -t ' + session_name + '.right'
    tmux_spread_layout = 'tmux select-layout even-horizontal'
    tmux_run_cmd = 'tmux send -t ' + session_name + ' "' + cmd + '" ENTER'

    # start new session. This returns 'duplicate session' if session already exists
    result = subprocess.run(tmux_create_session, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # split window if session already existed before,
    if 'duplicate session' in result.stderr.decode("utf-8"):
        subprocess.run(tmux_split_window, shell=True)

    # spread out the windows evenly on the screen
    subprocess.run(tmux_spread_layout, shell=True)

    # run command in last created window
    subprocess.run(tmux_run_cmd, shell=True)

# start iperf client
def start_iperf_client(server_ip: str, port: int, tmux_session_name: str='',  
                       iperf_target_rate: str='0', iperf_udp: bool=False, 
                       reversed: bool=True, duration: int=600, loop: bool=True,
                       json: bool=False, json_filename: str='') -> None:
    
    # In order to be able to measure RTTs precisely, iperf needs to be 
    # configured to measure 1 packet worth data delivery
    iperf_block_length = 1378

    iperf_cmd = 'iperf3 -c {} -p {} -t {} --length {}'.format(server_ip, port, duration, iperf_block_length)

    if (reversed):
        iperf_cmd += ' -R'

    if (iperf_target_rate != '0'):
        iperf_cmd += ' -b ' + iperf_target_rate

    if (iperf_udp):
        iperf_cmd += ' -u'

    if (json_filename != ''):
        iperf_cmd += ' --json --logfile ' + json_filename
    elif (json):
        iperf_cmd += ' --json'

    iperf_cmd += ' --get-server-output'

    if (loop):
        # wrap command in while loop to repeat it if it fails to start
        # (e.g., if ue is not yet connected to the bs)
        iperf_cmd = 'while ! %s; do sleep 1; done' % (iperf_cmd)

    logging.info('Starting iperf3 client: ' + iperf_cmd)

    if (tmux_session_name):
        run_tmux_command(iperf_cmd, tmux_session_name)
    else:
        os.system(iperf_cmd)


# start iperf server
def start_iperf_server(port: int, tmux_session_name: str='',
                       run_as_deamon: bool=True) -> None:

    iperf_cmd = 'iperf3 -s -p ' + str(port)
    
    if (run_as_deamon):
        iperf_cmd += ' -D'
        logging.info('Starting iperf3 server in background on port ' + str(port))
        os.system(iperf_cmd)
    elif (tmux_session_name):
        logging.info('Starting iperf3 server on port ' + str(port))
        run_tmux_command(iperf_cmd, tmux_session_name)
    else:
        logging.error('No tmux session name provded, exiting. ' + \
        'If you want to run iperf server on the bakground, ' + \
        'set the run_as_deamon parameter to True. ')
        exit(1)


def kill_process_using_port(port):
    pids = subprocess.run(
        ['lsof', '-t', '-i:{}'.format(port)], 
        universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).stdout.strip().split('\n')
    for pid in pids:
        if (not pid):
            continue
        if (subprocess.run(['kill', '-TERM', pid]).returncode != 0):
            subprocess.run(['kill', '-KILL', pid], check=True)
            logging.info('Process {} killed to free port {}'.format(pid, port))