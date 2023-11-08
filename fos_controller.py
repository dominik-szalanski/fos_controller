import asyncio
import argparse
import websockets
import json
import signal
import functools
import subprocess
import time
from asyncio.subprocess import PIPE, STDOUT
from pprint import pp
import logging
from logging.handlers import RotatingFileHandler
import sys
import os

shutdown = False

running_fos = {}
binary_name = 'fos'
binary_flags = ''
gateio_future_websocket_url = 'wss://fxws-private.gateapi.io:443/v4/ws/'
ws_monitoring_timeout = 600 
connection_count = 20 
connect_delay_s = 61 

async def main(args):
    global running_fos
    global shutdown
    uri = f"ws://{args.strategyserver}/ws/v1"
    logger.log(logging.INFO,f"Connection to Strategyserver ({uri})")
    async with websockets.connect(uri) as websocket:
        login_dict = {'op':'login','origin':'fos_controller_service'}
        await websocket.send(json.dumps(login_dict))
        response = await websocket.recv()
        subscribe_dict = {'op':'subscribe','channel':'fos'}
        await websocket.send(json.dumps(subscribe_dict))
        response = await websocket.recv()
        while not shutdown:
            try:
                response = await websocket.recv()
                fos_event_dict = json.loads(response)
                fos_start_message_dict = json.loads(fos_event_dict.get('message'))
                process_id = f'{args.process_id}_{fos_start_message_dict.get("trading_instrument_symbol").upper()}_vs_{fos_start_message_dict.get("reference_instrument_symbol").upper()}'
                if process_id not in running_fos:
                    running_fos[process_id] = True
                    asyncio.ensure_future(spawn_fos(fos_start_message_dict, uri, process_id))
            except (websockets.exceptions.ConnectionClosed,websockets.exceptions.ConnectionClosedError,websockets.exceptions.ConnectionClosedOK):
                logger.log(logging.WARN,f"connection to {uri} closed")
                shutdown = True
        unsubscribe_dict = {'op':'unsubscribe','channel':'fos'}
        try: 
            await websocket.send(json.dumps(unsubscribe_dict))
            response = await websocket.recv()
        except (websockets.exceptions.ConnectionClosed,websockets.exceptions.ConnectionClosedError,websockets.exceptions.ConnectionClosedOK):
            killall_fos()
    time.sleep(10)
    killall_fos()
    
async def spawn_fos(fos_args:dict, ws_address:str, process_id:str):
    global running_fos
    fos_log_file = open(f'{process_id}.log','w')
    logger.log(logging.INFO,f"spawn new fos {process_id} binary {binary_name}")
    process = await asyncio.create_subprocess_shell(f'./{binary_name} -p {process_id} -r {fos_args.get("reference_instrument_symbol").lower()} -t {fos_args.get("trading_instrument_symbol")} -s {ws_address} -c {fos_args.get("monitor_order_channel_name")} --api-key {fos_args.get("api_key")} --api-secret {fos_args.get("api_secrect")} --connection-count {connection_count} --connect-delay-s {connect_delay_s} -exchange {fos_args.get("exchange")} --gateio-future-websocket-url {gateio_future_websocket_url}', stdin = PIPE, stdout = fos_log_file, stderr = fos_log_file)
    async with websockets.connect(ws_address) as websocket:
        login_dict = {'op':'login','origin':process_id}
        await websocket.send(json.dumps(login_dict))
        response = await websocket.recv()
        subscribe_dict = {'op':'subscribe','channel':fos_args.get("monitor_order_channel_name")}
        await websocket.send(json.dumps(subscribe_dict))
        response = await websocket.recv()
        timeout = False
        while not shutdown and not timeout:
            try:
                await asyncio.wait_for(websocket.recv(), timeout=ws_monitoring_timeout)
            except asyncio.TimeoutError:
                timeout = True
                logger.log(logging.INFO,f"no message for {ws_monitoring_timeout} received for process {process_id}")
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    logger.log(logging.INFO,f"despawn fos {process_id}")
    running_fos.pop(process_id, None)

def killall_fos():
    logger.log(logging.INFO,f"killall fos")
    subprocess.run(f'kill -15 $(pgrep {binary_name})', shell = True, executable="/bin/bash")

def soft_signal_handler(signum):
    logger.log(logging.INFO,f'soft signal handler called with signal {signum}')
    global shutdown
    shutdown = True

def hard_signal_handler(signum):
    killall_fos()
    logger.log(logging.WARN,f'hard signal handler called with signal {signum}')
    exit(1)

async def mainFunction(args):
    main_event_loop = asyncio.get_event_loop()
    main_event_loop.add_signal_handler(signal.SIGTERM, functools.partial(soft_signal_handler, signal.SIGTERM))
    main_event_loop.add_signal_handler(signal.SIGINT, functools.partial(soft_signal_handler, signal.SIGINT))
    await asyncio.create_task(main(args))


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-c", "--config", help="path to config json file", type=str)
    argParser.add_argument("-p", "--process_id", help="process id prefix", type=str)
    argParser.add_argument("-s", "--strategyserver", help="address for strategyserver", type=str)
    args = argParser.parse_args()
    if (args.config is None) and (args.process_id is None or args.strategyserver is None):
        print(f'config or mandatory arguments missing: {args}')
        exit(1)
    if args.config is not None:
        with open(args.config, 'r') as config_infile:
            config_dict = json.load(config_infile)
            if args.process_id is None:
                args.process_id = config_dict.get('process_id')
            if args.strategyserver is None:
                args.strategyserver = config_dict.get('strategyserver')
            binary_name = binary_name if config_dict.get('binary_name') is None else config_dict.get('binary_name')
            binary_flags = binary_flags if config_dict.get('binary_flags') is None else config_dict.get('binary_flags')
            ws_monitoring_timeout = ws_monitoring_timeout if config_dict.get('ws_monitoring_timeout') is None else config_dict.get('ws_monitoring_timeout')
            connection_count = connection_count if config_dict.get('connection_count') is None else config_dict.get('connection_count')
            connect_delay_s = connect_delay_s if config_dict.get('connect_delay_s') is None else config_dict.get('connect_delay_s')
            gateio_future_websocket_url = gateio_future_websocket_url if config_dict.get('gateio_future_websocket_url') is None else config_dict.get('gateio_future_websocket_url')
    logfile = 'fos_controller.log'
    loglevel = logging.INFO
    logging.basicConfig(filename=logfile,filemode='w+',level=loglevel,format='%(asctime)s %(levelname)s - %(name)s - %(message)s')
    logger = logging.getLogger()
    logger.addHandler(RotatingFileHandler(logfile, maxBytes=1024*1024*5, backupCount=1))
    
    killall_fos()
    asyncio.run(mainFunction(args))

