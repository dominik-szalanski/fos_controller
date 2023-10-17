import asyncio
import argparse
import websockets
import time
import json
import signal
import functools
from subprocess import Popen
from asyncio.subprocess import PIPE, STDOUT  
from pprint import pp

shutdown = False

running_fos = {}

async def main(args):
    global running_fos
    uri = f"ws://{args.strategyserver}/ws/v1"
    print(f"Connection to Strategyserver ({uri})")
    async with websockets.connect(uri) as websocket:
        login_dict = {'op':'login','origin':'fos_controller_service'}
        await websocket.send(json.dumps(login_dict))
        response = await websocket.recv()
        subscribe_dict = {'op':'subscribe','channel':'fos'}
        await websocket.send(json.dumps(subscribe_dict))
        response = await websocket.recv()
        while not shutdown:
            response = await websocket.recv()
            fos_event_dict = json.loads(response)
            fos_start_message_dict = json.loads(fos_event_dict.get('message'))
            process_id = f'{args.process_id}_{fos_start_message_dict.get("monitor_order_channel_name")}'
            if process_id not in running_fos:
                running_fos[process_id] = True
                asyncio.ensure_future(spawn_fos(fos_start_message_dict, uri, process_id))
        unsubscribe_dict = {'op':'unsubscribe','channel':'fos'}
        await websocket.send(json.dumps(unsubscribe_dict))
        response = await websocket.recv()
    
async def spawn_fos(fos_args:dict, ws_address:str, process_id:str):
    global running_fos
    
    process = asyncio.create_subprocess_shell(f'bash fos_dummy.sh -r {fos_args.get("reference_instrument_symbol")} -t {fos_args.get("trading_instrument_symbol")} -s {ws_address} -c {fos_args.get("monitor_order_channel_name")} -p {process_id} --api-key {fos_args.get("api_key")} --api-secret {fos_args.get("api_secrect")}', stdin = PIPE, stdout = PIPE, stderr = STDOUT)
    
    pp(f'spawned: {process_id}')
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
                await asyncio.wait_for(websocket.recv(), timeout=60)
            except asyncio.TimeoutError:
                timeout = True
    process
    pp(f'killed: spawned: {process_id}')
    running_fos.pop(process_id, None)

def handler(signum):
    print(f'Signal handler called with signal {signum}')
    global shutdown
    shutdown = True

async def mainFunction(args):
    main_event_loop = asyncio.get_event_loop()
    main_event_loop.add_signal_handler(signal.SIGTERM, functools.partial(handler, signal.SIGTERM))
    main_event_loop.add_signal_handler(signal.SIGINT, functools.partial(handler, signal.SIGINT))
    await asyncio.create_task(main(args))


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-p", "--process_id", help="process id prefix", type=str)
    argParser.add_argument("-s", "--strategyserver", help="address for strategyserver", type=str)
    args = argParser.parse_args()
    asyncio.run(mainFunction(args))

