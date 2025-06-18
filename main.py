#!/usr/bin/env python3
# coding=utf8
#
# AUTHOR: shubham negi <shubham16negi@gmail.com>
# Goal: To parse alb logs and push to destination for analyzing
#

import signal
import sys
import os
import argparse
import logging
from typing import Optional
from util.LogsConsumer import LogsConsumer
from util.albparser import parse_alb_log_line
from destinations.DestinationHandler import DestinationHandler
from destinations.log import ConsoleDestinationHandler, CSVHandler

def parse_alb_log_file(filepath, handler:Optional[DestinationHandler] = ConsoleDestinationHandler):
	if handler is None:
		handler = ConsoleDestinationHandler
	with open(filepath, 'r', encoding='utf-8') as file:
		for line in file:
			handler.push(parse_alb_log_line(line.rstrip('\n')))

def parse_args(handler:Optional[DestinationHandler] = ConsoleDestinationHandler):
    parser = argparse.ArgumentParser(description=f"To parse alb logs and push to destination",prog='PROG',usage=f"{sys.argv[0]} --file file_name")
    parser.add_argument('-d','--dir', help='Logs directory Example: --dir log dir for logs')
    parser.add_argument('-f','--file', help='Log file path Example: --file file_path')
    args = parser.parse_args()
    
    if args.dir == None and args.file == None:
        sys.exit(parser.print_help())
    
    
    if (args.dir != None):
        # diretory provived, will be parsing directory and searching for logs        
        try:
            files = os.listdir(args.dir)
            for f in files:
                filepath = f"{args.dir}/{f}"
                parse_alb_log_file(filepath, handler)
        except NotADirectoryError as err:
            print(f"Invalid directory |  Error: {err}")
        except BaseException as err:
            print(f"Error: {err}")
    else:
        # file path provided will be parsing file directly        
        parse_alb_log_file(args.file, handler)

consumers=[]

def signal_handler(sig, frame):
    for c in consumers:
        c.stop()
    logging.info('Stop signal recieved')    

if __name__ == '__main__':
    #logging.basicConfig(level = logging.INFO,format='{"name":"%(name)s","level":"%(levelname)s","message":"%(message)s"}')
    logging.basicConfig(level = logging.INFO)
    if len(sys.argv) > 2:
        logging.info('Using CLI for parsing logs')
        parse_args(CSVHandler)
        sys.exit(0)    
    logging.info('Starting consumers')
    c = LogsConsumer(os.getenv('QUEUE_URL'))
    c.set_destination(ConsoleDestinationHandler)
    consumers.append(c)
    
    signal.signal(signal.SIGINT, signal_handler)        
    signal.signal(signal.SIGTERM, signal_handler)        
    
    c.start()