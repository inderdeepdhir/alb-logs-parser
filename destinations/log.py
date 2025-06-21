import logging
import sys
import json
import csv
from destinations.DestinationHandler import DestinationHandler

STDOUT_CSV_WRITER = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)

class ConsoleDestinationHandler(DestinationHandler):
    @staticmethod
    def push(log:dict):
        if not isinstance(log, dict) or len(dict) < 1:
            logging.debug("Unexpected type=%s (or empty dict)", type(log))
            return
        print(json.dumps(log))

class CSVHandler(DestinationHandler):
    @staticmethod
    def push(log:dict):
        if not isinstance(log, dict) or len(dict) < 1:
            logging.debug("Unexpected type=%s (or empty dict)", type(log))
            return
        STDOUT_CSV_WRITER.writerow(log.values())
