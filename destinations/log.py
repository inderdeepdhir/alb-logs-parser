import sys
import json
import csv
from destinations.DestinationHandler import DestinationHandler

STDOUT_CSV_WRITER = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)

class ConsoleDestinationHandler(DestinationHandler):
    @staticmethod
    def push(log:dict):
        print(json.dumps(log))

class CSVHandler(DestinationHandler):
    @staticmethod
    def push(log:dict):
        STDOUT_CSV_WRITER.writerow(log.values())
