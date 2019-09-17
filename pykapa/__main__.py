from pykapa.qcMessenger import qc_manager
import sys
import argparse
import time

parser = argparse.ArgumentParser(description='Run The Pykapa qcmessenger.')

parser.add_argument('-p', '--password', help='password')
parser.add_argument('-u', '--username', help='username')
parser.add_argument('-url', '--url', help='google sheet url')
parser.add_argument('-s', '--server', help='server')

if __name__ == '__main__':
    args = parser.parse_args()
    print("Version 2")
    qc_manager(google_sheet_url=args.url,
               username=args.username,
               password=args.password,
               server=args.server)
