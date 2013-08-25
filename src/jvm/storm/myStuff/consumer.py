#forked from https://github.com/measuredvoice/gogogon/blob/master/consumer.py

import os
import sys
import json
import logging
import logging.handlers
import signal
import pycurl
import optparse

def main():
  global logger
  
  parser = optparse.OptionParser()
  parser.add_option('-f', '--file', dest="use_log_file", default='/home/user/pys/datagov_consumer.log')
  options, remainder = parser.parse_args()
  log_file = options.use_log_file
    
  #formatter = logging.Formatter('%(process)d %(levelname)s %(created)d %(message)s', '%Y-%m-%d %H:%M:%S')
  formatter = logging.Formatter('%(message)s', '%Y-%m-%d %H:%M:%S')

  handler = logging.handlers.TimedRotatingFileHandler(
    log_file, 'midnight', 1, backupCount=3
  )
  handler.setFormatter(formatter)

  logger = logging.getLogger()
  logger.addHandler( handler )
  logger.setLevel(logging.DEBUG)
  
  signal.signal(signal.SIGINT, shutdown)
  
  def recv(line):
    if line.strip():
      #(globalhash, url) = get_fields(line)
      #logger.info("%s %s" % (globalhash, url))

      #(globalhash, url,country,time,city) = get_all_fields(line)
      #logger.info("%s %s %s %s %s" % (globalhash, url,country,time,city))

      #(data) = get_raw_json(line)
      #logger.info("%s %s %s %s %s" % (globalhash, url,country,time,city))
	
      logger.info("%s" % (line))#output raw json
 
  #logger.debug("starting up")
  curl = pycurl.Curl()
  #curl.setopt(pycurl.URL, "http://bitly.measuredvoice.com/usa.gov")  
  curl.setopt(pycurl.URL,"http://developer.usa.gov/1usagov");
  curl.setopt(pycurl.WRITEFUNCTION, recv)  
  curl.perform()
#end of mail

def get_fields(line):  
  data = json.loads(line)
  globalhash = data.get('g')
  url = data.get('u')
  return (globalhash, url)

def get_all_fields(line):  
  data = json.loads(line)
  globalhash = data.get('g')
  url = data.get('u')
  country = data.get('c')
  time=data.get('t')
  city=data.get('cy')
  return (globalhash, url,country,time,city)

def get_raw_json(line):
  data = json.load(line)
  return(data)#return raw json


def shutdown(*args):
  global logger

  logger.debug("shutting down")
  logger.flush()
  sys.exit()

if __name__ == '__main__':
  main()
