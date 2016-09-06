import glob
import os
import pika
import redis
from ConfigParser import ConfigParser


def make_redis(host='localhost'):
    r = redis.StrictRedis(host=host, port=6379, db=0)

    return r


def make_queue(host='localhost'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host))
    channel = connection.channel()

    channel.queue_declare(queue='scraper_queue', durable=True)

    return channel


def parse_config():
    """Function to parse the config file."""
    config_file = glob.glob('config.ini')
    if config_file:
        print 'Found a config file in working directory'
    else:
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        print 'No config found. Using default.'

    config_dict = dict()
    parser = ConfigParser(allow_no_value=True)
    parser.read(config_file)
    for section in parser.sections():
        for option in parser.options(section):
            config_dict[option] = parser.get(section, option)
    # handle special case of URL 'proxies' comma delimited list
    plist = config_dict.get('proxy_list')
    config_dict['proxy_list'] = plist.split(',') if type(plist) is str else []

    return config_dict
