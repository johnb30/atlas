import glob
import os
import pika
import redis
from pymongo import MongoClient
from ConfigParser import ConfigParser


def make_coll(coll_name, db_auth, db_user, db_pass, mongo_server_ip=None):
    """
    Function to establish a connection to a local MonoDB instance.


    Parameters
    ----------

    coll_name: String.
                Name of MongoDB collection to retrieve.

    db_auth: String.
                MongoDB database that should be used for user authentication.

    db_user: String.
                Username for MongoDB authentication.

    db_user: String.
                Password for MongoDB authentication.

    Returns
    -------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.
    """
    connection = MongoClient(mongo_server_ip)
    if db_auth:
        connection[db_auth].authenticate(db_user, db_pass)
    db = connection.event_scrape
    collection = db[coll_name]

    return collection


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
