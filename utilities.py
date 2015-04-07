import os
import pika
import glob
import redis
from pymongo import MongoClient
from ConfigParser import ConfigParser


def make_coll(COLL, db_auth, db_user, db_pass):
    """
    Function to establish a connection to a local MonoDB instance.


    Parameters
    ----------

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
    connection = MongoClient('198.74.56.4')
    if db_auth:
        connection[db_auth].authenticate(db_user, db_pass)
    db = connection.event_scrape
    collection = db[COLL]

    return collection


def make_redis():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    return r


def make_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='scraper_queue', durable=True)

    return channel


def parse_config():
    #TODO: Put this out as a named tuple or two rather than 10 diff vars
    """Function to parse the config file."""
    config_file = glob.glob('config.ini')
    parser = ConfigParser()
    if config_file:
        print 'Found a config file in working directory'
        parser.read(config_file)
        try:
            if 'Auth' in parser.sections():
                auth_db = parser.get('Auth', 'auth_db')
                auth_user = parser.get('Auth', 'auth_user')
                auth_pass = parser.get('Auth', 'auth_pass')
            else:
                auth_db = ''
                auth_user = ''
                auth_pass = ''
            log_dir = parser.get('Logging', 'log_file')
            log_level = parser.get('Logging', 'level')
            collection = parser.get('Database', 'collection_list')
            whitelist = parser.get('URLS', 'file')
            sources = parser.get('URLS', 'sources').split(',')
            pool_size = int(parser.get('Processes', 'pool_size'))
            return collection, whitelist, sources, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass
        except Exception, e:
            print 'Problem parsing config file. {}'.format(e)
    else:
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        parser.read(config_file)
        print 'No config found. Using default.'
        try:
            if 'Auth' in parser.sections():
                auth_db = parser.get('Auth', 'auth_db')
                auth_user = parser.get('Auth', 'auth_user')
                auth_pass = parser.get('Auth', 'auth_pass')
            else:
                auth_db = ''
                auth_user = ''
                auth_pass = ''
            log_dir = parser.get('Logging', 'log_file')
            log_level = parser.get('Logging', 'level')
            collection = parser.get('Database', 'collection_list')
            whitelist = parser.get('URLS', 'file')
            sources = parser.get('URLS', 'sources').split(',')
            pool_size = int(parser.get('Processes', 'pool_size'))
            return collection, whitelist, sources, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass
        except Exception, e:
            print 'Problem parsing config file. {}'.format(e)
