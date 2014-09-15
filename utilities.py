import os
import glob
from pymongo import MongoClient
from ConfigParser import ConfigParser


def make_conn(COLL, db_auth, db_user, db_pass):
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
    connection = MongoClient()
    if db_auth:
        connection[db_auth].authenticate(db_user, db_pass)
    db = connection.event_scrape
    collection = db[COLL]

    return collection


def parse_config():
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
