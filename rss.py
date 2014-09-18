import pika
import json
import time
import logging
import datetime
import pattern.web
import utilities


def get_rss(address, website):
    """
    Function to parse an RSS feed and extract the relevant links.

    Parameters
    ----------

    address: String.
                Address for the RSS feed to scrape.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    results : pattern.web.Results.
                Object containing data on the parsed RSS feed. Each item
                represents a unique entry in the RSS feed and contains relevant
                information such as the URL and title of the story.

    """
    try:
        results = pattern.web.Newsfeed().search(address, count=100,
                                                cached=False, timeout=30)
        logger.debug('There are {} results from {}'.format(len(results),
                                                           website))
    except Exception, e:
        print 'There was an error. Check the log file for more information.'
        print e
        logger.warning('Problem fetching RSS feed for {}. {}'.format(address,
                                                                     e))
        results = None

    return results


def process_rss(rss_result, message_body, db_collection, message_queue):
    for result in rss_result:
        print type(message_body)
        page_url = _convert_url(result.url, message_body['website'])

        in_database = _check_mongo(page_url, db_collection)

        message_body['title'] = result.title
        message_body['date'] = result.date
        message_body['url'] = page_url

        to_send = json.dumps(message_body)
        print to_send

        if not in_database:
            message_queue.basic_publish(exchange='',
                                        routing_key='scraper_queue',
                                        body=to_send,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,))


def _convert_url(url, website):
    """
    Private function to clean a given page URL.

    Parameters
    ----------

    url: String.
            URL for the news stories to be scraped.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    page_url: String.
                Cleaned and unicode converted page URL.
    """

    if website == 'xinhua':
        page_url = url.replace('"', '')
        page_url = page_url.encode('ascii')
    elif website == 'upi':
        page_url = url.encode('ascii')
    elif website == 'zaman':
        #Find the weird thing. They tend to be ap or reuters, but generalized
        #just in case
        com = url.find('.com')
        slash = url[com + 4:].find('/')
        replaced_url = url.replace(url[com + 4:com + slash + 4], '')
        split = replaced_url.split('/')
        #This is nasty and hackish but it gets the jobs done.
        page_url = '/'.join(['/'.join(split[0:3]), 'world_' + split[-1]])
    else:
        page_url = url.encode('utf-8')

    return page_url


def _check_mongo(url, db_collection):
    """
    Private function to check if a URL appears in the database.

    Parameters
    ----------

    url: String.
            URL for the news stories to be scraped.

    db_collection: pymongo Collection.
                        Collection within MongoDB that in which results are
                        stored.

    Returns
    -------

    found: Boolean.
            Indicates whether or not a URL was found in the database.
    """

    if db_collection.find_one({"url": url}):
        found = True
    else:
        found = False

    return found


def process_whitelist(filepath):
    url_whitelist = open(filepath, 'r').readlines()
    url_whitelist = [line.replace('\n', '').split(',') for line in
                     url_whitelist if line]
    #Filtering based on list of sources from the config file
    to_scrape = {listing[0]: [listing[1], listing[3]] for listing in
                 url_whitelist if listing[2] in sources}

    return to_scrape


def main(scrape_dict, db_collection, db_auth, db_user, db_pass):
    coll = utilities.make_coll(db_collection, db_auth, db_user, db_pass)
    channel = utilities.make_queue()

    while True:
        logger.info('Starting a new scrape. {}'.format(datetime.datetime.now()))
        for website, (address, lang) in scrape_dict.iteritems():
            body = {'address': address, 'website': website, 'lang': lang}
            results = get_rss(address, website)

            if results:
                process_rss(results, body, coll, channel)
            else:
                logger.warning('No results for {}.'.format(website))
                pass
        logger.info('Finished a scrape. {}'.format(datetime.datetime.now()))
        time.sleep(2700)


if __name__ == '__main__':
    #Get the info from the config
    db_collection, whitelist_file, sources, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass = utilities.parse_config()
    #Setup the logging
    logger = logging.getLogger('scraper_log')
    if log_level == 'info':
        logger.setLevel(logging.INFO)
    elif log_level == 'warning':
        logger.setLevel(logging.WARNING)
    elif log_level == 'debug':
        logger.setLevel(logging.DEBUG)

    if log_dir:
        fh = logging.FileHandler(log_dir, 'a')
    else:
        fh = logging.FileHandler('scraping.log', 'a')
    formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('Running. Processing in 45 min intervals.')

    print 'Running. See log file for further information.'

    #Convert from CSV of URLs to a dictionary
    try:
        to_scrape = process_whitelist(whitelist_file)
    except IOError:
        print 'There was an error. Check the log file for more information.'
        logger.warning('Could not open URL whitelist file.')

    main(to_scrape, db_collection, auth_db, auth_user, auth_pass)
