import re
import json
import time
import random
import scrape
import logging
import datetime
import argparse
import requests
import utilities
import connectors
from goose import Goose


def main(args):
    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                        level=logging.INFO)

    channel = utilities.make_queue(args.rabbit_conn)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue='scraper_queue')
    channel.start_consuming()


def callback(ch, method, properties, body):
    global coll
    body = json.loads(body)
    # TODO: This is bad
    try:
        logging.info("Received {}. {}".format(body.get('url'),
                                              datetime.datetime.now()))
        parse_results(body, coll)
    except UnicodeEncodeError:
        pass

    ch.basic_ack(delivery_tag=method.delivery_tag)


def parse_results(message, db_collection):
    """
    Function to parse the links drawn from an RSS feed.

    Parameters
    ----------

    message: pattern.web.Results.
                Object containing data on the parsed RSS feed. Each item
                represents a unique entry in the RSS feed and contains
                relevant information such as the URL and title of the
                story.

    db_collection: pymongo Collection.
                        Collection within MongoDB that in which results are
                        stored.
    """
    global proxies, proxy_user, proxy_pass

    if proxies:
        proxy_choice = {'http': random.choice(proxies)}
        proxy_login = requests.auth.HTTPProxyAuth(proxy_user,
                                                  proxy_pass)
    else:
        proxy_choice = ''
        proxy_login = {}

    lang = message.get('lang')
    story_url = message.get('url')
    website = message.get('website')
    title = message.get('title')
    date = message.get('date')
    if lang == 'english':
        goose_extractor = Goose({'use_meta_language': False,
                                 'target_language': 'en',
                                 'enable_image_fetching': False})
    elif lang == 'arabic':
        from goose.text import StopWordsArabic
        goose_extractor = Goose({'stopwords_class': StopWordsArabic,
                                 'enable_image_fetching': False})
    else:
        print(lang)

    if 'bnn_' in website:
        # story_url gets clobbered here because it's being replaced by
        # the URL extracted from the bnn content.
        #TODO: Deprecate this for now since using GhostJS is weird.
        logging.info('A BNN story.')
#        text, meta, story_url = scrape.bnn_scrape(story_url, goose_extractor)
        text = ''
        pass
    else:
        text, meta = scrape.scrape(story_url, goose_extractor, proxy_choice,
                                   proxy_login)
    text = text.encode('utf-8')

    if text:
        cleaned_text = _clean_text(text, website)

        # TODO: Figure out where the title, URL, and date should come from
        # TODO: Might want to pull title straight from the story since the RSS
        # feed is borked sometimes.
        entry_id = connectors.add_entry(db_collection, cleaned_text, title,
                                        story_url, date, website, lang)
        if entry_id:
            try:
                logging.info('Added entry from {} with id {}. {}.'.format(story_url,
                                                                          entry_id,
                                                                          datetime.datetime.now()))
            except UnicodeDecodeError:
                logging.info('Added entry from {}. Unicode error for id'.format(story_url))
    else:
        logging.warning('No text from {}'.format(story_url))


def _clean_text(text, website):
    """
    Private function to clean some of the cruft from the content pulled from
    various sources.

    Parameters
    ----------

    text: String.
            Dirty text.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    text: String.
            Less dirty text.
    """
    site_list = ['menafn_algeria', 'menafn_bahrain', 'menafn_egypt',
                 'menafn_iraq', 'menafn_jordan', 'menafn_kuwait',
                 'menafn_lebanon', 'menafn_morocco', 'menafn_oman',
                 'menafn_palestine', 'menafn_qatar', 'menafn_saudi',
                 'menafn_syria', 'menafn_tunisia', 'menafn_turkey',
                 'menafn_uae', 'menafn_yemen']

    if website == 'bbc':
        text = text.replace("This page is best viewed in an up-to-date web browser with style sheets (CSS) enabled. While you will be able to view the content of this page in your current browser, you will not be able to get the full visual experience. Please consider upgrading your browser software or enabling style sheets (CSS) if you are able to do so.", '')
    if website == 'almonitor':
        text = re.sub("^.*?\(photo by REUTERS.*?\)", "", text)
    if website in site_list:
        text = re.sub("^\(.*?MENAFN.*?\)", "", text)
    elif website == 'upi':
        text = text.replace("Since 1907, United Press International (UPI) has been a leading provider of critical information to media outlets, businesses, governments and researchers worldwide. UPI is a global operation with offices in Beirut, Hong Kong, London, Santiago, Seoul and Tokyo. Our headquarters is located in downtown Washington, DC, surrounded by major international policy-making governmental and non-governmental organizations. UPI licenses content directly to print outlets, online media and institutions of all types. In addition, UPI's distribution partners provide our content to thousands of businesses, policy groups and academic institutions worldwide. Our audience consists of millions of decision-makers who depend on UPI's insightful and analytical stories to make better business or policy decisions. In the year of our 107th anniversary, our company strives to continue being a leading and trusted source for news, analysis and insight for readers around the world.", '')

    text = text.replace('\n', ' ')

    return text


if __name__ == '__main__':
    time.sleep(60)

    aparse = argparse.ArgumentParser(prog='rss')
    aparse.add_argument('-rb', '--rabbit_conn', default='localhost')
    aparse.add_argument('-db', '--db_conn', default='127.0.0.1')
    args = aparse.parse_args()

    config_dict = utilities.parse_config()
    coll = utilities.make_coll(config_dict.get('auth_db'),
                               config_dict.get('auth_user'),
                               config_dict.get('auth_pass'),
                               args.db_conn)
    proxies = config_dict.get('proxy_list')
    proxy_pass = config_dict.get('proxy_pass')
    proxy_user = config_dict.get('proxy_user')

    main(args)
