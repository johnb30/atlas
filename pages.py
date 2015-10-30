import re
import json
import scrape
import requests
# TODO: Setup logging
# import logging
import datetime
import utilities
import mongo_connection
from goose import Goose


def main():
    channel = utilities.make_queue()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue='scraper_queue')
    channel.start_consuming()


def callback(ch, method, properties, body):
    global coll
    body = json.loads(body)
    # TODO: This is bad
    try:
        print " [x] Received {}. {}".format(body['url'],
                                            datetime.datetime.now())
        parse_results(body, coll)
    except UnicodeEncodeError:
        pass
    print ' \tParsed URL.'

    ch.basic_ack(delivery_tag=method.delivery_tag)


def parse_results(message, db_collection):
    """
    Function to parse the links drawn from an RSS feed.

    Parameters
    ----------

    rss_results: pattern.web.Results.
                    Object containing data on the parsed RSS feed. Each item
                    represents a unique entry in the RSS feed and contains
                    relevant information such as the URL and title of the
                    story.

    website: String.
                Nickname for the RSS feed being scraped.

    db_collection: pymongo Collection.
                        Collection within MongoDB that in which results are
                        stored.
    """
    lang = message['lang']
    story_url = message['url']
    website = message['website']
    title = message['title']
    date = message['date']
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
        print('\tA BNN story.')
        text, meta, story_url = scrape.bnn_scrape(story_url, goose_extractor)
        text = text.encode('utf-8')
    else:
        text, meta = scrape.scrape(story_url, goose_extractor)
        text = text.encode('utf-8')

    if text:
        cleaned_text = _clean_text(text, website)
        # Hit the hermes API
        if lang == 'english':
            data = json.dumps({'content': text})
            headers = {'Content-Type': 'application/json'}
            url = 'http://52.6.20.198:5000/'
            print('\tGetting features. {}.'.format(datetime.datetime.now()))
            text_feats = requests.post(url, data=data, auth=('user',
                                                             'text2features'),
                                       headers=headers).json()
            print('\tDone getting features. {}.'.format(datetime.datetime.now()))
            if 'message' in text_feats.keys():
                print text_feats
                print('\tBad text features...')
                text_feats = {}

        else:
            print('\tNo text features...')
            text_feats = {}

        # TODO: Figure out where the title, URL, and date should come from
        # TODO: Might want to pull title straight from the story since the RSS
        # feed is borked sometimes.
        print('\tAdding entry...')
        entry_id = mongo_connection.add_entry(db_collection, cleaned_text,
                                              text_feats, title, story_url,
                                              date, website, lang)
        if entry_id:
            try:
                print '\tAdded entry from {} with id {}. {}.'.format(story_url,
                                                                     entry_id,
                                                                     datetime.datetime.now())
            except UnicodeDecodeError:
                print '\tAdded entry from {}. Unicode error for id'.format(story_url)
    else:
        print('\tWARNING: No text from {}'.format(story_url))


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
    db_collection, whitelist_file, sources, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass = utilities.parse_config()
    coll = utilities.make_coll(db_collection, auth_db, auth_user, auth_pass)
    main()
