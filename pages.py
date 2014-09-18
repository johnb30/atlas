import re
import json
import scrape
#TODO: Setup logging
import logging
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
    #TODO: This is bad
    try:
        print " [x] Received {}".format(body['url'])
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

    try:
        text, meta = scrape.scrape(story_url, goose_extractor)
        text = text.encode('utf-8')
    except TypeError:
        print 'Problem obtaining text from URL: {}'.format(story_url)
        #logger.warning('Problem obtaining text from URL: {}'.format(story_url))
        text = ''

    if text:
        cleaned_text = _clean_text(text, website)

        #TODO: Figure out where the title, URL, and date should come from
        #TODO: Might want to pull title straight from the story since the RSS
        #feed is borked sometimes.
        entry_id = mongo_connection.add_entry(db_collection, cleaned_text,
                                              title, story_url, date, website,
                                              lang)
        if entry_id:
            try:
                print 'Added entry from {} with id {}'.format(story_url,
                                                              entry_id)
                #logger.info('Added entry from {} with id {}'.format(story_url,
                #                                                    entry_id))
            except UnicodeDecodeError:
                print 'Added entry from {}. Unicode error for id'.format(story_url)
                #logger.info('Added entry from {}. Unicode error for id'.format(result.url))


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
