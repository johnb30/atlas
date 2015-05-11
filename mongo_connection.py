import json
import requests
import datetime
from kafka import SimpleProducer, KafkaClient


def add_entry(collection, text, text_feats, title, url, date, website, lang):
    """
    Function that creates the dictionary of content to add to a MongoDB
    instance and inserts the information into an external data store.

    Parameters
    ----------

    collection : pymongo Collection.
                    Collection within MongoDB that in which results are stored.

    text : String.
            Text from a given webpage.

    text_feats : Dict.
                    Features returned by the hermes API.

    title : String.
            Title of the news story.

    url : String.
            URL of the webpage from which the content was pulled.

    date : String.
            Date pulled from the RSS feed.

    website : String.
                Nickname of the site from which the content was pulled.

    Returns
    -------

    object_id : String
    """

    to_insert = make_entry(collection, text, text_feats, title, url, date,
                           website, lang)
    object_id = collection.insert(to_insert)

    json_friendly = to_insert
    json_friendly['_id'] = str(json_friendly['_id'])
    json_friendly['date'] = str(json_friendly['date'])
    json_friendly['date_added'] = str(json_friendly['date_added'])
    json_friendly = json.dumps(json_friendly)

    # Send to Elasticsearch
    base_url = 'http://52.6.147.254:9200/api/stories/'
    url = base_url + '{}/_create'.format(json_friendly['_id'])
    print('\tSending to ES.')
    out = requests.put(url, data=json_friendly)

    if out != 201:
        print('\tError sending to ES.')
    # Send "ISIL-related" stories to XDATA
    # Keywords defined by Uncharted
    keywords = ['isil', 'eiil', 'isis', 'islamic', 'taliban', 'qaeda',
                'caliphate', 'daesh']
    if any([x in text for x in keywords]):
        print('\tSending to Kafka...')

        kafka = KafkaClient('k01.istresearch.com:9092')
        producer = SimpleProducer(kafka)
        producer.send_messages("caerus-news", json_friendly)

    return object_id


def make_entry(collection, text, text_feats, title, url, date, website, lang):
    """
    Function that creates the dictionary of content to add to an external data
    store.

    Parameters
    ----------

    text : String.
            Text from a given webpage.

    title : String.
            Title of the news story.

    url : String.
            URL of the webpage from which the content was pulled.

    date : String.
            Date pulled from the RSS feed.

    website : String.
                Nickname of the site from which the content was pulled.

    Returns
    -------

    to_inser : Dictionary.
                Dictionary of text and other content.
    """
    if lang == 'arabic':
        toInsert = {"url": url,
                    "title": title,
                    "source": website,
                    "date": date,
                    "date_added": datetime.datetime.utcnow(),
                    "content_ar": text,
                    "content_en": '',
                    "stanford": 0,
                    "geo": 0,
                    "language": lang}
    elif lang == 'english':
        if text_feats:
            # No stanford for now...
#            try:
#                trees = []
#                stanford = text_feats['stanford']['sentences']
#                full_stanford = text_feats['stanford']
#                for i in xrange(len(stanford)):
#                    trees.append(stanford[i]['parsetree'])
#                stanford_coded = 1
#            except TypeError:
#                full_stanford = {}
#                stanford_coded = 0
            trees = []
            full_stanford = {}
            stanford_coded = 0
            mitie_info = text_feats['MITIE']
            for key in mitie_info.keys():
                mitie_info[key] = json.loads(mitie_info[key])
            geo_info = text_feats['CLIFF']
            topic_info = json.loads(text_feats['topic_model'])
            good_text_feats = 1
        else:
            trees = []
            stanford_coded = 0
            mitie_info = {}
            geo_info = {}
            topic_info = {}
            full_stanford = {}
            good_text_feats = 0
        toInsert = {"url": url,
                    "title": title,
                    "source": website,
                    "date": date,
                    "date_added": datetime.datetime.utcnow(),
                    "content_en": text,
                    "content_ar": '',
                    "stanford": stanford_coded,
                    "good_text_feats": good_text_feats,
                    "mitie_info": mitie_info,
                    "geo_info": geo_info,
                    "topic_info": topic_info,
                    "full_stanford": full_stanford,
                    "parsed_sents": trees,
                    "language": lang}

    return toInsert
