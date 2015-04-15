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

    to_insert = make_entry(collection, text, title, url, date, website, lang)
    print(to_insert)
    object_id = collection.insert(to_insert)

    #Send "ISIL-related" stories to XDATA
    #Keywords defined by Uncharted
    keywords = ['terror', 'attack', 'weapon', 'bomb', 'militant', 'islam',
                'isil', 'eiil', 'isis', 'islamic', 'state', 'taliban', 'qaeda',
                'jihad', 'iraq', 'syria', 'suicide', 'infidel', 'pakistan',
                'taliban', 'afghanistan', 'yemen', 'kurdish', 'caliphate']
    if any([x in text for x in keywords]):
        kafka = KafkaClient('k01.istresearch.com:9092')
        producer = SimpleProducer(kafka)
        producer.send_messages("caerus-news", to_insert)

    return object_id


def make_entry(collection, text, title, url, date, website, lang):
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
                    "stanford": 0,
                    "geo": 0,
                    "language": lang}
    elif lang == 'english':
        toInsert = {"url": url,
                    "title": title,
                    "source": website,
                    "date": date,
                    "date_added": datetime.datetime.utcnow(),
                    "content_en": text,
                    "stanford": 0,
                    "mitie_info": text_feats['MITIE'],
                    "geo_info": text_feats['CLIFF'],
                    "topic_info": text_feats['topic_model'],
                    "language": lang}

    return toInsert
