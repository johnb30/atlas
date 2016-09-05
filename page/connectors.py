import datetime


def add_entry(collection, text, title, url, date, website, lang):
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

    to_insert = make_entry(collection, text, title, url, date,
                           website, lang)
    object_id = collection.insert(to_insert)

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
                    "content_en": '',
                    "stanford": 0,
                    "geo": 0,
                    "language": lang}
    elif lang == 'english':
        trees = []
        stanford_coded = 0
        geo_info = {}
        topic_info = {}
        full_stanford = {}
        toInsert = {"url": url,
                    "title": title,
                    "source": website,
                    "date": date,
                    "date_added": datetime.datetime.utcnow(),
                    "content_en": text,
                    "content_ar": '',
                    "stanford": stanford_coded,
                    "geo_info": geo_info,
                    "topic_info": topic_info,
                    "full_stanford": full_stanford,
                    "parsed_sents": trees,
                    "language": lang}

    return toInsert
