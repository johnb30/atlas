from __future__ import unicode_literals
import requests
from lxml import etree
from bs4 import BeautifulSoup
from selenium import webdriver


def scrape(url, extractor, raw_html=''):
    """
    Function to request and parse a given URL. Returns only the "relevant"
    text.

    Parameters
    ----------

    url : String.
            URL to request and parse.

    extractor : Goose class instance.
                An instance of Goose that allows for parsing of content.

    Returns
    -------

    text : String.
            Parsed text from the specified website.

    meta : String.
            Parsed meta description of an article. Usually equivalent to the
            lede.
    """
    #try:
    headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.107 Safari/537.36"}


    try:
        if not raw_html:
            page = requests.get(url, headers=headers)
            html = page.content
        else:
            html = raw_html
    except Exception, e:
        return '', ''
        print('\tProblem requesting url: {}. {}'.format(url, e))

    try:
        article = extractor.extract(raw_html=html)
    except UnicodeDecodeError:
        article = extractor.extract(raw_html=html.decode('utf-8',
                                                         errors='replace'))

    try:
        text = article.cleaned_text
        meta = article.meta_description
        return text, meta
        # Generic error catching is bad
    except Exception, e:
        return '', ''
        print('\tProblem scraping URL: {}. {}.'.format(url, e))


def bnn_scrape(base_url, extractor):
    """
    Function specifically scoped to the BNN news sources. Uses PhantomJS to
    request the page, finds the "Read More" link, passes this secondary link
    to the standard `scrape()` function, and returns the text and associated
    metadata.

    Parameters
    ----------

    base_url : String.
                URL to request and parse.

    extractor : Goose class instance.
                An instance of Goose that allows for parsing of content.

    Returns
    -------

    text : String.
            Parsed text from the specified website.

    meta : String.
            Parsed meta description of an article. Usually equivalent to the
            lede.

    follow_url : String.
                    URL extracted from the original bnn source. This URL
                    contains the actual content and is one that is stored
                    in the database.
    """
    browser = webdriver.PhantomJS()
    browser.get(base_url)
    html_source = browser.page_source
    soup = BeautifulSoup(html_source)
    # Get the real link to follow
    all_links = soup.findAll("a")
    follow_url = ''
    for li in all_links:
        if li.text == "Read more":
            follow_url = li['href']
            print('\tFollow URL found: {}'.format(follow_url))
    # Get the right date
    # TODO: Implement this moar better
#    sp = soup.findAll("span")
#    for i in sp:
#        if i.find("em"):
#            info['date'] = i.em.text

    if follow_url:
        try:
            text, meta = scrape(follow_url, extractor)
        except TypeError:
            text = ''
            meta = ''
    else:
        follow_url = base_url
        print('\tScraping...')
        text, meta = scrape(base_url, extractor, html_source)

    return text, meta, follow_url
