atlas
=====

Distributed web scraper for political news content.

In short, the program pulls news links from RSS feeds, checks whether they've
been scraped yet, sends the URL to a worker queue, and spawns worker processes
to do the page scraping from the worker queue.

##Whats new in v2

The new version of `atlas` is based on Docker and docker-compose. Each of
the processes, the page extractor and RSS extractor, resides in its own docker container.
Through the use of `docker-compose`, all of the dependencies are installed and
linked to the scraping components. The IP information for the dependencies is
passed through commandline arguments, however, which means that the information
can be modified as needed.

**But why Docker?**

There are pros and cons to the use of Docker and `docker-compose` for the
deploy and management of `atlas`. The cons are mainly related to the fairly
rigid structure that `docker-compose` imparts on the linkages between pieces.
Additionally, there are some parts that are hardcoded in to the extractors
based on the assumption of Docker and `docker-compose`. It's possible to modify
all of these things, however, and a relatively sophisticated end user should be
able to get the pieces up and running in whatever configuration they wish. In
these scenarios the Docker information provides a decent template for getting
started. All of this is outweighed by the pros of the Docker setup, which is
mainly that deploying and managing all of the dependencies is *much* easier.
`docker-compose` also makes scaling the various pieces relatively easy.

##Use

Basic usage:

`docker-compose up -d`

`docker-compose stop`

More advanced users should read the various guides to Docker and
`docker-compose` to determine how best to setup the program for their specific
needs.
