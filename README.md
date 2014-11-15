atlas
=====

Distributed web scraper for political news content.

##Use

Spawn a few worker processes either in a new shell or using something like
supervisor:

```
python pages.py
```

Then spawn a single process of the main script:

```
python rss.py
```

And let it rip. 

##Other Notes

If you're using supervisor, which you should be, you should write the stdout of
the worker and primary processes to log files. There's also a log file in the
`atlas` directory that picks up the logging messages that are scattered
throughtout the code, such as when a page doesn't return any results. 
