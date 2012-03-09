#!/usr/bin/env python

import sys, re, datetime, urllib2, traceback
from urllib import urlencode, quote_plus
from urlparse import parse_qs
from getopt import getopt
import simplejson as json
import sqlite3
import MySQLdb
import _mysql_exceptions
from MySQLdb.cursors import DictCursor
import fuzzy
import nltk

def sqlite_db_cursor(filename='tweets.db'):
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    return cur

def mysql_db_cursor(db='tweets', user='root', password=''):
    conn = MySQLdb.connect(host='localhost', user=user, passwd=password, db=db, cursorclass=DictCursor, use_unicode=True)
    conn.set_character_set('utf8')
    cur = conn.cursor()
    cur.execute('''SET NAMES utf8;''')
    cur.execute('''SET CHARACTER SET utf8;''')
    cur.execute('''SET character_set_connection=utf8;''')
    return cur

def initialise_sqlite_database(db):
    db.execute('''
CREATE TABLE IF NOT EXISTS tweets (
  id                INTEGER NOT NULL PRIMARY KEY,
  text              TEXT NOT NULL,
  terms             TEXT NOT NULL,
  from_user         TEXT NOT NULL,
  from_user_id      INTEGER NOT NULL,
  to_user           TEXT,
  to_user_id        INTEGER,
  created_at        TEXT NOT NULL,
  iso_language_code TEXT,
  clean_text        TEXT,
  parse_tree        TEXT,
  emoticon          TEXT,
  subjectivity      INTEGER,
  polarity          INTEGER)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS resources (
  url               TEXT NOT NULL UNIQUE)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_resource (
  tweet_id          INTEGER NOT NULL,
  resource          TEXT NOT NULL)
''')

def initialise_mysql_database(db):
    db.execute('''
CREATE TABLE IF NOT EXISTS tweets (
  id                BIGINT NOT NULL UNIQUE,
  text              VARCHAR(140) NOT NULL,
  terms             VARCHAR(64) NOT NULL,
  from_user         VARCHAR(16) NOT NULL,
  from_user_id      BIGINT NOT NULL,
  to_user           VARCHAR(16),
  to_user_id        BIGINT,
  created_at        VARCHAR(32) NOT NULL,
  iso_language_code CHAR(2),
  clean_text        VARCHAR(255),
  parse_tree        TEXT,
  emoticon          CHAR(2),
  subjectivity      INT,
  polarity          INT)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS resources (
  url               VARCHAR(255) NOT NULL UNIQUE)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_resource (
  tweet_id          BIGINT NOT NULL,
  resource          VARCHAR(255) NOT NULL,
  KEY mention (tweet_id, resource))
''')

def tweet_exists(db, tweet):
    sql = '''SELECT id FROM tweets WHERE id=%s LIMIT 1'''
    db.execute(sql, (tweet['id'],))
    return db.rowcount == 1

def insert_tweet(db, tweet, terms=None):
    insert_fields = ['id', 'text', 'terms', 'from_user', 'from_user_id', 'to_user', 'to_user_id', 'created_at', 'iso_language_code']
    tweet['terms'] = terms
    sql = '''INSERT INTO tweets (%s) VALUES (%s)''' % (','.join(insert_fields), ','.join(['%s' for f in insert_fields]))
    args = tuple([tweet[f] for f in insert_fields])
    db.execute(sql, args)

def retrieve_tweets(db, terms):
    search_base = 'http://search.twitter.com/search.json'
    args = {'q': quote_plus(terms),
            'page': '1',
            'rpp': '100',
            'lang': 'en'}

    while args['page'] is not None:
        try:
            resp = urllib2.urlopen(search_base + '?' + urlencode(args))
        except urllib2.HTTPError:
            traceback.print_exc()
            break
        encoding = resp.headers['content-type'].split('charset=')[-1]
        results = json.loads(unicode(resp.read(), encoding))

        for tweet in results['results']:
            print tweet['id'], '...',
            if not tweet_exists(db, tweet):
                insert_tweet(db, tweet, terms)
                print 'Done.'
            else:
                print 'Duplicate.'

        if 'next_page' not in results:
            break

        args = dict([(k, v[0]) for (k, v) in parse_qs(results['next_page'].lstrip('?')).items()])

# Regular expressions
positive_emoticon_re = re.compile(r'[:=]-?[)DpP]')
negative_emoticon_re = re.compile(r'[:=]-?[(<]')
url_re               = re.compile(r'https?://[A-Za-z0-9-_.]+\.[A-Za-z]+[A-Za-z0-9/._%?=&-]*')
at_mention_re        = re.compile(r'@[A-Za-z0-9_]{1,15}')

# Emoticons

def find_emoticon(tweet_text):
    positive_emoticon_mo = positive_emoticon_re.search(tweet_text)
    negative_emoticon_mo = negative_emoticon_re.search(tweet_text)

    if positive_emoticon_mo and not negative_emoticon_mo:
        return ':)'
    elif negative_emoticon_mo and not positive_emoticon_mo:
        return ':('
    else:
        return None

def tag_emoticons(db):
    db.execute('''SELECT id, text FROM tweets''')
    tags = []
    for tweet in db.fetchall():
        emoticon = find_emoticon(tweet['text'])
        if emoticon is not None:
            print emoticon, '->', tweet['text']
            tags.append((emoticon, tweet['id']))

    update_emoticons = '''UPDATE tweets SET emoticon=%s WHERE id=%s'''
    db.executemany(update_emoticons, tags)

def remove_emoticons(tweet_text):
    (s, positive_removed) = positive_emoticon_re.subn('', tweet_text)
    (t, negative_removed) = negative_emoticon_re.subn('', s)

    return t

# Resources/URLs

def record_resources(db):
    db.execute('''SELECT id, text FROM tweets''')
    urls = []
    mentions = []
    for tweet in db.fetchall():
        found_urls = url_re.findall(tweet['text'])
        urls.extend([(url,) for url in found_urls])
        for url in found_urls:
            mentions.append((tweet['id'], url))
    
    insert_urls = '''REPLACE INTO resources (url) VALUES (%s)'''
    db.executemany(insert_urls, urls)

    insert_mentions = '''REPLACE INTO tweet_mentions_resource (tweet_id, resource) VALUES (%s, %s)'''
    db.executemany(insert_mentions, mentions)
    
def remove_urls(tweet_text):
    (s, urls_removed) = url_re.subn('', tweet_text)

    return s

# @-mentions

def remove_at_mentions(tweet_text):
    (s, at_mentions_removed) = at_mention_re.subn('', tweet_text)

    return s

# Cleaning

def clean_tweet(tweet_text):
    return remove_at_mentions(remove_urls(remove_emoticons(tweet_text)))

def clean_tweets(db):
    db.execute('''SELECT id, text FROM tweets''')
    clean_tweets = []
    for tweet in db.fetchall():
        clean_tweets.append((clean_tweet(tweet['text']), tweet['id']))
        print '"%s" => "%s"' % (tweet['text'], clean_tweets[-1][0])

    update_tweets = '''UPDATE tweets SET clean_text=%s WHERE id=%s'''
    db.executemany(update_tweets, clean_tweets)

def main():
    # get command line options
    options, args = getopt(sys.argv[1:], 'e:d:u:p:t:')
    options = dict([(o.strip('-'), a) for (o, a) in options if a != ''])

    # connect to database
    db = None
    if not 'e' in options or options['e'] == 'mysql':
        try:
            db = mysql_db_cursor(options.get('d', 'tweets'), options.get('u', 'root'), options.get('p', ''))
            initialise_mysql_database(db)
        except _mysql_exceptions.OperationalError, e:
            print 'Database error:', e
            sys.exit(1)
    elif options['e'] == 'sqlite':
        try:
            db = sqlite_db_cursor(options.get('d', 'tweets.db'))
            initialise_sqlite_database(db)
        except sqlite3.OperationalError, e:
            print 'Database error:', e
            sys.exit(1)
    else:
        print 'You must specify a database type (-e mysql|sqlite) and database name (-d).'
        sys.exit(1)

    # retrieve some tweets
    if 't' in options:
        retrieve_tweets(db, options['t'])

    # begin NLP pipeline
    tag_emoticons(db)
    clean_tweets(db)
    record_resources(db)

if __name__ == '__main__':
    main()
