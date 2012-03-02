#!/usr/bin/env python

import sys, re, datetime, urllib2
from urllib import urlencode, quote_plus
from urlparse import parse_qs
import simplejson as json
import sqlite3
import MySQLdb
from MySQLdb.cursors import DictCursor
import fuzzy
import nltk

def sqlite_db_cursor(filename='tweets.db'):
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    return cur

def mysql_db_cursor(db='tweets', user='root', password='tbatst'):
    conn = MySQLdb.connect(host='localhost', user=user, passwd=password, db=db, cursorclass=DictCursor, use_unicode=True)
    conn.set_character_set('utf8')
    cur = conn.cursor()
    cur.execute('''SET NAMES utf8;''')
    cur.execute('''SET CHARACTER SET utf8;''')
    cur.execute('''SET character_set_connection=utf8;''')
    return cur

def initialise_sqlite_database(db):
    db.execute('''
CREATE TABLE tweets (
  id                INTEGER NOT NULL PRIMARY KEY,
  text              TEXT NOT NULL,
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

def initialise_mysql_database(db):
    db.execute('''DROP TABLE IF EXISTS tweets''')
    db.execute('''
CREATE TABLE tweets (
  id                BIGINT NOT NULL UNIQUE,
  text              VARCHAR(140) NOT NULL,
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

def tweet_exists(db, tweet):
    sql = '''SELECT id FROM tweets WHERE id=%s LIMIT 1'''
    db.execute(sql, (tweet['id'],))
    return db.rowcount == 1

def insert_tweet(db, tweet):
    insert_fields = ['id', 'text', 'from_user', 'from_user_id', 'to_user', 'to_user_id', 'created_at', 'iso_language_code']
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
            break
        encoding = resp.headers['content-type'].split('charset=')[-1]
        results = json.loads(unicode(resp.read(), encoding))

        for tweet in results['results']:
            print tweet['id']
            if not tweet_exists(db, tweet):
                insert_tweet(db, tweet)

        if 'next_page' not in results:
            break

        args = dict([(k, v[0]) for (k, v) in parse_qs(results['next_page'].lstrip('?')).items()])

positive_emoticon_re = re.compile(r'[:=]-?[)DpP]')
negative_emoticon_re = re.compile(r'[:=]-?[(<]')

def find_emoticon(tweet):
    positive_emoticon_mo = positive_emoticon_re.search(tweet)
    negative_emoticon_mo = negative_emoticon_re.search(tweet)

    if positive_emoticon_mo and not negative_emoticon_mo:
        return ':)'
    elif negative_emoticon_mo and not positive_emoticon_mo:
        return ':('
    else:
        return None


def main():
    #db = sqlite_db_cursor(sys.argv[1])
    db = mysql_db_cursor()
    #initialise_sqlite_database(db)
    #initialise_mysql_database(db)
    #retrieve_tweets(db, sys.argv[2])

if __name__ == '__main__':
    main()
