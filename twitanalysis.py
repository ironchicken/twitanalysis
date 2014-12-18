#!/usr/bin/env python

import sys, re, urllib2, traceback
from urllib import urlencode, quote_plus
from urlparse import parse_qs
from getopt import getopt
import sqlite3
import MySQLdb
import _mysql_exceptions
from MySQLdb.cursors import DictCursor
import requests
from requests_oauthlib import OAuth1

REQUEST_TOKEN_URL = "https://api.twitter.com/oauth/request_token"
AUTHORIZE_URL = "https://api.twitter.com/oauth/authorize?oauth_token="
ACCESS_TOKEN_URL = "https://api.twitter.com/oauth/access_token"

CONSUMER_KEY = None
CONSUMER_SECRET = None

OAUTH_TOKEN = None
OAUTH_TOKEN_SECRET = None

def setup_oauth():
    """Authorize your app via identifier."""
    # Request token
    oauth = OAuth1(CONSUMER_KEY, client_secret=CONSUMER_SECRET)
    r = requests.post(url=REQUEST_TOKEN_URL, auth=oauth)
    credentials = parse_qs(r.content)

    resource_owner_key = credentials.get('oauth_token')[0]
    resource_owner_secret = credentials.get('oauth_token_secret')[0]

    # Authorize
    authorize_url = AUTHORIZE_URL + resource_owner_key
    print 'Please go here and authorize: ' + authorize_url

    verifier = raw_input('Please input the verifier: ')
    oauth = OAuth1(CONSUMER_KEY,
                   client_secret=CONSUMER_SECRET,
                   resource_owner_key=resource_owner_key,
                   resource_owner_secret=resource_owner_secret,
                   verifier=verifier)

    # Finally, Obtain the Access Token
    r = requests.post(url=ACCESS_TOKEN_URL, auth=oauth)
    credentials = parse_qs(r.content)
    token = credentials.get('oauth_token')[0]
    secret = credentials.get('oauth_token_secret')[0]

    return token, secret

def get_oauth():
    oauth = OAuth1(CONSUMER_KEY,
                   client_secret=CONSUMER_SECRET,
                   resource_owner_key=OAUTH_TOKEN,
                   resource_owner_secret=OAUTH_TOKEN_SECRET)
    return oauth

DB_PARAM_PLACEHOLDER = None

def get_placeholder(paramstyle):
    if paramstyle == 'qmark': return '?'
    elif paramstyle == 'format': return '%s'
    else:
        return None

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def sqlite_db_cursor(filename='tweets.db'):
    conn = sqlite3.connect(filename, isolation_level=None)
    conn.row_factory = dict_factory
    cur = conn.cursor()
    global DB_PARAM_PLACEHOLDER
    DB_PARAM_PLACEHOLDER = get_placeholder(sqlite3.paramstyle)
    return (conn, cur)

def mysql_db_cursor(db='tweets', user='root', password=''):
    conn = MySQLdb.connect(host='localhost', user=user, passwd=password, db=db, cursorclass=DictCursor, use_unicode=True)
    conn.set_character_set('utf8')
    cur = conn.cursor()
    cur.execute('''SET NAMES utf8;''')
    cur.execute('''SET CHARACTER SET utf8;''')
    cur.execute('''SET character_set_connection=utf8;''')
    global DB_PARAM_PLACEHOLDER
    DB_PARAM_PLACEHOLDER = get_placeholder(MySQLdb.paramstyle)
    return (conn, cur)

def initialise_sqlite_database(db):
    db.execute('''
CREATE TABLE IF NOT EXISTS tweets (
  id                INTEGER NOT NULL PRIMARY KEY,
  text              TEXT NOT NULL,
  terms             TEXT NOT NULL,
  from_user         TEXT NOT NULL,
  from_user_id      INTEGER NOT NULL,
  in_reply_to_status INTEGER,
  in_reply_to_user  INTEGER,
  retweeted         INTEGER NOT NULL DEFAULT 0,
  retweet_count     INTEGER NOT NULL DEFAULT 0,
  created_at        TEXT NOT NULL,
  iso_language_code TEXT,
  geo               TEXT,
  clean_text        TEXT,
  parse_tree        TEXT,
  emoticon          TEXT,
  subjectivity      INTEGER,
  polarity          INTEGER)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS resources (
  url               TEXT NOT NULL UNIQUE,
  long_url          TEXT,
  title             TEXT)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_resource (
  tweet_id          INTEGER NOT NULL,
  resource          TEXT NOT NULL)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_user (
  tweet_id          INTEGER NOT NULL,
  user              TEXT NOT NULL,
  user_id           INTEGER NOT NULL)
''')

def initialise_mysql_database(db):
    db.execute('''
CREATE TABLE IF NOT EXISTS tweets (
  id                BIGINT NOT NULL UNIQUE,
  text              VARCHAR(140) NOT NULL,
  terms             VARCHAR(64) NOT NULL,
  from_user         VARCHAR(16) NOT NULL,
  from_user_id      BIGINT NOT NULL,
  in_reply_to_status BIGINT,
  in_reply_to_user  BIGINT,
  retweeted         TINYINT NOT NULL DEFAULT 0,
  retweet_count     INTEGER NOT NULL DEFAULT 0,
  created_at        VARCHAR(32) NOT NULL,
  iso_language_code CHAR(2),
  geo               VARCHAR(255),
  clean_text        VARCHAR(255),
  parse_tree        TEXT,
  emoticon          CHAR(2),
  subjectivity      INT,
  polarity          INT)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS resources (
  url               VARCHAR(255) NOT NULL UNIQUE,
  long_url          TEXT,
  title             TEXT)
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_resource (
  tweet_id          BIGINT NOT NULL,
  resource          VARCHAR(255) NOT NULL,
  KEY mention (tweet_id, resource))
''')
    db.execute('''
CREATE TABLE IF NOT EXISTS tweet_mentions_user (
  tweet_id          BIGINT NOT NULL,
  user              VARCHAR(16) NOT NULL,
  user_id           BIGINT NOT NULL)
''')

def tweet_exists(db, tweet):
    sql = '''SELECT id FROM tweets WHERE id=%s LIMIT 1''' % (DB_PARAM_PLACEHOLDER,)
    db.execute(sql, (tweet['id'],))
    return db.rowcount == 1

def extract_user(user):
    if user is not None:
        return user.get('screen_name')
    else:
        return None

def extract_user_id(user):
    if user is not None:
        return user.get('id')
    else:
        return None

def extract_lang_code(metadata):
    if metadata is not None:
        return metadata.get('iso_language_code')
    else:
        return None

def extract_coords(geo):
    if geo is not None and geo['type'] == 'Point':
        return '%f,%f' % (geo['coordinates'][0], geo['coordinates'][1])
    else:
        return None

def insert_tweet(db, tweet, terms=None):
    #              [ ((DB field, JSON field), extraction function) ]
    insert_fields = [(('id', 'id'), lambda a: a),
                     (('text', 'text'), lambda a: a),
                     (('terms', 'terms'), lambda a: a),
                     (('from_user', 'user'), extract_user),
                     (('from_user_id', 'user'), extract_user_id),
                     (('created_at', 'created_at'), lambda a: a),
                     (('in_reply_to_status', 'in_reply_to_status_id'), lambda a: a != '0' and a or None),
                     (('in_reply_to_user', 'in_reply_to_user_id'), lambda a: a != '0' and a or None),
                     (('retweeted', 'retweeted'), lambda a: a),
                     (('retweet_count', 'retweet_count'), lambda a: a),
                     (('iso_language_code', 'metadata'), extract_lang_code),
                     (('geo', 'geo'), extract_coords)]
    
    tweet['terms'] = terms
    sql = '''INSERT INTO tweets (%s) VALUES (%s)''' % (','.join([db_fn for ((db_fn, tw_fn), x) in insert_fields if tw_fn in tweet]),
                                                       ','.join([DB_PARAM_PLACEHOLDER for ((db_fn, tw_fn), x) in insert_fields if tw_fn in tweet]))
    args = [extract(tweet[tw_fn]) for ((db_fn, tw_fn), extract) in insert_fields if tw_fn in tweet]

    try:
        db.execute(sql, args)
        record_at_mentions(db, tweet)
    except (sqlite3.IntegrityError, _mysql_exceptions.IntegrityError):
        print 'missed duplicate!',

def retrieve_tweets(db, terms, lat=None, long=None, radius=None):
    MAX_PAGES = 3

    search_base = 'https://api.twitter.com/1.1/search/tweets.json'
    args = {'q': quote_plus(terms),
            'result_type': 'recent',
            'count': '100',
            'lang': 'en'}

    if lat is not None and long is not None and radius is not None:
        args['geocode'] = '%f,%f,%dkm' % (float(lat), float(long), int(radius))

    oauth = get_oauth()

    for i in range(MAX_PAGES):
        try:
            r = requests.get(url=search_base + '?' + urlencode(args), auth=oauth)
        except urllib2.HTTPError:
            traceback.print_exc()
            break

        for tweet in r.json()['statuses']:
            print tweet['id'], '...',
            if not tweet_exists(db, tweet):
                insert_tweet(db, tweet, terms)
                print 'Done.'
            else:
                print 'Duplicate.'

        if 'next_results' not in r.json()['search_metadata']:
            print 'No next results'
            break

        args = dict([(k, v[0]) for (k, v) in parse_qs(r.json()['search_metadata']['next_results'].lstrip('?')).items()])

# Regular expressions
positive_emoticon_re = re.compile(r'[:=]-?[)DpP]')
negative_emoticon_re = re.compile(r'[:=]-?[(<]')
url_re               = re.compile(r'https?://[A-Za-z0-9-_.]+\.[A-Za-z]+[A-Za-z0-9/._%?=&-]*')
at_mention_re        = re.compile(r'@[A-Za-z0-9_]{1,15}')
retweet_re           = re.compile(r'(?<![\w\d])RT:?(?=\s)')

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
            tags.append((emoticon, tweet['id']))

    update_emoticons = '''UPDATE tweets SET emoticon=%s WHERE id=%s''' % (DB_PARAM_PLACEHOLDER, DB_PARAM_PLACEHOLDER)
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
    
    insert_urls = '''INSERT IGNORE INTO resources (url) VALUES (%s)''' % (DB_PARAM_PLACEHOLDER,)
    db.executemany(insert_urls, urls)

    insert_mentions = '''REPLACE INTO tweet_mentions_resource (tweet_id, resource) VALUES (%s, %s)''' % (DB_PARAM_PLACEHOLDER, DB_PARAM_PLACEHOLDER)
    db.executemany(insert_mentions, mentions)
    
def remove_urls(tweet_text):
    (s, urls_removed) = url_re.subn('', tweet_text)

    return s

# @-mentions

def record_at_mentions(db, tweet):
    try:
        mentions = tweet['entities']['user_mentions']
    except KeyError:
        return

    insert_mentions = '''INSERT IGNORE INTO tweet_mentions_user (tweet_id, user, user_id) VALUES (%(p)s, %(p)s, %(p)s)''' % {'p': DB_PARAM_PLACEHOLDER}
    db.executemany(insert_mentions, [[tweet['id'], m['screen_name'], m['id']] for m in mentions])

def remove_at_mentions(tweet_text):
    (s, at_mentions_removed) = at_mention_re.subn('', tweet_text)

    return s

# Retweets

def is_retweet(tweet_text):
    return retweet_re.search(tweet_text) is not None

def remove_rt_markers(tweet_text):
    (s, rts_removed) = retweet_re.subn('', tweet_text)

    return s

def tag_retweets(db):
    db.execute('''SELECT id, text FROM tweets''')
    db.executemany('''UPDATE tweets SET retweeted=1 WHERE id=%s''', [(tweet['id'],) for tweet in db.fetchall() if is_retweet(tweet['text'])])
    
# Cleaning

def clean_tweet(tweet_text):
    return remove_at_mentions(remove_urls(remove_emoticons(remove_rt_markers(tweet_text))))

def clean_tweets(db):
    db.execute('''SELECT id, text FROM tweets''')
    clean_tweets = []
    for tweet in db.fetchall():
        clean_tweets.append((clean_tweet(tweet['text']), tweet['id']))

    update_tweets = '''UPDATE tweets SET clean_text=%s WHERE id=%s''' % (DB_PARAM_PLACEHOLDER, DB_PARAM_PLACEHOLDER)
    db.executemany(update_tweets, clean_tweets)

# Tokenizing

def tokenize(tweet_text):
    return tweet_text.split()

# Word lists

def tag_word_list(db):
    pass

def main():
    # get command line options
    options, args = getopt(sys.argv[1:], 'k:s:o:c:e:d:u:p:t:l:')
    options = dict([(o.strip('-'), a) for (o, a) in options if a != ''])

    # set up OAuth secrets
    global CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET
    if 'k' not in options and not CONSUMER_KEY:
        print 'Please specify Twitter consumer key with -k'
        sys.exit(1)
    if 's' not in options and not CONSUMER_SECRET:
        print 'Please specify Twitter consumer secret with -s'
        sys.exit(1)
    if 'o' not in options and not OAUTH_TOKEN:
        print 'Please specify OAuth token with -o'
        sys.exit(1)
    if 'c' not in options and not OAUTH_TOKEN_SECRET:
        print 'Please specify OAuth secret with -c'
        sys.exit(1)

    CONSUMER_KEY = options.get('k', CONSUMER_KEY)
    CONSUMER_SECRET = options.get('s', CONSUMER_SECRET)
    OAUTH_TOKEN = options.get('o', OAUTH_TOKEN)
    OAUTH_TOKEN_SECRET = options.get('c', OAUTH_TOKEN_SECRET)

    # connect to database
    conn = db = None
    if not 'e' in options or options['e'] == 'mysql':
        try:
            conn, db = mysql_db_cursor(options.get('d', 'tweets'), options.get('u', 'root'), options.get('p', ''))
            initialise_mysql_database(db)
        except _mysql_exceptions.OperationalError, e:
            print 'Database error:', e
            sys.exit(1)
    elif options['e'] == 'sqlite':
        try:
            conn, db = sqlite_db_cursor(options.get('d', 'tweets.db'))
            initialise_sqlite_database(db)
        except sqlite3.OperationalError, e:
            print 'Database error:', e
            sys.exit(1)
    else:
        print 'You must specify a database type (-e mysql|sqlite) and database name (-d).'
        sys.exit(1)

    # retrieve some tweets
    if 't' in options:
        if 'l' in options:
            location_mo = re.search(r'^(-?[0-9]+\.?[0-9]*),(-?[0-9]+\.?[0-9]*),([0-9]+)km$', options['l'])
            if location_mo:
                print 'Location = ', location_mo.groups()
                retrieve_tweets(db, options['t'], *location_mo.groups())
            else:
                print 'Invalid location: "%s"' % options['l']
                sys.exit(1)
        else:
            retrieve_tweets(db, options['t'])
        conn.commit()

    # begin NLP pipeline
    tag_emoticons(db)
    tag_retweets(db)
    clean_tweets(db)
    record_resources(db)

    conn.commit()

if __name__ == '__main__':
    main()
