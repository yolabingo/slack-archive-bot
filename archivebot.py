#!/usr/bin/env python
import datetime
import os
import sqlite3
import time
import traceback

from string import Template
from slackclient import SlackClient
from websocket import WebSocketConnectionClosedException

# Connects to the previously created SQL database
conn = sqlite3.connect('slack.sqlite')
cursor = conn.cursor()
cursor.execute('create table if not exists messages (message text, user text, channel text, timestamp text, UNIQUE(channel, timestamp) ON CONFLICT REPLACE)')
cursor.execute('create table if not exists users (name text, id text, avatar text, UNIQUE(id) ON CONFLICT REPLACE)')
cursor.execute('create table if not exists channels (name text, id text, UNIQUE(id) ON CONFLICT REPLACE)')

# This token is given when the bot is started in terminal
slack_token = os.environ["SLACK_API_TOKEN"]

# Makes bot user active on Slack
# NOTE: terminal must be running for the bot to continue
sc = SlackClient(slack_token)

# Double naming for better search functionality
# Keys are both the name and unique ID where needed
ENV = {
    'user_id': {},
    'id_user': {},
    'channel_id': {},
    'id_channel': {}
}

# Uses slack API to get most recent user list
# Necessary for User ID correlation
def update_users():
    info = sc.api_call('users.list')
    ENV['user_id'] = dict([(m['name'], m['id']) for m in info['members']])
    ENV['id_user'] = dict([(m['id'], m['name']) for m in info['members']])

    args = []
    for m in info['members']:
        args.append((
            m['name'],
            m['id'],
            m['profile'].get('image_72', 'https://secure.gravatar.com/avatar/c3a07fba0c4787b0ef1d417838eae9c5.jpg?s=32&d=https%3A%2F%2Ffst.slack-edge.com%2F66f9%2Fimg%2Favatars%2Fava_0024-32.png')
        ))
    cursor.executemany("INSERT INTO users(name, id, avatar) VALUES(?,?,?)", args)
    conn.commit()

def get_user_name(uid):
    if uid not in ENV['id_user']:
        update_users()
    return ENV['id_user'].get(uid, None)

def get_user_id(name):
    if name not in ENV['user_id']:
        update_users()
    return ENV['user_id'].get(name, None)


def update_channels():
    info = sc.api_call('channels.list')
    ENV['channel_id'] = dict([(m['name'], m['id']) for m in info['channels']])
    ENV['id_channel'] = dict([(m['id'], m['name']) for m in info['channels']])

    args = []
    for m in info['channels']:
        args.append((
            m['name'],
            m['id'] ))
    cursor.executemany("INSERT INTO channels(name, id) VALUES(?,?)", args)
    conn.commit()

def get_channel_name(uid):
    if uid not in ENV['id_channel']:
        update_channels()
    return ENV['id_channel'].get(uid, None)

def get_channel_id(name):
    if name not in ENV['channel_id']:
        update_channels()
    return ENV['channel_id'].get(name, None)

def convert_timestamp(ts):
    return datetime.datetime.fromtimestamp(
        int(ts.split('.')[0])
    ).strftime('%Y-%m-%d %H:%M:%S')

def send_slack_message(message, channel):
    if not message:
        message = 'No results found ' + handle_query.__doc__
    sc.api_call(
      "chat.postMessage",
      channel=channel,
      text=message
    )

def handle_query(event):
    """
    Handles a DM to the bot that is requesting a search of the archives.

    Usage:

        <query> from:<user> in:<channel> sort:asc|desc limit:<number> context:<number>

        query: The text to search for.
        user: If you want to limit the search to one user, the username.
        channel: If you want to limit the search to one channel, the channel name.
        sort: Either asc if you want to search starting with the oldest messages,
            or desc if you want to start from the newest. Default desc.
        limit: The number of responses to return. Default 10.
        context: The number of messages before and after the found message to return. Default 0.
    """

    try:
        text = []
        user = None
        channel = None
        sort = 'desc'
        limit = 10
        context = 0

        params = event['text'].lower().split()
        for p in params:
            # Handle emoji
            # usual format is " :smiley_face: "
            if len(p) > 2 and p[0] == ':' and p[-1] == ':':
                text.append(p)
                continue

            p = p.split(':')

            if len(p) == 1:
                text.append(p[0])
            if len(p) == 2:
                if p[0] == 'from':
                    user = get_user_id(p[1].replace('@','').strip())
                    if user is None:
                        raise ValueError('User %s not found' % p[1])
                if p[0] == 'in':
                    channel = get_channel_id(p[1].replace('#','').strip())
                    if channel is None:
                        raise ValueError('Channel %s not found' % p[1])
                if p[0] == 'sort':
                    if p[1] in ['asc', 'desc']:
                        sort = p[1]
                    else:
                        raise ValueError('Invalid sort order %s' % p[1])
                if p[0] == 'limit':
                    try:
                        limit = int(p[1])
                    except:
                        raise ValueError('%s not a valid number' % p[1])
                if p[0] == 'context':
                    try:
                        context = int(p[1])
                        if context < 0:
                            context = 0
                    except:
                        raise ValueError('%s not a valid number' % p[1])

        if " ".join(text) == "help":
            send_slack_message(handle_query.__doc__, event['channel'])

        query = 'SELECT message,user,timestamp,channel FROM messages WHERE message LIKE "%%%s%%"' % " ".join(text)
        if user:
            query += ' AND user="%s"' % user
        if channel:
            query += ' AND channel="%s"' % channel
        if sort:
            query += ' ORDER BY timestamp %s' % sort

        # print(query)
        cursor.execute(query)
        res = cursor.fetchmany(limit)
        msg_txt = ''
        if context:
            for (msg, user, timestamp, channel) in res:
                if msg_txt:
                    msg_txt += "\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n"
                # get this message and the messages immediately preceeding it from channel
                cursor.execute('SELECT message,user,timestamp,channel FROM messages WHERE channel="%s" AND timestamp <= "%s" ORDER BY timestamp DESC' % (channel, timestamp))
                # reverse from DESC so they are chronological
                # if len(res) > 1:
                msg_txt += format_results(reversed([i for i in cursor.fetchmany(context + 1)]))

                # get messages immediately folliwing from channel
                cursor.execute('SELECT message,user,timestamp,channel FROM messages WHERE channel="%s" AND timestamp > "%s" ORDER BY timestamp ASC' % (channel, timestamp))
                msg_txt += format_results(cursor.fetchmany(context))
        else:
            msg_txt = format_results(res)
        send_slack_message(msg_txt, event['channel'])
    except ValueError as e:
        print(traceback.format_exc())
        send_slack_message(str(e), event['channel'])

def format_results(result):
    """
    convert results from 'SELECT message,user,timestamp FROM messages...' sqlite fetchmany() into text string
    """
    message = ''
    for (msg, user, timestamp, channel) in result:
        message += '%s (@%s in #%s, %s) \n' % (msg, get_user_name(user), get_channel_name(channel), convert_timestamp(timestamp))
    print(message.strip())
    return(message.strip())

def handle_message(event):
    try:
        if event['subtype'] == 'message_changed':
            # rely on 'ON CONFLICT REPLACE' for messages table
            event['text'] = event['message']['text']
            event['user'] = event['previous_message']['user']
            event['ts'] = event['previous_message']['ts']
    except KeyError:
        pass
    if 'text' not in event:
        return
    if 'username' in event and event['username'] == 'bot':
        return

    try:
        print(event)
    except:
        print("*"*20)

    # If it's a DM, treat it as a search query
    if event['channel'][0] == 'D':
        handle_query(event)
    elif 'user' not in event:
        print("No valid user. Previous event not saved")
    else: # Otherwise save the message to the archive.
        cursor.executemany('INSERT INTO messages VALUES(?, ?, ?, ?)',
            [(event['text'], event['user'], event['channel'], event['ts'])]
        )
        conn.commit()
        print("--------------------------")

# Loop
if sc.rtm_connect():
    update_users()
    print('Users updated')
    update_channels()
    print('Channels updated')
    print('Archive bot online. Messages will now be recorded...')
    while True:
        try:
            for event in sc.rtm_read():
                if event['type'] == 'message':
                    handle_message(event)
        except WebSocketConnectionClosedException:
            sc.rtm_connect()
        except:
            print(traceback.format_exc())
        time.sleep(1)
else:
    print("Connection Failed, invalid token?")
