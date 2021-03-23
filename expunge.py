#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mysql import connector as MySQL
from mysql.connector import Error as MySQLError
from enum import Enum
from sys import exit as sys_exit
from subprocess import run as subprocess_run, PIPE as subprocess_PIPE
from os import environ

class LogLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4

try:
    LOG_LEVEL = LogLevel[environ.get('LOG_LEVEL').upper()]
except:
    LOG_LEVEL = LogLevel.INFO
CONFIG_FILE = environ.get('SQL_CONFIG', '/etc/dovecot/conf.d/dovecot-sql.conf')

def debug(msg, *args, **kwargs):
    __log(LogLevel.DEBUG, msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    __log(LogLevel.INFO, msg, *args, **kwargs)

def warn(msg, *args, **kwargs):
    __log(LogLevel.WARN, msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    __log(LogLevel.ERROR, msg, *args, **kwargs)

def fatal(msg, *args, **kwargs):
    __log(LogLevel.FATAL, msg, *args, **kwargs)
    sys_exit(1)

def __log(level, msg, *args, **kwargs):
    if level.value < LOG_LEVEL.value:
        return

    print('[{level:>5}] {msg}'.format(level=level.name,
                                      msg=msg.format(*args, **kwargs)))

def get_connection():
    connect = None
    try:
        with open(CONFIG_FILE) as config:
            for line in config:
                if line.startswith('#') or '=' not in line:
                    continue

                key, value = line.split('=', 1)
                if key.strip(' ') == 'connect':
                    connect = value.strip(' "\n')
                    break
    except (OSError, IOError) as err:
        fatal('Could not read connection parameter file {config_file}: {err}',
              config_file=CONFIG_FILE, err=err)

    if not connect:
        fatal('Could not read connection parameters '
              'from config file {config_file}', config_file=CONFIG_FILE)

    try:
        con_params = dict(item.split('=', 1) for item in connect.split(' '))
    except:
        fatal('Bad format for connection parameters')

    try:
        con = MySQL.connect(unix_socket=con_params['host'],
                            user=con_params['user'],
                            password=con_params['password'],
                            database=con_params['dbname'])
        return con
    except MySQLError as err:
        fatal('Could not connect to database: {err}', err=err)

def for_records(con, action):
    cur = con.cursor()
    cur.execute('''
            SELECT CONCAT(`accounts`.`username`, '@', `domains`.`domain`) AS `user`, `expiry`.`mailbox` AS `mailbox`, `expiry`.`expiry` AS `expiry`
            FROM `expiry`
            LEFT JOIN `accounts` ON `expiry`.`account_id` = `accounts`.`id`
            LEFT JOIN `domains` ON `accounts`.`domain_id` = `domains`.`id`
            WHERE `expiry`.`enabled`
        ''')
    
    for (user, mailbox, expiry) in cur:
        action(user, mailbox, expiry)

    cur.close()

def run_expunge(user, mailbox, expiry):
    debug('Expunging {user} - {mailbox} : {expiry}',
          user=user, mailbox=mailbox, expiry=expiry)
    try:
        fetch = subprocess_run(['doveadm',
                                'fetch',
                                '-u', user,
                                'uid hdr.subject hdr.from',
                                'mailbox', mailbox,
                                'savedbefore', '{expiry}d'.format(expiry=expiry)
                                ], stdout=subprocess_PIPE) 
        for lines in fetch.stdout.decode('utf-8').split('\n\f'):
            if ':' not in lines:
                continue
            message = dict(item.split(': ', 1) for item in lines.strip().split('\n'))
            debug('Will expunge message {uid}, from {hdr_from} : {hdr_subject}',
                  uid=message['uid'], hdr_from=message['hdr.from'],
                  hdr_subject=message['hdr.subject'])
    except:
        pass

    expunge = subprocess_run(['doveadm',
                              'expunge',
                              '-u', user,
                              'mailbox', mailbox,
                              'savedbefore', '{expiry}d'.format(expiry=expiry)
                              ], stdout=subprocess_PIPE)
    r = expunge.stdout.decode('utf-8').strip()
    if r:
        info('Expunge for user {user} returned: {r}', user=user, r=r)


def main():
    info('Beginning Dovecot Expunge')

    con = get_connection()
    for_records(con, run_expunge)
    con.close()

    info('Finished Dovecot Expunge')

if __name__ == '__main__':
    main()

