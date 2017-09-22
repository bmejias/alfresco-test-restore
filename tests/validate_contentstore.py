#! /usr/bin/env python

import argparse
import os
import psycopg2
import psycopg2.extras
import zlib

ALF_NS_CONTENT = 'http://www.alfresco.org/model/content/1.0'
QNAME_CONTENT = 'content'
qids = {}
stores = {}


def connect_to_db(pg_info):
    db_url = "host='%s'" % pg_info.host
    db_url += " port='%s'" % pg_info.port
    db_url += " dbname='%s'" % pg_info.dbname
    db_url += " user='%s'" % pg_info.username
    db_url += " password='%s'" % pg_info.password
    try:
        connection = psycopg2.connect(db_url)
        return connection
    except:
        print("I am unable to connect to the database")


def get_content_qid(cursor):
    if QNAME_CONTENT not in qids:
        query = """
        SELECT q.id FROM alf_qname q, alf_namespace ns
        WHERE q.local_name='%s' AND q.ns_id=ns.id AND ns.uri='%s'
        """ % (QNAME_CONTENT, ALF_NS_CONTENT)
        cursor.execute(query)
        res = cursor.fetchone()
        qids[QNAME_CONTENT] = res[0]
    return qids[QNAME_CONTENT]


def get_stores(cursor):
    if stores == {}:
        query = "SELECT * from alf_store"
        cursor.execute(query)
        for row in cursor.fetchall():
            stores[row['id']] = row
    return stores


def find_node(cursor, content_url):
    short_url = content_url[-12:]
    crc_value = zlib.crc32(content_url)
    content_qid = get_content_qid(cursor)
    query = """
    SELECT n.*
    FROM alf_node n, alf_node_properties np, alf_content_data cd,
         alf_content_url cu
    WHERE cu.content_url_short = '%s' AND cu.content_url_crc = '%s'
    AND cu.id = cd.content_url_id
    AND np.long_value = cd.id
    AND np.qname_id = %s
    AND n.id = np.node_id
    """ % (short_url, crc_value, content_qid)
    cursor.execute(query)
    res = cursor.fetchone()
    if res is None:
        return None
    else:
        return res


def get_nodeRef(cursor, content_url):
    node = find_node(cursor, content_url)
    if node is None:
        return "no node"
    else:
        stores = get_stores(cursor)
        prefix = "%(protocol)s://%(identifier)s" % stores[node['store_id']]
        return prefix + '/' + node['uuid']


def get_dict_cursor(connection):
    return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_content_urls(cursor):
    query = "SELECT content_url, content_size FROM alf_content_url"
    cursor.execute(query)
    return cursor.fetchall()


def filename(content_url, contentstore):
    return content_url.replace('store://', contentstore + '/')


def main():
    # Default properties
    pg_props = {'host': '127.0.0.1',
                'port': 5432,
                'dbname': 'alfresco',
                'username': 'alfresco',
                'password': 'alfresco'}
    contentstore = '/opt/alfresco/alf_data/contentstore'
    # prepare properties from arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', action="store",
                        default=pg_props['host'],
                        help="database server host")
    parser.add_argument('--port', action="store", type=int,
                        default=pg_props['port'],
                        help="database server port")
    parser.add_argument('--dbname', action="store",
                        default=pg_props['dbname'],
                        help="database name to connect to")
    parser.add_argument('--username', action="store",
                        default=pg_props['username'],
                        help="database user name")
    parser.add_argument('--password', action="store",
                        default=pg_props['password'],
                        help="user's password")
    parser.add_argument('--contentstore', action="store",
                        default=contentstore,
                        help="location of the content store")
    args = parser.parse_args()
    contentstore = args.contentstore
    connection = connect_to_db(args)
    cursor = get_dict_cursor(connection)
    ok = 0
    nok = []
    orphans = []
    content_urls = get_content_urls(cursor)
    for row in content_urls:
        ok += check_size(filename(row['content_url'], contentstore),
                         row['content_size'], nok, orphans, row['content_url'],
                         cursor)
    print('Total files: %s' % len(content_urls))
    print('Validated: %s' % ok)
    if len(nok) is 0:
        print('NOK: 0')
    else:
        for nok_file in nok:
            print('NOK: %s' % nok_file)
    if len(orphans) is 0:
        print('Orphans: 0')
    else:
        for orphan_file in orphans:
            print('Orphans: %s' % orphan_file)


def check_size(filename, size, nok, orphans, content_url, cursor):
    if os.path.isfile(filename):
        actual_size = os.path.getsize(filename)
        if size == actual_size:
            return 1
        else:
            nok.append([filename, size, actual_size])
            return 0
    else:
        noderef = get_nodeRef(cursor, content_url)
        orphans.append({'filename': filename, 'noderef': noderef})
        return 0


if __name__ == '__main__':
    main()
