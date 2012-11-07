#!/usr/bin/python

import os
import sys
import logging
import time
import datetime

import xappy
from xappy import Field
import baker

def init_logging(verbose):
    logging.basicConfig(
            level=logging.INFO if verbose else logging.WARN,
            format='%(asctime)s %(name)s [%(levelname)s]: %(message)s')

def open_db_write(index):
    logging.info('opening database connection')
    db = xappy.IndexerConnection(index)

    logging.info('initializing schema')
    db.add_field_action('path', xappy.FieldActions.INDEX_EXACT)
    db.add_field_action('mtime', xappy.FieldActions.SORTABLE,
            type='float')
    db.add_field_action('ctime', xappy.FieldActions.SORTABLE,
            type='float')

    # Data that gets stored in the index.
    for field in ['path', 'mtime', 'ctime']:
        db.add_field_action(field, xappy.FieldActions.STORE_CONTENT)

    # We index but do not store document content.
    db.add_field_action('content', xappy.FieldActions.INDEX_FREETEXT,
            language='en', spell=True)

    return db

def open_db_read(index):

    db = xappy.SearchConnection(index)
    return db

def normalize_path(path):
    path = os.path.realpath(path)
    path = os.path.normpath(path)
    return path

@baker.command(shortopts={
    'index': 'i',
    'source': 's',
    'verbose':'v',
    'git': 'G',
    })
def index(index='.fti', source='.', verbose=False, git=False):
    '''Index a collection of documents.
    
    :param index: Path to index directory
    :param source: Path to document collection
    '''

    init_logging(verbose)

    index = normalize_path(index)
    db = open_db_write(index)

    logging.info('indexing files')
    for dirpath, dirnames, filenames in os.walk(source):
        dirpath = normalize_path(dirpath)
        if dirpath == index:
            continue

        if '.git' in dirnames and not git:
            dirnames.remove('.git')

        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            filepath = normalize_path(filepath)
            filestat = os.stat(filepath)

            try:
                check = db.get_document(filepath)

                # NB: check.data[field] returns a *list*.
                if not filestat.st_mtime > check.data['mtime'][0]:
                    logging.info('already in index: %s' % filepath)
                    continue

                logging.info('updating: %s' % filepath)
                db.delete(filepath)
            except KeyError:
                pass

            logging.info('adding: %s' % filepath)
            doc = xappy.UnprocessedDocument()
            doc.id = filepath
            doc.fields.append(Field('path', filepath))
            doc.fields.append(Field('content', 
                open(filepath).read()))

            doc.fields.append(Field('mtime', filestat.st_mtime))
            doc.fields.append(Field('ctime', filestat.st_ctime))

            db.add(doc)

    db.flush()
    db.close()

@baker.command(shortopts={'index': 'i', 'verbose': 'v'})
def search(index='.fti', verbose=False, *terms):
    '''Search an index and display matching files.

    :param index: Path to index directory
    :param verbose: Display verbose diagnostics if True
    '''

    init_logging(verbose)

    index = normalize_path(index)
    db = open_db_read(index)

    query = ' '.join(terms)
    logging.info('searching %s for: %s' % (index, query))

    parsed_query = db.query_parse(query, default_op=db.OP_AND)
    results = db.search(parsed_query, 0, 10)

    if results.estimate_is_exact:
        logging.info('found %d results' % results.matches_estimated)
    else:
        logging.info('found approximately %d results' % results.matches_estimated)

    for res in results:
        print res.id



if __name__ == '__main__':
    baker.run()

