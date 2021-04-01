#!/usr/local/bin/python3
# encoding: utf-8
'''
@author:     Sid Probstein
@license:    MIT License (https://opensource.org/licenses/MIT)
@contact:    sidprobstein@gmail.com
'''

import argparse
import sys
import glob
import os

from whoosh.index import create_in, open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser

#############################################    

def main(argv):

    script_name = "whoosh_cmd"
    
    parser = argparse.ArgumentParser(description="Index and query text files with whoosh from the command line")
    parser.add_argument('target', help="path to one or more text files to index, or the search query to execute")
    parser.add_argument('-c', '--collection', default='default.idx', help="name of the collection, optional")
    parser.add_argument('-i', '--index', action='store_true', help="index content")
    parser.add_argument('-d', '--debug', action='store_true', help="provide debugging information")
    parser.add_argument('-e', '--explain', action='store_true', help="show explanation after classifying")    
    args = parser.parse_args()
    
    # to do: for now this is the only schema we support
    schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)

    if args.index:
        # index files
        # create collection if it does not exist
        if not os.path.isdir(args.collection):
            os.mkdir(args.collection)
        ix = create_in(args.collection, schema)
        writer = ix.writer()
        # to do: read the docs
        writer.add_document(title=u"First document", path=u"/a",
                        content=u"This is the first document we've added!")
        writer.add_document(title=u"Second document", path=u"/b",
                        content=u"The second one is even more interesting!")
        writer.commit()
    else:   
        if not os.path.isdir(args.collection):
            print("Error: collection not found:", args.collection)
            sys.exit(1)
        # to do: check if we should replace the below
        ix = open_dir(args.collection)
        # query index
        with ix.searcher() as searcher:
            print("target:", args.target)
            query = QueryParser("content", ix.schema).parse(args.target)
            results = searcher.search(query)
            print(results[0])
        # end with
    # end if
        
# end main

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end