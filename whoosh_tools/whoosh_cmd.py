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
    parser.add_argument('-v', '--verbose', action='store_true', help="provide debugging information")
    args = parser.parse_args()
    
    # to do: for now this is the only schema we support
    schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)

    if args.index:
        # index files
        # create collection if it does not exist
        if not os.path.isdir(args.collection):
            os.mkdir(args.collection)
        if args.verbose:
            print("Info: collection:", args.collection)
        ix = create_in(args.collection, schema)
        writer = ix.writer()
        # read specified files
        lst_files = glob.glob(args.target)
        for f in lst_files:
            if os.path.exists(f):
                if args.verbose:
                    print("Info: reading:", f)
                # open teh file
                try:
                    fi = open(f, 'r')
                except e:
                    print("Error:", e)
                    sys.exit(1)
                # end if
            else:
                print("Warning: file not found:", f)
                continue
            # end if
            if f.endswith(".csv"):
                # read csv
                n_rows = 0
                csv_reader = csv.reader(fi)   
                lst_row = csv_reader.next()
                for lst_row in csv_reader:
                    fi_title = lst_row[0]
                    fi_path = os.getcwd() + '/' + lst_row[1]
                    fi_content = lst_row[2]
                    n_rows = n_rows + 1
                    # index
                    # to do: batch the records
                    writer.add_document(title=fi_title, path=fi_path, content=fi_content)
                # end for
                if args.verbose:
                    print("Info: added", n_rows, "rows to collection")
                fi.close()
            else:
                # read file
                fi_content = fi.readlines()
                if args.verbose:
                    print("Debug:", fi_content)
                fi.close()
                fi_title = f
                fi_path = "a"
                # index
                # to do: batch the files
                writer.add_document(title=fi_title, path=fi_path, content=fi_content)
                if args.verbose:
                    print("Info: added file:", f, "to collection")
            # end if
        # end for
        # commit changes
        writer.commit()
        if args.verbose:
            print("Info: Commit()")
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