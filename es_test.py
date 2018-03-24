import os
import glob
# import mailbox
import email
import mailparser
import argparse

from elasticsearch import Elasticsearch
from elasticsearch import helpers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='input directory')
    args = parser.parse_args()

    # files = os.listdir(args.directory)
    files = glob.glob(args.directory + '/*.eml')
    files = glob.glob('./*.eml')
    # box = mailbox.mbox(m)

    emails = [mailparser.parse_from_file(x) for x in files]
    emails_json = [x.mail for x in emails]

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    load_to_es(es, emails_json)


def load_to_es(es, emails_json):

    actions = [
        {
            "_index": "emails",
            "_type": "email",
            "_id": str(ix),
            "doc": js
        }
        for ix, js in enumerate(emails_json)
    ]

    helpers.bulk(es, actions, index='emails', doc_type='email')

    #res = es.bulk(index='emails', body=emails_json, refresh=True)


if __name__ == "__main__":
    main()
