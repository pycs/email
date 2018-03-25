import os
import glob
# import mailbox
import email
import mailparser
import argparse

from elasticsearch import Elasticsearch
from elasticsearch import helpers

lexicon = {'technologies', 'party', 'hour'}


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
    emails_json = split_fields(emails_json)

    for mail in emails_json:
        mail['matches'] = lexicon_matches(mail)

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    load_to_es(es, emails_json)


def split_fields(emails_json):

    def split_people(mail, field):
        if(mail[field]):
            mail[field + '_name'] = [x[0] for x in mail[field]]
            mail[field + '_email'] = [x[1] for x in mail[field]]
        return mail

    emails_json = [split_people(mail, field)
                   for mail in emails_json
                   for field in set(mail.keys()).intersection({'from', 'to', 'cc'})]

    return emails_json


def lexicon_matches(mail):
    return [w for w in lexicon if w in mail['body'].lower()]


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
