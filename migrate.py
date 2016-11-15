#!/usr/bin/env python3

import argparse
import io
import mailbox
import os
import sys
import time
import traceback

import apiclient
import httplib2
import oauth2client


SCOPES = ['https://www.googleapis.com/auth/apps.groups.migration']


def upload(service, args, mbox):
    archive = service.archive()

    total_messages = len(mbox)
    skipped = 0
    failed = 0
    for i, message in enumerate(mbox):
        if i < args.starting_from:
            skipped += 1
            continue

        if i % args.log_every_n == 0:
            print('Uploading message', i, 'of', total_messages)
            print('   ', 'Subject:', message['Subject'], '  Date:', message['Date'])

        mstr = message.as_string()
        if len(mstr) > args.message_length_limit:
            print('Length of', message['Subject'], '(', len(mstr), 'bytes)',
                  'exceeds maximum message length of', args.message_length_limit, file=sys.stderr)
            failed += 1
            continue

        try:
            mio = io.StringIO(mstr)
            media = apiclient.http.MediaIoBaseUpload(mio, mimetype='message/rfc822')
            time.sleep(1.0 / args.qps)
            result = archive.insert(groupId=args.group_id, media_body=media).execute()
            assert result['responseCode'].lower() == 'success'
        except Exception as e:
            print('Exception when uploading message', message['Subject'], file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            failed += 1
            continue

    print('Successfully uploaded', total_messages - failed - skipped,
          'of', total_messages, 'messages!')


def get_service(args):
    credential_dir = os.path.dirname(args.credential_file)
    if not os.path.isdir(credential_dir):
        os.makedirs(credential_dir)
    store = oauth2client.file.Storage(args.credential_file)
    credentials = store.get()

    if not credentials or credentials.invalid:
        flow = oauth2client.client.flow_from_clientsecrets(args.client_id_file, SCOPES)
        credentials = oauth2client.tools.run_flow(flow, store, args)

    http = credentials.authorize(httplib2.Http())
    return apiclient.discovery.build('groupsmigration', 'v1', http=http)


def main():
    parser = argparse.ArgumentParser(parents=[oauth2client.tools.argparser],
                                     description='Migrate an mbox archive to Google Groups')
    parser.add_argument('mbox_file',
                        help='path of .mbox archive file')
    parser.add_argument('group_id',
                        help='email address of google group')
    parser.add_argument('--log_every_n', type=int, default=100)
    parser.add_argument('--message_length_limit', type=int, default=15 * (2 ** 20))
    parser.add_argument('--qps', type=int, default=9)
    parser.add_argument('--starting_from', type=int, default=0)
    parser.add_argument('--client_id_file', default='client_id.json')
    parser.add_argument('--credential_file', default=os.path.join(os.path.expanduser('~'),
                                                                  '.google',
                                                                  'mailmanmigration.json'))
    args = parser.parse_args()

    assert os.path.isfile(args.mbox_file)
    mbox = mailbox.mbox(args.mbox_file)

    service = get_service(args)
    upload(service, args, mbox)


if __name__ == '__main__':
    main()
