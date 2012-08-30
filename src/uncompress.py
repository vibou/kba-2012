#!/usr/bin/python
'''
uncompress the corpus

uncompress.py <gpg_key_path> <thrift_dir> <date_hour> <save_dir>

'''

import re
import os
import sys
import gzip
import json
import time
import copy
import hashlib
import subprocess
from cStringIO import StringIO

gpg_base_dir = './gpg/';

def log(m, newline='\n'):
    sys.stderr.write(m + newline)
    sys.stderr.flush()

def decrypt_and_verify(gpg_key_path, thrift_dir, date_hour, save_dir):
    '''
    reads in the compressed-&-encrypted thrifts of thrift_dir and
    uncompress them
    '''
    ### setup gpg for encryption
    gpg_dir = gpg_base_dir + date_hour + '.gpg-dir'
    if not os.path.exists(gpg_dir):
        os.makedirs(gpg_dir)
    gpg_child = subprocess.Popen(['gpg', '--no-permission-warning', '--homedir',
      gpg_dir, '--import', gpg_key_path], stderr=subprocess.PIPE)
    s_out, errors = gpg_child.communicate()
    if errors:
        log("gpg prints to stderr, even when nothing is wrong, read carefully:\n\n%s"
            % errors)

    for fname in os.listdir(os.path.join(thrift_dir, date_hour)):
        ## ignore other files, e.g. stats.json
        if not fname.endswith('.xz.gpg'): continue

        ### reverse the steps from above:
        ## load the encrypted data
        fpath = os.path.join(thrift_dir, date_hour, fname)
        encrypted_data = open(fpath).read()

        assert len(encrypted_data) > 0, "failed to load: %s" % fpath

        ## decrypt it, and free memory
        ## encrypt using the fingerprint for our trec-kba-rsa key pair
        gpg_child = subprocess.Popen(
            ## setup gpg to encrypt with trec-kba public key
            ## (i.e. make it the recipient), with zero compression,
            ## ascii armoring is off by default, and --output - must
            ## appear before --encrypt -
            ['gpg',   '--no-permission-warning', '--homedir', gpg_dir,
              '--trust-model', 'always', '--output', '-', '--decrypt', '-'],
            stdin =subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        ## communicate with child via its stdin
        compressed_data, errors = gpg_child.communicate(encrypted_data)
        if errors:
            log(errors)

        ## save the compressed and decrypted data:
        decrypt = False
        if decrypt:
            parts = fname.split('.')
            assert 'gpg' == parts.pop()
            open(os.path.join(thrift_dir, date_hour, '.'.join(parts)),
                'wb').write(compressed_data)

        ## uncompress it
        ## speak to xz over pipes as a child process, because python
        ## bindings to liblzma are... insufficient.
        # use subprocess.Popen and its commuicate method to avoid
        # blocking the child's stdin while waiting for us to read
        # stdout
        xz_child = subprocess.Popen(
            ['xz', '--decompress'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        thrift_data, errors = xz_child.communicate(compressed_data)

        ## catch anything from xz's stderr
        assert not errors, errors

        ## free memory
        compressed_data = None

        ## compare md5 hashes:
        content_md5 = hashlib.md5(thrift_data).hexdigest()
        assert content_md5 == fname.split('.')[1], \
            '%r != %r' % (content_md5, fname.split('.')[1])

        ## save the thirft data
        save_data_dir = save_dir + date_hour
        if not os.path.exists(save_data_dir):
            os.makedirs(save_data_dir)
        parts = fname.split('.')
        assert 'gpg' == parts.pop()
        assert 'xz' == parts.pop()
        open(os.path.join(save_data_dir, '.'.join(parts)),
            'wb').write(thrift_data)

        ## free memory
        thrift_data = None


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument('gpg_key_path')
    parser.add_argument('thrift_dir')
    parser.add_argument('date_hour')
    parser.add_argument('save_dir')

    args = parser.parse_args()

    decrypt_and_verify(args.gpg_key_path, args.thrift_dir, args.date_hour,
        args.save_dir)
