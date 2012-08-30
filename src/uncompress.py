#!/usr/bin/python
'''
Provides two utilities:

1) transform_dir: is for your reference.  This is what we used to
transform the deprecated JSON-version of the corpus into this new
format.  This utility takes a directory path as input, reads a set of
gzip'ed files containing multiple JSON lines, converts them to
thrifts, compresses with xz, encrypts with gnupg, and writes out to a
new directory.

Example usage:
                                               new directory for thrifts    date_hour dir to transform           dir of input corpus
                                            vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv  vvvvvvvvvv                        vvvvvvvvvvvvvvvvvvvvvvvvvvv
./kba_thrift_verify trec-kba-rsa.pub /media/kba-stream-corpus-2012-thrifts 2012-02-03-04   --transform /media/kba-stream-corpus-2012-json
                                ^^^^
                                see note below

2) decrypt_and_verify: allows you to verify that you have received the
corpus.  It also provides a reference implementation for accessing the
content.  Decrypt and verify compares the compressed-&-encrypted
thrifts from the corpus and compares them with the stats.json file in
each directory.  Passing --decrypt on the command line causes this
function to also save the decrypted version of the data while
computing the verification stats.

Example usage:
                                          input dir of thrifts          date_hour dir to verify
                                        vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv  vvvvvvvvvv
./kba_thrift_verify trec-kba-rsa /media/kba-stream-corpus-2012-thrifts 2012-02-03-04   --verify --decrypt
                    ^^^^^^^^^^^^
                    private key

Note that one can also decrypt the corpus from the command line using
regular gpg, like this:

gpg --import trec-kba-rsa
for a in `ls /media/kba-stream-corpus-2012/*/*gpg`; do gpg $a; done;

Observe that the semantics of gpg are oriented around email, so you as
the recipient of the corpus (the 'message') have the so-called private
key in the symmetric key pair.  We have used the so-called public key
to encrypt the corpus.
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

try:
    from thrift import Thrift
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
except:
    ## If we are running in condor, then we might have to tell it
    ## where to find the python thrift library.  This path is from
    ## building thrift from source and not installing it.  It can be
    ## downloaded from:
    ## http://pypi.python.org/packages/source/t/thrift/thrift-0.8.0.tar.gz#md5=1f0f1524690e0849e35172f3ee033711
    ## and built using
    ##    python setup.py build
    sys.path.append('/data/zfs-scratch/kba/tools/KBA/2012-corpus/thrift-0.8.0/build/lib.linux-x86_64-2.6')

    from thrift import Thrift
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol

from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem

def log(m, newline='\n'):
    sys.stderr.write(m + newline)
    sys.stderr.flush()

dir_type_rec = {
    'chunks': 0,
    'raw': 0,
    'cleansed': 0,
    'ner': 0,
    'size': 0,
    'sq_size': 0,
    'size_ner': 0,
    'sq_size_ner': 0,
    }

def make_content_item(ci):
    '''
    converts an in-memory content item generated from deserializing a
    json content-item into a thrift ContentItem
    '''
    ## return empty content item if received None
    if ci is None:
        return ContentItem()
    ## convert the cleansed and ner to byte arrays, or None
    cleansed = ci.pop('cleansed', b'') ## default to empty byte array
    if cleansed: cleansed = cleansed.decode('string-escape')
    ner = ci.pop('ner', b'') ## default to empty byte array
    if ner: ner = ner.decode('string-escape')
    ## construct a ContentItem and return it
    return ContentItem(
        raw = ci.pop('raw').decode('string-escape'),
        encoding = ci.pop('encoding'),
        cleansed = cleansed,
        ner = ner)

def transform_dir(gpg_key_path, json_dir, thrift_dir, date_hour, doc_limit=None):
    '''
    reads all files from json_dir, transforms to thrifts, compresses,
    encrypts, and saves them in thrift_dir

    records a stats.json file in thrift_dir, for verification and stats
    '''
    ## record start time for rate logging at end
    start_time = time.time()
    file_count = 0.
    line_count = 0.

    ## make sure date_hour is the expected format
    assert re.match('^201\d-\d{2}-\d{2}-\d{2}$', date_hour), \
        'wrong format date_hour: %r' % date_hour

    ## assemble a stats data record
    stats = {
        'date_hour': date_hour,
        'news':     copy.deepcopy(dir_type_rec),
        'linking':  copy.deepcopy(dir_type_rec),
        'social':   copy.deepcopy(dir_type_rec)
        }

    ## prepare to write files an a temp version of thrift_dir for atomic
    ## rename at end
    out_dir = os.path.join(thrift_dir, date_hour)
    tmp_out_dir = out_dir + '.partial'

    ## we write the stats data to a file every pass through the write
    ## loop, see two-stage commit below
    stats_path = os.path.join(tmp_out_dir, 'stats.json')
    tmp_stats_path = stats_path + '.partial'

    ## this list carries the state of whether we were forcibly restarted by condor
    restart_fnames = []
    if os.path.exists(tmp_out_dir):
        ## this condor job just got restarted, so gracefully pickup
        ## where we left off:
        print "restarting!"
        for existing_fname in os.listdir(tmp_out_dir):
            if existing_fname.endswith('partial'):
                ## this file was partially written, so delete it
                os.unlink(os.path.join(tmp_out_dir, existing_fname))
                print "found partial file: %s" % existing_fname
            else:
                restart_fnames.append(existing_fname)
        print "using %d restart_fnames" % len(restart_fnames)
        if os.path.exists(tmp_stats_path):
            ## we got killed in the middle of the two-stage commit.
            ## Wow.  Must restart whole process for this dir.
            restart_fnames = []
        else:
            ## reload the stats data, which should be current with the
            ## restart_fnames list
            assert os.path.exists(stats_path), "missing %s" % stats_path
            stats = json.load(open(stats_path))
            print "loaded stats and working forward now"
    else:
        os.makedirs(tmp_out_dir)

    ### setup gpg for encryption
    gpg_dir = out_dir + '.gpg-dir'
    if not os.path.exists(gpg_dir):
        os.makedirs(gpg_dir)
    gpg_child = subprocess.Popen(['gpg', '--no-permission-warning', '--homedir', gpg_dir, '--import', gpg_key_path], stderr=subprocess.PIPE)
    s_out, errors = gpg_child.communicate()
    if errors:
        log("gpg prints to stderr, even when nothing is wrong, read carefully:\n\n%s" % errors)

    ## iterate over all files of in_dir
    in_dir = os.path.join(json_dir, date_hour)
    input_fnames = os.listdir(in_dir)
    input_fnames.sort()
    ## if we are restarting, cut off the beginning of the list
    input_fnames = input_fnames[len(restart_fnames):]
    for fname in input_fnames:
        ## count all files processed
        file_count += 1

        ## for debugging, we might have set --limit on the command line
        if doc_limit and doc_limit < file_count:
            break

        ## Make output file obj for thrifts, wrap in protocol
        transport = StringIO()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        ## iterate over gzip'ed file of JSON lines
        fpath = os.path.join(in_dir, fname)
        data = gzip.GzipFile(fileobj=open(fpath, 'rb'), mode='rb').read()
        for line in data.splitlines():
            ## count all lines processed
            line_count += 1

            try:
                doc = json.loads(line)
            except Exception, exc:
                log(traceback.format_exc(exc))
                sys.exit('died on: %r' % line)

            ### gather stats
            ## size of this single jsonline, including newline
            size = len(doc['body']['raw'].decode('string-escape'))
            ## source should be one of the known types
            assert doc['source'] in ['news', 'linking', 'social'], \
                'what subcorpus is %r?' % doc['source']
            stats_rec = stats[doc['source']]
            ## record generic counts, all docs have these three
            stats_rec['raw'] += 1
            stats_rec['size'] += size
            stats_rec['sq_size'] += size**2
            ## record special counts for docs with 'cleansed' and 'ner'
            if 'cleansed' in doc['body']:
                stats_rec['cleansed'] += 1
            if 'ner' in doc['body']:
                size_ner = len(doc['body']['ner'].decode('string-escape'))
                stats_rec['ner'] += 1
                stats_rec['size_ner'] += size_ner
                stats_rec['sq_size_ner'] += size_ner**2
            ### done with stats

            ## get the three possible ContentItems
            body   = make_content_item(doc.pop('body'))
            title  = make_content_item(doc.pop('title', None))
            anchor = make_content_item(doc.pop('anchor', None))

            ## assemble the stream_time
            stream_time = StreamTime(
                epoch_ticks = doc['stream_time']['epoch_ticks'],
                zulu_timestamp = doc['stream_time']['zulu_timestamp'])

            ## assemble source_metadata
            source_metadata = json.dumps(doc.pop('source_metadata'))

            ## construct a StreamItem
            stream_item = StreamItem(
                doc_id          = doc['doc_id'],
                abs_url         = bytes(doc['abs_url'].encode('utf-8')),
                schost          = doc['schost'],
                original_url    = doc['original_url'] and bytes(doc['original_url'].encode('utf-8')) or b'',
                source          = doc['source'],
                title           = title,
                body            = body,
                anchor          = anchor,
                source_metadata = source_metadata,
                stream_id       = doc['stream_id'],
                stream_time     = stream_time,
                )

            ## write out the stream_item to protocol, which wraps the
            ## StringIO created above
            stream_item.write(protocol)

        ## done creating thrifts
        ## seek to start of sio, get the thrifts bytes, and compress
        transport.seek(0)
        thrifts_bytes = transport.getvalue()

        ## speak to xz over pipes as a child process, because python
        ## bindings to liblzma are... insufficient.
        # use subprocess.Popen and its commuicate method to avoid
        # blocking the child's stdin while waiting for us to read
        # stdout
        xz_child = subprocess.Popen(
            ['xz', '--compress', '-6'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        compressed_bytes, errors = xz_child.communicate(thrifts_bytes)
        log("closed input to xz")

        ## catch anything from xz's stderr
        assert not errors, errors

        ## compute md5 of uncompressed bytes
        content_md5 = hashlib.md5(thrifts_bytes).hexdigest()

        ## free memory
        thrifts_bytes = None

        ## encrypt using the fingerprint for our trec-kba-rsa key pair
        gpg_child = subprocess.Popen(
            ## setup gpg to encrypt with trec-kba public key
            ## (i.e. make it the recipient), with zero compression,
            ## ascii armoring is off by default, and --output - must
            ## appear before --encrypt -
            ['gpg',  '--no-permission-warning', '--homedir', gpg_dir, '-r', 'trec-kba', '-z', '0', '--trust-model', 'always', '--output', '-', '--encrypt', '-'],
            stdin =subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        ## communicate with child via its stdin
        encrypted_bytes, errors = gpg_child.communicate(compressed_bytes)
        assert not errors, errors

        ## free memory
        compressed_bytes = None

        ## construct output filename
        subcorpus, _md5, _gz = fname.split('.')
        assert subcorpus in ['news', 'linking', 'social'], subcorpus
        out_fname = '%s.%s.xz.gpg' % (subcorpus, content_md5)

        ## record chunk count for stats of this subcorpus in this dir
        stats[subcorpus]['chunks'] += 1

        ## save it to temp file called .partial, then do an atomic move to
        ## avoid partial files on crashes
        out_fpath = os.path.join(tmp_out_dir, out_fname)
        tmp_out_fpath = out_fpath + '.partial'
        fh = open(tmp_out_fpath, 'wb')
        ## must call str on encrypted_bytes object to get ascii string
        fh.write(encrypted_bytes)
        fh.close()

        ## begin two-stage commit.  write intermediate stats file with
        ## .partial before rename of real data file
        print "initiating two-stage commit"
        open(tmp_stats_path, 'wb').write(json.dumps(stats))

        ## atomic move of fully written file
        os.rename(tmp_out_fpath, out_fpath)

        ## atomic move of stats file -- as ack that we completed full
        ## loop. If we ever see a stats.json.partial file on restart,
        ## we have to restart the full directory...
        os.rename(tmp_stats_path, stats_path)

        print "completed two-stage commit"

        ## loop to next jsonlines file

    ## atomic move of tmp_out_dir to out_dir
    os.rename(tmp_out_dir, out_dir)

    ## log some speed stats
    elapsed = time.time() - start_time
    line_rate = float(line_count) / elapsed
    sec_per_file = elapsed / file_count
    log('finished %s processed %d JSON lines from %d files in %.1f sec --> %.1f lines/sec, %.1f sec/file' % (
            date_hour, line_count, file_count, elapsed, line_rate, sec_per_file))

def decrypt_and_verify(gpg_key_path, thrift_dir, date_hour, decrypt=False):
    '''
    reads in the compressed-&-encrypted thrifts of thrift_dir and
    compares their counts to thrift_dir/stats.json

    If decrypt is True, then save the decrypted files alongside the
    unmodified predecessor files.
    '''
    ### setup gpg for encryption
    gpg_dir = date_hour + '.gpg-dir'
    if not os.path.exists(gpg_dir):
        os.makedirs(gpg_dir)
    gpg_child = subprocess.Popen(['gpg', '--no-permission-warning', '--homedir', gpg_dir, '--import', gpg_key_path], stderr=subprocess.PIPE)
    s_out, errors = gpg_child.communicate()
    if errors:
        log("gpg prints to stderr, even when nothing is wrong, read carefully:\n\n%s" % errors)

    stats = json.load(open(os.path.join(thrift_dir, date_hour, 'stats.json')))
    print '## valid thrifts for:\n' + json.dumps(stats, indent=4)

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
            ['gpg',   '--no-permission-warning', '--homedir', gpg_dir, '--trust-model', 'always', '--output', '-', '--decrypt', '-'],
            stdin =subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        ## communicate with child via its stdin
        compressed_data, errors = gpg_child.communicate(encrypted_data)
        if errors:
            log(errors)

        ## save the compressed and decrypted data:
        if decrypt:
            parts = fname.split('.')
            assert 'gpg' == parts.pop()
            open(os.path.join(thrift_dir, date_hour, '.'.join(parts)), 'wb').write(compressed_data)

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

        ## wrap it in a file obj, thrift transport, and thrift protocol
        transport = StringIO(thrift_data)
        transport.seek(0)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        ## iterate over all thrifts and decrement stats
        while 1:
            stream_item = StreamItem()
            try:
                stream_item.read(protocol)
            except EOFError:
                break

            ## dump data
            #stats[stream_item.source]['cleansed'] -= len(stream_item.body.cleansed)
            print stream_item.doc_id
            print stream_item.abs_url
            print stream_item.original_url
            print stream_item.body.cleansed
            print '\n\n'

            ## ensure that json source_metadata can still be decoded
            source_metadata = json.loads(stream_item.source_metadata)
            if stream_item.source == 'news':
                assert 'language' in source_metadata

        ## close that transport
        transport.close()

if __name__ == '__main__':
    try:
        import argparse
    except:
        sys.path.append('/data/zfs-scratch/kba/tools/KBA/2012-corpus/argparse-1.2.1/build/lib.linux-x86_64-2.6')
        import argparse
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument('gpg_key_path')
    parser.add_argument('thrift_dir')
    parser.add_argument('date_hour')
    parser.add_argument('--transform', metavar='DIR', dest='json_dir', default=None, help='dir of files of JSON lines to transform into thrift_dir/date_hour/<source>.<md5>.xz.gpg.  creates thrift_dir/stats.json')
    parser.add_argument('--limit', metavar='MAX', dest='doc_limit', default=None, type=int, help='only transform up to MAX docs (for debugging purposes only)')
    parser.add_argument('--verify',  dest='verify',  action='store_true', default=False, help='verify that files in thrift_dir/date_hour/* agree with thrift_dir/stats.json')
    parser.add_argument('--decrypt', dest='decrypt', action='store_true', default=False, help='save the decrypted files (does not delete .gpg files, so doubles disk space used)')
    args = parser.parse_args()

    if args.json_dir:
        transform_dir(args.gpg_key_path, args.json_dir, args.thrift_dir, args.date_hour, doc_limit=args.doc_limit)

    elif args.verify:
        decrypt_and_verify(args.gpg_key_path, args.thrift_dir, args.date_hour, args.decrypt)
