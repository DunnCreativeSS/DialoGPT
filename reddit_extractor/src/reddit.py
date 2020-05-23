#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Microsoft Corporation. 
#  Licensed under the MIT license. 

import sys
import time
import os.path
import math
import re
import argparse
import traceback
import json
import bz2
import gzip
from nltk.tokenize import TweetTokenizer
from flashtext import KeywordProcessor
import hashlib

def makedirs(fld):
    if not os.path.exists(fld):
        os.makedirs(fld)

PICKLE_MAX_LEN = 1e4
TAG_COMMENT = 't1_'
TAG_SUBMISSION = 't3_'
dontuse = '__dontuse__'
url_str = '__url__'

parser = argparse.ArgumentParser()

parser.add_argument("dump_name", help="YYYY-MM, dumped files to be loaded")
parser.add_argument("--bl_words", help="list of offensive words, to avoid in responses")
parser.add_argument("--ignore_keys", default=False, type=bool, help="If true ignore any keys provided as arguments")
parser.add_argument("--keep_keys", help="hashes of instances to keep")
parser.add_argument("--discard_tgt_keys", help="hashes of targets to discard")
parser.add_argument("--freq_words", help="words sorted by their corpus frequencies")
parser.add_argument("--bl_subreddits", help="blocklist of offensive subreddits")
parser.add_argument("--wl_subreddits", help="whitelist of relatively safe subreddits")
parser.add_argument("--reddit_input", default="d:/data/reddit/bz2/", help="Location of the input reddit data (bz2 files)")
parser.add_argument("--reddit_output", default="d:/data/reddit/", help="Location of the output reddit data (conversations)")
parser.add_argument("--max_len", default=30, type=int)
# 30 words means roughly 70 characters on average for Reddit
parser.add_argument("--max_len_type", default='w') # w for words, c for chars
parser.add_argument("--min_depth", default=2, type=int)
parser.add_argument("--max_depth", default=10, type=int)
parser.add_argument("--min_score", default=0, type=int)
parser.add_argument("--use_title", default=1, type=int)
parser.add_argument("--leaves_only", default=0, type=int)
parser.add_argument("--split_size", default=int(5e5), type=int)
parser.add_argument("--task", default='conv')
parser.add_argument("--parallel", default=False, type=bool)
parser.add_argument("--pre_tok", default=False, type=bool, help="whether to tokenize during the extract step")
parser.add_argument("--clean", default=False, type=bool, help="apply some filters to significantly reduce number of instances")

args = parser.parse_args()
print("Args: %s" % args, file=sys.stderr)

fields_subm = [ "id", "score", "num_comments", "domain", "permalink", "title" ]
fields_comm = [ "id", "author", "parent_id", "link_id", "score", "n_char", "body"]

bl_words = KeywordProcessor()
bl_subreddits = {}
wl_subreddits = ['EroticWriting', '/r/EroticWriting', 'eroticauthors', 'erotica', '/r/eroticauthors', '/r/erotica', 'gonewildstories', '/r/gonewildstories', 'sluttyconfessions', '/r/sluttyconfessions']
keys = {}
keys_rm = {}

def jareprint(string):
    if fld_out is not None:
        with open(fld_out + '/%s.tsv.gz.logtemp'%args.dump_name, 'a', encoding="utf-8") as f:
            f.write(str(string) + '\n')
    print(string)
def get_submission_id(submission):
    return TAG_SUBMISSION + submission["id"]


def get_comment_id(comment):
    return TAG_COMMENT + comment["id"]


def norm_sentence(txt, is_extract):
    if is_extract:
        return minimal_norm_sentence(txt)
    else:
        return gpt_norm_sentence(txt)


def minimal_norm_sentence(txt):
    txt = txt.replace(chr(92),'') # chr(92) = '\'. as twitter has 'b\/c' rather than 'b/c'
    txt = txt.replace('\n', ' ')
    txt = txt.replace('\r', ' ')
    txt = txt.replace('\t', ' ')
    #print ("Tokenized: [%s]" % txt, file=sys.stderr)
    return txt


def gpt_norm_sentence(txt):
    # url and tag
    words = []
    for word in txt.split():
        if word[0] == '#': # don't allow tag
            continue
        i = word.lower().find('http')
        if i >= 0:
            word = word[:i] + ' ' + '__url__'
        words.append(word.strip())
    txt = ' '.join(words)

    # remove illegal char
    txt = txt.replace(chr(92),'') # chr(92) = '\'. as twitter has 'b\/c' rather than 'b/c'
    txt = txt.replace("b/c","because").replace('j/k','just kidding').replace('w/o','without').replace('w/','with')
    txt = re.sub('__mention__','MENTION',txt)
    txt = re.sub('__url__','URL',txt)
    txt = re.sub(r"[^A-Za-z0-9()\[\]:,.!?'“” ]", " ", txt)
    txt = re.sub('MENTION','__mention__',txt)
    txt = re.sub('URL','__url__',txt)

    tokenizer = TweetTokenizer(preserve_case=True)
    txt = ' ' + ' '.join(tokenizer.tokenize(txt)) + ' '

    # remove un-necessary space
    return ' '.join(txt.split())


def extract_submissions(fld_bz2, fld_split, size=2e5):
    path_in = fld_bz2 + '/RS_%s.bz2'%args.dump_name
    n = 0
    m = 0
    sub = 0
    sid = []
    sids = []
    lines = []
    with bz2.open(path_in, 'rt', encoding="utf-8") as f:
        for line in f:
            n += 1
            if n%1e4 == 0:
                jareprint('[%s] selected %.3fM from %.2fM submissions'%(
                    args.dump_name, m/1e6, n/1e6))
            try:
                submission = json.loads(line)
                if int(submission['num_comments']) < 2: # filter 1
                    continue
                submission['title'] = norm_sentence(submission['title'], True)
                lines.append('\t'.join([str(submission[k]) for k in fields_subm]))
                m += 1
                sid.append(get_submission_id(submission))

            except Exception:
                traceback.print_exc()
                continue

            if len(sid) == size:
                jareprint('writing submissions_sub%i'%sub)
                sids.append(set(sid))
                with open(fld_split + '/rs_sub%i.tsv'%sub, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))
                sid = []
                lines = []
                sub += 1

    jareprint('writing submissions_sub%i'%sub)
    sids.append(set(sid))
    with open(fld_split + '/rs_sub%i.tsv'%sub, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    jareprint('extract_submissions done.\n')
    return sids, m, n


def extract_comments(fld_bz2, fld_split, sids):
    path_in = fld_bz2 + '/RC_%s.bz2'%args.dump_name
    n = 0
    m = 0
    n_sub = len(sids)
    lines = [[] for i in range(n_sub)]
    for sub in range(n_sub):
        open(fld_split + '/rc_sub%i.tsv'%sub, 'w')

    with  open(path_in, 'rt', encoding="utf-8") as f:
        for line in f:
            n += 1
            if n%1e4 == 0:
                jareprint('[%s] selected %.3fM from %.2fM comments'%(
                    args.dump_name, m/1e6, n/1e6))

                for sub in range(n_sub):
                    jareprint('    sub %i: %i'%(sub, len(lines[sub])))
                    if len(lines[sub]) > 0:
                        with open(fld_split + '/rc_sub%i.tsv'%sub, 'a', encoding='utf-8') as f:
                            f.write('\n'.join(lines[sub]) + '\n')
                        lines[sub] = []
            try:
                comment = json.loads(line)
                if args.keep_keys:
                    k = '\t'.join([comment['link_id'], get_comment_id(comment), 'dep'])
                    if k not in keys.keys():
                        continue
                if comment['body'] == '[deleted]': # filter 1
                    continue
                if '>' in comment['body'] or '&gt;' in comment['body']: # filter 3: '&gt;' means '>'
                    continue
                sid = comment['link_id']
                for sub in range(n_sub):
                    if sid in sids[sub]:
                        comment['n_char'] = len(comment['body'])
                        comment['body'] = norm_sentence(comment['body'], True)
                        if len(comment['body'].split()) < 2: # filter 2
                            break
                        lines[sub].append('\t'.join([str(comment[k]) for k in fields_comm]))
                        m += 1
                        break

            except Exception:
                traceback.print_exc()

    jareprint('the rest...')
    for sub in range(n_sub):
        jareprint('    sub %i: %i'%(sub, len(lines[sub])))
        with open(fld_split + '/rc_sub%i.tsv'%sub, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines[sub]))

    jareprint('extract_comments done.\n')
    return m, n


def get_convo(sid, rootid, cid, submissions, comments, index, depth=args.max_depth):
    if depth == 0:
        return []
    c = comments[cid]
    if args.max_len_type == 'w' and len(c['body'].split()) > args.max_len: # len filter
        return []
    if args.max_len_type == 'c' and int(c['n_char']) > args.max_len:
        return []

    pid = c['parent_id']
    if args.use_title and pid.startswith(TAG_SUBMISSION):
        txts = [ "title: " + submissions[c['link_id']]['title'] ]
    elif pid in comments:
        txts = get_convo(sid, rootid, pid, submissions, comments, index, depth-1)
    else:
        txts = []
    txts.append(c['body'])
    return txts


def filter_instance(src, tgt, info):
    # Remove offensive words:
    if args.bl_words and not args.leaves_only:
        bad_words = bl_words.extract_keywords(tgt)
        if bad_words:
            jareprint("skip\toffensive\t%s\t%s\tbad word(s): %s" % (info, tgt, bad_words), file=sys.stderr)
            return True

    # Remove empty targets:
    tgttoks = tgt.split()
    if len(tgttoks) <= 1: # 1 means there is only a weight, and 0 means there's a bug..
        jareprint("skip\temptytarget\t%s\t%s" % (info, tgt), file=sys.stderr)
        return True

    # Skip if word too long:
    toolong = False
    for w in tgttoks:
        if len(w) > 30:
            toolong = True
            break
    if toolong:
        jareprint("skip\tlongword\t%s\t%s\tword too long" % (info, tgt), file=sys.stderr)
        return True

    srctoks = src.split()
    # Remove empty sources: (should probably uncomment, but left for reproducibility)
    #if len(srctoks) <= 1: # 1 means there is only a weight, and 0 means there's a bug..
    #    jareprint("skip\temptysource\t%s\t%s" % (info, src), file=sys.stderr)
    #    return True

    # Remove too long turns:
    nsrctgt = len(srctoks) + len(tgttoks)
    if nsrctgt > 200:
        jareprint("skip\ttoolong\t%s\t%s\tsrc+tgt too long, src=[%s]" % (info, tgt, src), file=sys.stderr)
        return True

    # Skip turns with URLs:
    srctgt = src + " " + tgt
    if "__url__" in srctgt:
        jareprint("skip\turl\t%s\t%s\turl in tgt, or src =[%s]" % (info, tgt, src), file=sys.stderr)
        return True

    # Skip responses with meta data:
    if re.search("[\[\]\(\)]", srctgt) != None:
        jareprint("skip\ttags\t%s\t%s\ttag in tgt (or src: [%s])" % (info, tgt, src), file=sys.stderr)
        return True

    # Skip yelling:
    if re.search("[A-Z]{5,}", srctgt) != None:
        jareprint("skip\tallcaps\t%s\t%s\tall caps in tgt (or src: [%s])" % (info, tgt, src), file=sys.stderr)
        return True

    # Skip word repetitions:
    reps = False
    for i in range(2, len(tgttoks)):
        if tgttoks[i-2] == tgttoks[i] and tgttoks[i-1] == tgttoks[i]:
            reps = True
            break
    if reps:
        jareprint("skip\trepetitions\t%s\t%s\ttoo many repetitions" % (info, tgt), file=sys.stderr)
        return True

    return False

import datetime
import sys
import requests
from time import sleep
import random
def getthecomments(lala, submission, index):
    print("https://api.pushshift.io/reddit/search/comment/?subreddit=" + lala + "&size=500&parent_id="+submission['id'])
    try:    
        resp = session.get("https://api.pushshift.io/reddit/search/comment/?subreddit=" + lala + "&size=500&parent_id="+submission['id'])
        jareprint(resp)
        if '200' in str(resp):
            sleep(random.randint(0, 3))
            comments = []
            try:
                resp = resp.json()['data']
                for line in resp:
                
                    comment = line
                    if index == 1:
                        jareprint(comment)
                    comments.append(comment)
                return(comments)
            except Exception as e:
                jareprint(e)
                if '429' not in str(resp) and '502' not in str(resp):
                    blocked.append(lala)
                    going = False
                else:
                    time.sleep(random.randint(1,5))
                    return getthecomments(lala, submission, index)
                traceback.print_exc()
                
    except Exception as e:
        jareprint(e)
        traceback.print_exc()
        

from pypac import PACSession, get_pac
pac = get_pac(url='http://localhost/proxies.PAC')
session = PACSession(pac)
blocked = []
def dogetsubmissions(ts, lala, ts2, going, submissions, comments,   index):
    jareprint("https://api.pushshift.io/reddit/search/submission/?subreddit=" + lala + "&size=500&before=" + str(ts2) + "&after="+str(ts)+"&fields=created_utc,id,score,num_comments,domain,permalink,title&score=>1&num_comments=>1")
                
    try:
        if ts != 0:
            resp = session.get("https://api.pushshift.io/reddit/search/submission/?subreddit=" + lala + "&size=500&before=" + str(ts2) + "&after="+str(ts)+"&fields=created_utc,id,score,num_comments,domain,permalink,title&score=>1&num_comments=>1", timeout=10)
        else:
            resp = session.get("https://api.pushshift.io/reddit/search/submission/?subreddit=" + lala + "&size=500&before=" + str(ts2) + "&fields=created_utc,id,score,num_comments,domain,permalink,title&score=>1&num_comments=>1", timeout=10)
        
        jareprint(resp)
        if '200' in str(resp): 
            try:
                resp = resp.json()['data']
                if len(resp) == 0:
                    blocked.append(lala)
                    going = False
                for line in resp:
                    submission = line
                    if index == 1:
                        jareprint(submission)
                    ts2o = ts2   
                 
                    ts2 = submission['created_utc']
                    ts = 0
                    if ts2 > ts2o or ts2 < ts:
                        going = False
                    submissions[get_submission_id(submission)] = submission
                    sleep(random.randint(0, 3))
                    toappend = getthecomments(lala, submission, index)
                    for comment in toappend:
                        comments[get_comment_id(comment)] = comment
                
            except Exception as e:
                if '429' not in str(e) and '502' not in str(e) and 'proxy' not in str(e) and 'timed out' not in str(e):
                    blocked.append(lala)
                    going = False
                    jareprint(e)
                else:
                    time.sleep(random.randint(1,5))
                    return dogetsubmissions(ts, lala, ts2, going, submissions, comments, index)
                traceback.print_exc()
    except Exception as e:
        if '429' not in str(e) and '502' not in str(e) and 'proxy' not in str(e) and 'timed out' not in str(e):
            blocked.append(lala)
            going = False
            jareprint(e)
        else:
            time.sleep(random.randint(1,5))
            return dogetsubmissions(ts, lala, ts2, going, submissions, comments, index)
        traceback.print_exc()
    return({'going': going, 'submissions': submissions, 'comments': comments})
            

  
def save_convo(path_rs, path_rc, path_out):
    jareprint('reading submissions...')
    path_out = fld_out + '/%s.tsv'%args.dump_name
    wl_subreddits = ['nsfw', 'porn', 'eroticwriting',  'eroticauthors', 'erotica','gonewildstories', 'sluttyconfessions']

    date_time_str = args.dump_name
    date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m')
    ts = datetime.datetime.timestamp(date_time_obj)
    ts2 = date_time_obj + datetime.timedelta(days=28)
    ts = int(ts) 
    ts2 = int(datetime.datetime.timestamp(ts2))
    submissions = dict()
    index = 0
   
    jareprint('reading comments...')
    comments = dict()
    index = 0
    i = 0
    m = 0
    n = 0
    lines = []
    sum_resp_len = 0
    for lala in wl_subreddits:
        jareprint(index)
        index = index + 1
        going = True
        while going == True:
            if index == len(wl_subreddits):
                going = False

            if lala not in blocked:
                
                sleep(random.randint(0, 3))
                subresult = dogetsubmissions(ts, lala, ts2, going, dict(), dict(), index)  
                print(subresult)
                going = subresult['going']
                submissions = subresult['submissions']
                comments = subresult['comments']
            else:
                going = False                  
        sorted_id = sorted([(
                        comments[cid]['link_id'],
                        comments[cid]['parent_id'],
                        cid
                        ) for cid in comments])

        n = len(comments)
        jareprint('total comments: %i'%n)

        

        skip_id = {}
        if args.leaves_only:
            for _, pid, _ in sorted_id:
                skip_id[pid] = 1

        for sid, pid, cid in sorted_id:
            if args.keep_keys:
                k = '\t'.join([sid, cid, 'keep'])
                
            
            i += 1
            if i%1e5 == 0:
                jareprint('selected %.2fM from %.1f/%.1fM comments'%(m/1e6, i/1e6, n/1e6), file=sys.stderr)
                if len(lines) > 0:
                    with open(path_out, 'a', encoding="utf-8") as f:
                        f.write('\n'.join(lines) + '\n')
                lines = []

            subreddit = ''
            domain = ''
            if sid in submissions.keys():
                subreddit = submissions[sid]['permalink'].split('/')[2].lower()
                domain = submissions[sid]['domain'].lower()
            if subreddit in wl_subreddits:
                info = subreddit + '\t' + domain

               
                comment = comments[cid]


                try:
                    txts = get_convo(sid, cid, cid, submissions, comments, index) # filter 2
                except Exception:
                    jareprint("skip\texception\t%s\t%s\texception" % (info, comment['body']), file=sys.stderr)
                    
                if len(txts) < 3: # filter 3
                    jareprint("skip\tmin_depth\t%s\t%s\tdepth %d < %d: %s" % (info, comment['body'], len(txts), args.min_depth, "|".join(txts)), file=sys.stderr)
                    

                for i in range(len(txts)):
                    txts[i] = norm_sentence(txts[i], False)
                    if args.leaves_only and args.clean:
                        sc = '1.0'
                        skip_target = False
                        if args.discard_tgt_keys:
                            tgt_h = hashlib.sha224(txts[i].encode("utf-8")).hexdigest()
                            if tgt_h in keys_rm.keys():
                                skip_target = True
                        if bl_words.extract_keywords(txts[i]) or skip_target:
                            sc = '0.0'
                        txts[i] = sc + ' ' + txts[i]

                src = ' EOS '.join(txts[:-1])
                tgt = txts[-1]

                

                header = ','.join([sid, pid, cid])
                lines.append(header + '\t' + src + '\t' + tgt)
                sum_resp_len += len(tgt.split())
                m += 1
                        
                        
                

                

    avg_len = sum_resp_len/(m+1)
    with open(path_out, 'a', encoding="utf-8") as f:
        f.write('\n'.join(lines) + '\n')
    jareprint('finally selected %i/%i, avg len = %.2f'%(m, n, avg_len))
    return m, n, avg_len


def extract():
    makedirs(fld_split)
    sids, ms, ns = extract_submissions(fld_root_in, fld_split, size=args.split_size)
    mc, nc = extract_comments(fld_root_in, fld_split, sids)
    with open(fld_split + '/stat.tsv', 'a') as f:
        f.write('\t'.join(map(str, [args.dump_name, mc, nc, ms, ns])) + '\n')


def build_conv(fld_out):
    makedirs(fld_out)
    path_out = fld_out + '/%s.tsv'%args.dump_name
    jareprint(path_out)

    if args.parallel:
        fs = open(fld_out + '/' + args.dump_name + '.stat.tsv', 'w')
    else:
        fs = open(fld_out + '/stat.tsv', 'a')

    sub = 0
    sum_m = 0
    sum_n = 0
    while True:
        path_rs = fld_split + '/rs_sub%i.tsv.gz'%sub
        if not os.path.exists(path_rs):
            if sub == 0:
                jareprint('no such file: '+path_rs)
            break
        jareprint('-'*10 + ' sub%i '%sub + '-'*10)
        path_rc = path_rs.replace('/rs_', '/rc_')
        m, n, avg_len = save_convo(path_rs, path_rc, path_out)
        fs.write('\t'.join([args.dump_name, str(sub), str(m), str(n), '%.2f'%avg_len]) + '\n')
        sum_m += m
        sum_n += n
        sub += 1

    fs.write('\t'.join([args.dump_name, 'all', str(sum_m), str(sum_n), '']) + '\n')
    fs.close()


def load_keys(key_file):
    d = {}
    with gzip.open(key_file, 'rt', encoding="utf-8") as f:
        for line in f:
            k = line.rstrip()
            if args.task == 'conv' and k.endswith('\tdep'):
                continue
            d[k] = 1
    return d


if args.freq_words:
    with open(args.freq_words, 'rt', encoding="utf-8") as f:
        n = 0
        for line in f:
            n += 1
            w = line.rstrip().lower()
            args.freq_words[w] = n

if args.bl_words:
    with open(args.bl_words, 'rt', encoding="utf-8") as f:
        for line in f:
            if line[0] == '#':
                continue
            w = line.rstrip()
            bl_words.add_keyword(w)

if args.bl_subreddits:
    with open(args.bl_subreddits, 'rt', encoding="utf-8") as f:
        for line in f:
            if line[0] == '#':
                continue
            s = line.rstrip().lower()
            bl_subreddits[s] = 1

if args.ignore_keys:
    args.keep_keys = None
    args.discard_tgt_keys = None
else:
    if args.keep_keys:
        keys = load_keys(args.keep_keys)
    if args.discard_tgt_keys:
        keys_rm = load_keys(args.discard_tgt_keys)

fld_root_in = args.reddit_input
fld_root_out = args.reddit_output
fld_split = fld_root_out + '/extract/%s'%(args.dump_name)
fld_out = None
if args.task == 'extract':
    extract()
elif args.task == 'conv':
    fld_out = fld_root_out + '/conv'
    build_conv(fld_out)
else:
    jareprint("Unknown task: %s" % args.task, file=sys.stderr)
