#!/usr/bin/env python
# -*- coding: utf-8 -*-


# The configuration of redis db

class RedisDB(object):
    host = "localhost"
    port = 6381

    rel_ent_dist_db = 0
    #rel_ent_dist_db = 1

    test_db = 2

    oair_doc_train_db = 10
    oair_doc_test_db = 11

    # specifications related to bootstraping
    query_ent_list = 'query_ent_list'
    query_ent_hash = 'query_ent_hash'
    wiki_ent_set = 'wiki_ent_set'
    ret_item_list = 'ret_item_list'

    # mutex
    async_mutex = 'async_mutex'
