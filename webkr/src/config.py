#!/usr/bin/env python
# -*- coding: utf-8 -*-


# The configuration of redis db

class RedisDB(object):
    host = "localhost"
    port = 6500

    ent_db = 0
    qrels_db = 4
    train_greedy_db = 5
    test_greedy_db = 6

    # for temporal pattern inspection
    temp_db = 1

    # all the documents come from Balog's list published with OAIR paper
    train_edmap_db = 7
    test_edmap_db = 8
    train_doc_db = 10
    test_doc_db = 11

    # specifications related to bootstraping
    query_ent_list = 'query_ent_list'
    query_ent_hash = 'query_ent_hash'
    ret_item_list = 'ret_item_list'

