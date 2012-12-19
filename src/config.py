#!/usr/bin/env python
# -*- coding: utf-8 -*-


# The configuration of redis db

class RedisDB(object):
    host = "localhost"
    port = 6379

    # specifications of which DB to use for different purposes
    eval_db = 0
    exact_match_db = 1

    wiki_match_db = 5
    #wiki_match_db = 6
    new_wiki_match_db = 7

    wiki_ent_list_db = 3

    test_exact_match_db = 8

    analyze_wiki_match_db = 9

    # specifications of hash tables
    # the to-be-processed tweet list
    raw_query_list = 'raw_query_list'
    formatted_query_list = 'formatted_query_list'

    # specifications related to bootstraping
    ret_item_list = 'ret_item_list'
    wiki_ent_list = 'wiki_ent_list'

    # mutex
    async_mutex = 'async_mutex'
