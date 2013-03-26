#!/usr/bin/env python
# -*- coding: utf-8 -*-


# The configuration of redis db

class RedisDB(object):
    host = "localhost"
    port = 6380

    # specifications of which DB to use for different purposes
    eval_db = 0
    train_exact_match_db = 1
    #fuzzy_match_db = 2

    #fuzzy_match_db = 23
    fuzzy_match_db = 25

    oair_train_db = 27
    oair_test_db = 24

    #wiki_match_db = 5
    #wiki_match_db = 6
    wiki_match_db = 26
    new_wiki_match_db = 7

    wiki_ent_list_db = 13

    test_exact_match_db = 8

    analyze_wiki_match_db = 9

    missed_docs_db = 10

    rel_ent_dist_db = 12
    #rel_ent_dist_db = 14

    filtered_train_db = 17
    filtered_test_db = 18

    #wiki_ent_dist_db = 20
    wiki_ent_dist_db = 21
    train_wiki_ent_dist_db = 22

    # specifications of hash tables
    # the to-be-processed tweet list
    raw_query_list = 'raw_query_list'
    formatted_query_list = 'formatted_query_list'

    # specifications related to bootstraping
    ret_item_list = 'ret_item_list'
    wiki_ent_list = 'wiki_ent_list'
    query_ent_list = 'query_ent_list'
    wiki_ent_set = 'wiki_ent_set'

    # mutex
    async_mutex = 'async_mutex'
