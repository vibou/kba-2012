#!/usr/bin/env python
# -*- coding: utf-8 -*-


# The configuration of redis db

class RedisDB(object):
    host = "localhost"
    port = 6379

    # specifications of which DB to use for different purposes
    eval_db = 0
    exact_match_db = 1

    # specifications of hash tables
    # the to-be-processed tweet list
    raw_query_list = 'raw_query_list'
    formatted_query_list = 'formatted_query_list'

    # specifications related to bootstraping
    ret_item_list = 'ret_item_list'

    # mutex
    async_mutex = 'async_mutex'
