#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
# created_on: 2018-08-12 23:51

"""
exception.py.py
"""


__author__ = 'Toran Sahu  <toran.sahu@yahoo.com>'
__license__ = 'Distributed under terms of the AGPL license.'


class InvalidDBEngine(BaseException):
    raise Exception("Invalid DB Engine.")
