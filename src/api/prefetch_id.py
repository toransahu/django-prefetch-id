#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
# created_on: 2018-08-12 21:57

"""
prefetch_id.py
"""

from uuid import uuid4
from django.db import connection
from django.conf import settings


__author__ = 'Toran Sahu  <toran.sahu@yahoo.com>'
__license__ = 'Distributed under terms of the AGPL license.'


def prefetch_id(instance):
    """
    Prefetch (fetches next) sequence for particular model from different databases.
    :param instance: <instance> of Model
    :return: <int> sequence id
    """
    db_engine = connection.settings_dict['ENGINE']

    if db_engine == 'django.db.backends.sqlite3':
        cursor = connection.cursor()
        table_name = instance._meta.app_label.lower() + '_' + instance._meta.object_name.lower()
        cursor.execute(f"select seq from sqlite_sequence where name='{table_name}'")
        row = cursor.fetchone()
        cursor.close()
        # if sqlite_sequence table has record for the table_name
        if row is not None:
            seq_id = int(row[0])
        # else if its first record insertion in the table_name
        else:
            seq_id = 0
        return seq_id + 1
    elif db_engine == 'django.db.backends.mysql':
        cursor = connection.cursor()
        # database_name = 'ethereal_machines'
        database_name = settings.DATABASES['default']['NAME']
        table_name = instance._meta.app_label.lower() + '_' + instance._meta.object_name.lower()
        cursor.execute(
            f"select auto_increment from information_schema.tables where auto_increment is not null and table_schema = '{database_name}' and table_name = '{table_name}'")
        row = cursor.fetchone()
        cursor.close()
        seq_id = int(row[0])
        return seq_id
    elif db_engine == 'django.db.backends.postgresql_psycopg2':
        cursor = connection.cursor()
        cursor.execute(
            "SELECT nextval('{0}_{1}_id_seq'::regclass)".format(
                instance._meta.app_label.lower(),
                instance._meta.object_name.lower(),
            )
        )
        row = cursor.fetchone()
        cursor.close()
        return int(row[0])
    elif db_engine == 'django.db.backends.oracle':
        pass
    else:
        raise

def directory_path_with_id(instance, filename):
    """
    Return media path based on MEDIA_ROOT/ModelName/UUID.EXT
    :param instance:
    :param filename:
    :return:
    """

    # if the directory path is going to be used while insert
    if instance.id is None:
        current_id = prefetch_id(instance)
    # else if the directory path is going to be used while update
    else:
        current_id = instance.id
    try:
        file, ext = filename.rsplit('.',1)
        return str.lower(f'{instance._meta.app_label}/{instance._meta.object_name}/{current_id}/{uuid4()}.{ext}')
    except:
        return str.lower(f'{instance._meta.app_label}/{instance._meta.object_name}/{current_id}/{filename}')
