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
from .exception import InvalidDBEngine


__author__ = 'Toran Sahu  <toran.sahu@yahoo.com>'
__license__ = 'Distributed under terms of the AGPL license.'


class DBEngine:
    r"""
    Database Engine Class.
    """

    def __init__(self):
        r"""
        Set DB Engine as Default Database in Django Settings.
        :return: String, DB Engine Name
        """
        self.db_engine = connection.settings_dict['ENGINE']
        return self.db_engine

    def set_db_engine(self, db_engine):
        r"""
        Set DB Engine of your choice.
        :param db_engine: String, DB Engine Name
        :return: String, DB Engine Name
        """
        self.db_engine = db_engine
        return self.db_engine

    def get_db_engine(self):
        r"""
        Return selected DB Engine.
        :return: String, DB Engine Name
        """
        return self.db_engine


class PrefetchID:
    r"""
    Prefetch Model ID.
    """

    def __init__(self):
        r"""
        Initialize with default DB Engine
        """
        self.db_engine = DBEngine()

    def set_db_engine(self, db_engine_obj):
        r"""
        Set DB Engine of your choice.
        :param db_engine: DBEngine Class Object
        :return: String, DB Engine Name
        """
        self.db_engine = db_engine_obj.db_engine
        return self.db_engine

    def prefetch_id(self, model_instance):
        r"""
        Prefetch Model ID by Model Instance (Default).

        :param model_instance:
        :return: Integer, Next Sequence ID/Number for the given Model Instance
        """
        pass

    def prefetch_id_by_table(self, table_name):
        r"""
        Prefetch Model ID by Table Name.

        :param table_name: String, Model Table Name
        :return: Integer, Next Sequence ID/Number for the given Table
        """
        pass




def prefetch_id(instance):
    r"""
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
            f"select auto_increment from information_schema.tables "
            f"where auto_increment is not null and table_schema = '{database_name}' and table_name = '{table_name}'")
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
        raise InvalidDBEngine


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
        file, ext = filename.rsplit('.', 1)
        return str.lower(f'{instance._meta.app_label}/{instance._meta.object_name}/{current_id}/{uuid4()}.{ext}')
    except:
        return str.lower(f'{instance._meta.app_label}/{instance._meta.object_name}/{current_id}/{filename}')
