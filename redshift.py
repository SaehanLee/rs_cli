# -*- coding: utf-8 -*-
"""Module for accessing redshift.

Currently has methods for pulling information into pandas and dealing with sql database behind
psycopg2. As well as bulk loading functions that deal with uploading csv's to aws S3 before using
a copy statement to get them into aws redshift.
"""
import boto3
import sys
import time
import tempfile

import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine


REDSHIFT_COPY_CSV = """COPY {tablename}
    FROM 's3://{s3_bucket}/{prefix}'
    credentials 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}'
    CSV
    IGNOREHEADER AS 1
    dateformat AS 'YYYY-MM-DD';"""

def create_pd_dataframe(config, query, dates_cols=None):
  """ Executes a sql query and read the data into a pandas data frame.

  Keyword arguments:
  config -- configuration dictionary- should contain user, pwd, host, post, dbname properties.
  query  -- sql query use to pull data into a dataframe.
  """
  url_str = 'redshift+psycopg2://{user}:{pwd}@{host}:{port}/{dbname}'.format(**config)
  redshift_engine = create_engine(url_str)
  return pd.read_sql_query(query, redshift_engine, parse_dates=dates_cols)

def upload_dataframe_to_s3(aws_config, df, filename, remote_filename_prefix=None, columns=None):
  """ Writes the dataframe to a csv in a csv bucket file

  Arguments
  :param filename - the filename that will be both to save the file in a temp dir locally and
                    also remotely.
                    Caveat, the remote file may have a prefix appended by specifying one.
  :param remote_filename_prefix - if trying to load multiple csv's into a single table, add a
      prefix to the remote filename in order to later do use a Redshift COPY statement.
  """
  LOG.info('bulk loading dataframe to redshift')

  if remote_filename_prefix is not None:
    filename = remote_filename_prefix + '_' + filename

  temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')
  LOG.debug('Saving a dataframe to csv: %s', temp_file.name)
  df.to_csv(temp_file, index=False, columns=columns)
  access_key_id = aws_config['aws_access_key_id']
  secret_access_key = aws_config['aws_secret_access_key']

  s3_client = boto3.client(
      's3',
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key,
  )

  LOG.debug(
      'Uploading csv: %s to bucket: %s',
      temp_file.name,
      aws_config['s3_bucket']
  )

  s3_client.upload_file(temp_file.name, aws_config['s3_bucket'], filename)
  temp_file.close()
  return

def copy_from_s3_to_redshift(db_config, aws_config, tablename, filename, create_tbl_stmt, no_recreate=None):
  """ Copies a file from a csv into a redshift table. If the filename is a "prefix", then all the
      csv's with that prefix will be loaded into the table together.
      Look at the following example under "Using a Manifest to Specify Data Files":
          http://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html
      This function
  """
  conn = create_sql_conn(db_config)
  cur = conn.cursor()
  copy_statement = REDSHIFT_COPY_CSV.format(
      tablename=tablename,
      s3_bucket=aws_config['s3_bucket'],
      prefix=filename,
      access_key_id=aws_config['aws_access_key_id'],
      secret_access_key=aws_config['aws_secret_access_key'],
  )
  if no_recreate is None:
    LOG.debug('create statement: %s', create_tbl_stmt)
    LOG.debug('rebuilding table --> [{0}]'.format(tablename))
    cur.execute(create_tbl_stmt)
    conn.commit()
  cur.execute(copy_statement)
  conn.commit()
  conn.close()
  return

def bulk_load_to_redshift(
    db_config,
    aws_config,
    df,
    tablename,
    create_tbl_stmt,
    filename=None,
    columns=None,
    no_recreate=None
):
  """ Convience method to upload a single dataframe into redshift. Does this via creating a
      csv that it uploads to csv and immediately copies into a redshift table via a copy statement
      before returning. If you want to write several dataframes into a single redshift table,
      individual write dataframes to csv with a common prefix then use the prefix functionality of
      the copy statement to upload all the tables into redshift efficiently.
  """
  if filename is None:
    filename = 'tmp_' + tablename + '_' + str(time.time())
  upload_dataframe_to_s3(aws_config, df, filename, columns=columns)
  copy_from_s3_to_redshift(db_config, aws_config, tablename, filename, create_tbl_stmt, no_recreate)
  remove_file_from_s3(aws_config, filename)
  return

def remove_file_from_s3(aws_config, filename):
  """Remove file from s3

    Arguments:
      aws_config - aws config dictionary - should contains aws_access_key_id, aws_secret_access_key,
                                           aws_region, s3_bucket
      filename - the key name needed to identify the to be removed file in the bucket
  """
  LOG.debug('Removing {0} from {1} bucket'.format(filename, aws_config['s3_bucket']))
  access_key_id = aws_config['aws_access_key_id']
  secret_access_key = aws_config['aws_secret_access_key']
  s3_client = boto3.client(
      's3',
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key,
  )
  resp = s3_client.delete_object(
    Bucket=aws_config['s3_bucket'],
    Key=filename
  )
  return

def create_sql_conn(config):
  """Connects to a sql instance and returns the connection object.

  Arguments:3
  :param config -- configuration dictionary- should contain user, pwd, host, post, dbname
      properties.
  :param query  -- sql query use to pull data into a dataframe.
  """
  return psycopg2.connect(
      dbname=config['dbname'],
      host=config['host'],
      port=config['port'],
      user=config['user'],
      password=config['pwd']
  )

def get_columns_for_table(config, tablename):
  """ Returns information about the columns for a table. More information:
      http://docs.aws.amazon.com/redshift/latest/dg/r_PG_TABLE_DEF.html

  Arguments
  :param config -- configuration dictionary- should contain user, pwd, host, post, dbname
      properties.
  :param tablename -- the name of the table you want information for.
  :returns -- tuples (String, Type) :: ("column name", "column type")
  """
  conn = create_sql_conn(config)
  query = """select "column", type
      from pg_table_def
      where tablename='{tablename}'""".format(tablename=tablename)
  columns = sql_query_fetch(conn, query)
  return columns

def sql_query_agg_func(connection, query):
  """Executes a sql query that is an aggregate function and returns the value.

  Arguments:
  connection -- configuration dictionary- should contain user, pwd, host, post, dbname properties.
  query  -- sql query use to pull data into a dataframe.
  """
  result = sql_query_fetch(connection, query)
  return result[0][0]

def sql_query_fetch(connection, query, get_return_value=True):
  """Executes a sql query against a sql connection and returns it results

  Arguments:
  connection -- configuration dictionary- should contain user, pwd, host, post, dbname properties.
  query  -- sql query use to pull data into a dataframe.
  """
  try:
    # retrieving all tables in my search_path
    cursor = connection.cursor()
    cursor.execute(query)
    if get_return_value:
      return cursor.fetchall()
    else:
      return
  except Exception as err:
    raise err

if __name__ == '__main__':
  raise Exception('%r cannot be used as a standalone script.' % sys.argv[0])
