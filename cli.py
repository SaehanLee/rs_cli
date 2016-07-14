# -*- coding: utf-8 -*-
""" CLI tool for creating a table in redshift
and populating the table with data from a csv."""

from dotenv import load_dotenv
from os.path import join, dirname
import argparse
import os
import sys

import query_script
import redshift as db
import pandas as pd
import script as cliTools


# loads dotenv
dotenv_path = join(dirname(__file__), '.env')
if os.path.exists(dotenv_path) == False:
  print('Cannot locate .env file using path --> {}'.format(dotenv_path), file=sys.stderr)
  sys.exit(1)
else:
  load_dotenv(dotenv_path)

HELP_STRING = """To run, call 'python cli.py'
  followed by an input file (csv) and a table name"""

def verify_aws_config(aws_config, command_name):
  """Verify the aws configuration has the proper parameters"""
  if aws_config['s3_bucket'] is None:
    raise RuntimeError('must supply s3bucket arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_access_key_id'] is None:
    raise RuntimeError('must supply access arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_secret_access_key'] is None:
    raise RuntimeError('must supply secret arg to run {cmd} command.'.format(cmd=command_name))
  return

def verify_db_config(db_config):
  """Verify the database config has the proper parameters"""
  if db_config['user'] is None:
    raise RuntimeError('must supply user arg to run a cli command.')
  if db_config['pwd'] is None:
    raise RuntimeError('must supply pwd arg to run a cli command')

#access AWS credentials by reading from .env file
parser = argparse.ArgumentParser(description='Pass data from CSV to Redshift')
parser.add_argument('action', type=str, help=HELP_STRING)
parser.add_argument('--db_user', type=str, default=os.environ.get('DATABASE_USR'),
                    help='database user')
parser.add_argument('--input', type=str, required=True, help='csv file to upload to redshift')
parser.add_argument('--table_name', type=str, required=True, help='destination table name')
parser.add_argument('--db_pwd', type=str, default=os.environ.get('DATABASE_PWD'),
                    help='database password')
parser.add_argument('--db_host', type=str, default=os.environ.get('DATABASE_HOST'),
                    help='database host')
parser.add_argument('--db_port', type=str, default=os.environ.get('DATABASE_PORT'),
                    help='database port')
parser.add_argument('--db_name', type=str, default=os.environ.get('DATABASE_NAME'),
                    help='database name')
parser.add_argument('--db_dest', type=str, default=os.environ.get('DATABASE_DESTINATION'),
                    help='the destination database type (mssql, oracle, postgres...)')
parser.add_argument('--db_dialect_driver', type=str, default=os.environ.get('DATABASE_DIALECT_DRIVER'),
                    help='specifies database conn dialect and driver')
parser.add_argument('--access', type=str, default=os.environ.get('AWS_ACCESS_KEY_ID'),
                    help='aws public access key')
parser.add_argument('--secret', type=str, default=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                    help='aws secret access key')
parser.add_argument('--dry_run', action='store_true', default=False,
                    help='execute calculation without writing to production DB (default -> False)')
parser.add_argument('--s3bucket', type=str, default=os.environ.get('S3_BUCKET'),
                    help='Name of the s3 bucket to use for this environment.')
parser.add_argument('--awsRegion', type=str, default=os.environ.get('AWS_DEFAULT_REGION'),
                    help='aws region to run calculation in. Default ["us-east-1"]')
parser.add_argument('--slack_web_hook', type=str, default=os.environ.get('SLACK_WEB_HOOK'),
                    help='the Slack url to send log message to')
parser.add_argument('--enable_slack', action='store_true', default=False,
                    help='enable slack notification (default --> False)')
parser.add_argument('--schema', type=str, default=os.environ.get('DATABASE_SCHEMA'),
                    help='the schema where tables should upload to')

args = parser.parse_args()


db_config = {
    'dbname': args.db_name,
    'user': args.db_user,
    'pwd': args.db_pwd,
    'host': args.db_host,
    'port': args.db_port,
    'dialect_and_driver': args.db_dialect_driver
}
aws_config = {
    'aws_access_key_id': args.access,
    'aws_secret_access_key': args.secret,
    'aws_default_region': args.awsRegion,
    's3_bucket': args.s3bucket
}

verify_db_config(db_config)

if args.action == 'csv':
  verify_aws_config(aws_config, 'csv')
  filePath = os.path.abspath(args.input)
  createQuery = query_script.get_query_from_csv(filePath, args.table_name)
  df = query_script.get_df_from_csv(filePath)
  print(df)
  # df = cliTools.load_csv_to_df(filePath)
  # cliTools.get_create_table(df)
  db.upload_dataframe_to_s3(aws_config, df, args.table_name)
  db.copy_from_s3_to_redshift(db_config, aws_config, args.table_name, args.table_name, createQuery)


else:
  raise RuntimeError("Invalid action specified to commandline.")

