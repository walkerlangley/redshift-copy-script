import os
import pg8000

port = 5439
# Set this up in a .envrc file in the root directory
dbname = os.environ.get('DBNAME')
host = os.environ.get('HOST')
user = os.environ.get('USER')
password = os.environ.get('PASSWORD')
bucket = os.environ.get('S3BUCKET')
arn = os.environ.get('IAM_ARN')
table = os.environ.get('TABLE')
key = os.environ.get('DATA_KEY')

conn = pg8000.connect(database=dbname, host=host, port=port, user=user, password=password)
cur = conn.cursor()

"""
  GENERAL COPY COMMAND DOCS: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
  Amazon Copy Command Docs: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html

  IGNOREHEADER AS 1 - use if the file has a header
  CSV - delmiter. The | is the default delimiter. Can also specify a DELIMITER parameter
  ACCEPTINVCHARS - Enables loading of data into VARCHAR columns even if the data contains invalid UTF-8 characters.
  TRUNCATECOLUMNS - Truncates data in columns to the appropriate number of characters so that it fits the column specification.
"""
query = f"""
  copy {table} FROM 's3://{bucket}/{key}'
  credentials 'aws_iam_role={arn}'
  IGNOREHEADER AS 1
  CSV
  REGION AS 'us-west-1'
  ACCEPTINVCHARS
  TRUNCATECOLUMNS;
"""
cur.execute(query)
conn.commit()
conn.close()
