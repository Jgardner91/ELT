import configparser
from datetime import date
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import BatchRunReportsRequest
import pandas as pd 
import boto3
import io
import os
import psycopg2 as ps

class Secrets:
	"""
	This class defines application secrets for various endpoints
	"""

	def __init__(self):
		
		self.getConfig()

	def getConfig(self):
		
		config = configparser.ConfigParser()
		config.read('/Users/jamesgardner/Desktop/configuration/config.ini')
		
		# aws properties
		self.aws_secret_key = config["AWS"]["AWS_SECRET_KEY"]
		self.aws_base_key = config["AWS"]["AWS_BASE_KEY"]

		# live chat properties
		self.full_basic_auth = config["LIVECHAT"]["BASIC_AUTH"]
		self.live_chat_reports_base = config["LIVECHAT"]["BASE_REPORT_URL"]
		self.live_chat_chats_base = config["LIVECHAT"]["BASE_CHATS_URL"]
		self.live_chat_org_id = config["LIVECHAT"]["ORG_ID"]

		# google analytics properties
		self.ga_scopes = config["GOOGLE_ANALYTICS"]["SCOPES"]
		self.ga_key_path = config["GOOGLE_ANALYTICS"]["KEY_FILE"]
		self.ga_property = config["GOOGLE_ANALYTICS"]["PROPERTY"]
		
		# Postgres properties
		self.pg_user = config['POSTGRES']['USER']
		self.pg_db = config['POSTGRES']['DB']
		self.pg_pass = config['POSTGRES']['PASS']
		self.pg_host = config['POSTGRES']['HOST']
		self.pg_cert = config['POSTGRES']['CERT']

		#convoso properties
		self.cnv_auth_token = config['CONVOSO']['AUTH']
		self.leads = config['CONVOSO']['LEADS']
		self.call_log = config['CONVOSO']['CALL_LOG']
		self.agent_productivity = config['CONVOSO']['AGENT_PRODUCTIVITY']
		self.call_back = config['CONVOSO']['CALLBACKS']

		# klavio properties
		self.klavio_api_key = config['KLAVIO']['API_KEY']
		self.klavio_base_url = config['KLAVIO']['BASE_URL']
		self.klavio_events = config['KLAVIO']['EVENTS']
		self.klavio_metrics = config['KLAVIO']['METRICS']
		self.klavio_segments = config['KLAVIO']['SEGMENTS']
		self.klavio_lists = config['KLAVIO']['LISTS']
		self.klavio_tags = config['KLAVIO']['TAGS']

	


class Utils(Secrets):
	def __init__(self,app_type):
		Secrets.__init__(self)

		self.s3_client = self.getS3()

		if app_type == 'google':
			self.ga_client = self.getGA()
		if app_type == 'live':
			pass
		if app_type == 'postgres':
			self.pg_client = self.getPG()
		if app_type == 'convoso':
			pass
		if app_type == 'klavio':
			pass

	def getS3(self):
		client = boto3.client("s3", aws_access_key_id= self.aws_base_key,aws_secret_access_key=self.aws_secret_key)
		return client

	def getGA(self):
		os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.ga_key_path
		client = BetaAnalyticsDataClient()
		return client 

	def getPG(self):
		con = ps.connect(database=self.pg_db,user=self.pg_user,password=self.pg_pass,host=self.pg_host,port = '25060',sslmode='verify-ca',sslrootcert=self.pg_cert)
		cursor_obj = con.cursor()
		return cursor_obj


	def putS3(self,bucket,key,df):
		
		with io.StringIO() as csv_buffer:
			df.to_csv(csv_buffer, index=False)
			response = self.s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
			status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
			
		return status



	

