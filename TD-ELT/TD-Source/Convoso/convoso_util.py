import pandas as pd
import boto3
import io
import sys 
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Utils') 
import Initialize
from Initialize import Secrets
from Initialize import Utils
import configparser
import requests 
import json 


class Convoso(Utils):
	
	def __init__(self,app_type):
		super().__init__(app_type)


	def getTotal(self,api_type,start_time,end_time):
		
		api_map = {'leads':self.leads,'call_log':self.call_log,'agent_productivity':self.agent_productivity,'call_back':self.call_back}
		lead_url = '{}{}&start_time={}&end_time={}'.format(api_map[api_type],self.cnv_auth_token,start_time,end_time)
		print(lead_url)
		response = requests.get(lead_url)
		raw = response.text
		data = json.loads(raw)
		
		if api_type == 'leads' or api_type == 'agent_productivity':
			total_records = data['data']['total']
			print(total_records)
		if api_type == 'call_log':
			total_records = data['data']['total_found']
			print(total_records)
		if api_type == 'call_back':
			if int(data['data']['total']) < int(data['data']['limit']):
				total_records = 0 
			if int(data['data']['total'])> int(data['data']['limit']):
				total_records = int(data['data']['total'])

		return total_records

	def lead_set(self,offset,limit,start_time,end_time):
		lead_url = '{}{}&offset={}&limit={}&start_time={}&end_time={}'.format(self.leads,self.cnv_auth_token,offset,limit,start_time,end_time)
		response = requests.get(lead_url)
		raw = response.text
		data = json.loads(raw)
		return pd.DataFrame(data['data']['entries'])

	def call_log_set(self,offset,limit,start_time,end_time):
		call_log_url = '{}{}&offset={}&limit={}&start_time={}&end_time={}'.format(self.call_log,self.cnv_auth_token,offset,limit,start_time,end_time)
		response = requests.get(call_log_url)
		raw = response.text
		data = json.loads(raw)
		df = pd.DataFrame(data['data']['results'])
		#call_log = df[['lead_id',	'list_id',	'campaign_id',	'campaign',	'queue',	'user',	'user_id',	'phone_number'	,'first_name' ,'last_name',	'status',	'status_name'	,'call_length'	,'call_date',	'agent_comment'	,'queue_id'	,'called_count'	,'caller_id_displayed'	,'inbound_number' ,'term_reason',	'call_type'	,'queue_position']]
		return df

	def agent_productivity_set(self,offset,limit):
		lead_url = '{}{}&offset={}&limit={}'.format(self.agent_productivity,self.cnv_auth_token,offset,limit)
		response = requests.get(lead_url)
		raw = response.text
		data = json.loads(raw)
		return pd.DataFrame(data['data']['entries'])

	def call_backs_set(self,offset,limit):
		lead_url = '{}{}&offset={}&limit={}'.format(self.call_back,self.cnv_auth_token,offset,limit)
		response = requests.get(lead_url)
		raw = response.text
		data = json.loads(raw)
		return pd.DataFrame(data['data']['results'])




	def run(self,sub_directory,start_time,end_time):
		carrier = [] 
		j = 0

		if sub_directory == 'call_log':
			k = 10000
			for i in range(int(int(self.getTotal(sub_directory,start_time,end_time))/k)+1):
				print(i)
				carrier.append(self.call_log_set(j,k,start_time,end_time))
				j += 10000

		if sub_directory == 'leads':
			k = 10000
			for i in range(int(int(self.getTotal(sub_directory,start_time,end_time))/k)+1):
				print(i)
				carrier.append(self.lead_set(j,k,start_time,end_time))
				j += 10000 

		if sub_directory == 'agent_productivity':
			k = 1000
			print(int(self.getTotal(sub_directory))/k)
			for i in range(int(int(self.getTotal(sub_directory))/k)+1):
				print(i)
				carrier.append(self.agent_productivity_set(j,k))
				j+=1000
		if sub_directory == 'call_back':

			if self.getTotal(sub_directory) == 0: 
				carrier.append(self.call_backs_set(j,20))
			if self.getTotal(sub_directory) > 0:
				k = 20 
				for i in range(int(int(self.getTotal(sub_directory))/k)+1):
					carrier.append(self.call_backs_set(j,k))
					j+=20



		if len(carrier) > 1:
			df = pd.concat(carrier)
			df.reset_index(drop=True,inplace=True)
		else:
			df = pd.DataFrame(carrier[0])
		
		self.putS3('turbo-debt-dev','convoso/{}/{}_{}.csv'.format(sub_directory,sub_directory,start_time.split(' ')[0]),df)

		return df




