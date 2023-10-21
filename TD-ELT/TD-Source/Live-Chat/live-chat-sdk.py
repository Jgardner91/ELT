from livechat.reports.base import ReportsApi
from livechat.configuration.base import ConfigurationApi
import configparser
import requests 
import json 
from datetime import date
from datetime import datetime, timedelta
import pandas as pd 
import boto3
import io
import sys
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Utils')
from Initialize import Secrets 
from Initialize import Utils
from livechat.agent.web.base import AgentWeb




class LiveChatSDK(Utils):
	
	def __init__(self,app_type):
		super().__init__(app_type)
		self.agentWeb =  AgentWeb.get_client(access_token = 'Basic ' + self.full_basic_auth)


	def chat_archives(self,report_type='chat_archives'):
		

		results = self.agentWeb.list_archives()
		raw_response = results.text
		data = json.loads(raw_response)
		df = pd.DataFrame(data["chats"])
		self.putS3('turbo-debt-dev','live-chat-reports/agent/{}'.format(report_type),df)


	def current_chats(self,p_id=None):
		data = json.loads(self.agentWeb.list_chats(page_id = p_id).text)
		id_ = pd.DataFrame(data["chats_summary"])
		return data,data["next_page_id"]
		
	def parse_JSON(self,data):
		df = pd.DataFrame(data["chats_summary"])
		return df




if __name__ == '__main__':

	live = LiveChatSDK('live')
	chats = []
	next_page = []
	
	for i in range(500):
		print(i)
		try: 
			if i == 0:
				result=live.current_chats()
				p_id = result[1]
				chats.append(live.parse_JSON(result[0]))

			else:
				result = live.current_chats(p_id)
				chats.append(live.parse_JSON(result[0]))
				p_id = result[1]

		except Exception as e:
			pass

		if result[1] not in next_page:
			next_page.append(result[1])
		

	df = pd.concat(chats)
	df.reset_index(drop=True,inplace=True)
	report_type = 'current_chats'
	live.putS3('turbo-debt-dev','live-chat-reports/agent/{}'.format(report_type),df)
	print(len(next_page))

	

