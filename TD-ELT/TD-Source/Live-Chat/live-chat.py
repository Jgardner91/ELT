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




class LiveChatReports(Utils):
	
	def __init__(self,app_type):
		super().__init__(app_type)

		# objects specific to LiveChat application
		self.date_from =  str((datetime.today()-timedelta(days=1)).date())
		self.date_to = str(datetime.today().date())


	def transformReport(self,report):
		
		raw_response = report.text
		data = json.loads(raw_response)
		df = pd.DataFrame(data['records']).T 
		df.reset_index(inplace=True)
		return df

	def getReport(self,report_path):

		if report_path == 'list_chats':
			url = self.live_chat_chats_base  + report_path + '?organization_id=' + self.live_chat_org_id
			print(url)
		
			headers = {"Authorization": "Basic {}".format(self.full_basic_auth),"Content-Type": "application/json",}
			params = {}
			getResponse = requests.get(url, params = params, headers=headers)
			print(getResponse)
			
		else:
		
			url = self.live_chat_reports_base + report_path
			
			headers = {"Authorization": "Basic {}".format(self.full_basic_auth),"Content-Type": "application/json",}
			params = {"distribution":"day","filters":{"date_from": self.date_from,"date_to": self.date_to}}
			getResponse = requests.get(url, params = params, headers=headers)
			print(getResponse)


		return getResponse
		
	def cleanReport(self,report_type):
		

		if report_type == 'engagement':
			
			report = self.getReport("chats/engagement")
			df = self.transformReport(report)
			df.rename(columns = {"index":"Day","started_by_agent":"ChatsStartedAgent","started_by_customer_without_greeting":"ChatsStartedCustomer"},inplace=True)
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)

		if report_type == "duration":
			
			report = self.getReport("chats/duration")
			df = self.transformReport(report)
			df.rename(columns = {"index":"Day","agents_chatting_duration":"AvgAgentChatDuration","count":"TotalChats"},inplace=True)
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)

		
		# aggregate chat level report, key on day 
		if report_type == "ratings":
			
			report = self.getReport("chats/ratings")
			df = self.transformReport(report)
			df.rename(columns = {"index":"Day","bad":"NumBadChats","good":"NumGoodChats"},inplace=True)
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)
		
		#  aggregate agent level report, key on agent
		# aggregate is set to last 7 days
		if report_type == "ranking":
			
			report = self.getReport("chats/ranking")
			df = self.transformReport(report)
			df.rename(columns = {"index":"Agent","score":"AgentScore","bad":"BadChats","good":"GoodChats","total":"TotalRatedChats"},inplace=True)
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)

		# aggregate agent level report, key on agent
		# aggregate is set to last 7 days
		if report_type == "performance":
			
			report = self.getReport("agents/performance")
			df = self.transformReport(report)
			df.rename(columns = {"index":"Agent","accepting_chats_time":"TimeAcceptingChats","chats_count":"TotalChats","chats_rated_bad":"BadChats",
				"chats_rated_good":"GoodChats","chatting_time":"ChatTime","first_response_chats_count":"FirstResponse","first_response_time":"AvgFirstReplyTime",
				"logged_in_time":"TimeLoggedIn","not_accepting_chats_time":"TimeNotAcceptingChats"},inplace=True)
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)

		if report_type == "forms":
			report = self.getReport("chats/forms")
			data = json.loads(report.text)

			master_list = []

			for r in range(len(data['records'])):
				form_11591 = []
				form_11592 = []
				
				form_id = data['records'][r]['form_id']
				form_count = data['records'][r]['count']
				group_id = data['records'][r]['group_id']
				
				form_11591.append(form_id)
				form_11591.append(form_count)
				form_11591.append(group_id)

				form_11592.append(form_id)
				form_11592.append(form_count)
				form_11592.append(group_id)

				try:
					question_1 = data['records'][r]['fields'][0]['field_id']
					form_11591.append(question_1)
				except Exception as e:
					question_1 = 0
					form_11591.append(question_1)
				
				try:
					question_2 = data['records'][r]['fields'][1]['field_id']
					form_11592.append(question_2)

				except Exception as e:
					question_2 = 0 
					form_11592.append(question_2)

				try:
					question_1_yes = data['records'][r]['fields'][0]['answers'][0]['count']
					form_11591.append(question_1_yes)
				except Exception as e:
					question_1_yes = 0
					form_11591.append(question_1_yes)

				try: 
					question_1_no = data['records'][r]['fields'][0]['answers'][1]['count']
					form_11591.append(question_1_no)
				except Exception as e:
					question_1_no = 0 
					form_11591.append(question_1_no)

				try:
					question_2_yes = data['records'][r]['fields'][1]['answers'][0]['count']
					form_11592.append(question_2_yes)
				except Exception as e:
					question_2_yes = 0
					form_11592.append(question_2_yes)

				try:
					question_2_no = data['records'][r]['fields'][1]['answers'][1]['count']
					form_11592.append(question_2_no)
				except Exception as e:
					question_2_no = 0
					form_11592.append(question_2_no)

				master_list.append(form_11591)
				master_list.append(form_11592)

			df = pd.DataFrame(master_list,columns = ['Form_id','Form_count','Group','Field_id','Yes','No'])
			self.putS3('turbo-debt-dev','live-chat-reports/reports/{}'.format(report_type),df)


if __name__ == '__main__':

	chat = LiveChatReports('live')
	chat.cleanReport('engagement')
	chat.cleanReport('duration')
	chat.cleanReport('ratings')
	chat.cleanReport('ranking')
	chat.cleanReport('forms')
	chat.cleanReport('performance')
	



	




			

