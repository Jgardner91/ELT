import psycopg2 as ps
import pandas as pd
import boto3
import configparser
import io
import datetime
import boto3
import io
import sys
from datetime import datetime
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Utils')
from Initialize import Secrets 
from Initialize import Utils



class Postgres(Utils):

	def __init__(self,app_type):
		self.logger_obj = {'rows_returned':[100000],'max_id':[0],'min_id':[0],'total_records':[0],'total_records_delta':[0]}
		self.logger_obj_delta = {'rows_returned':[100000],'max_id':[0],'min_id':[0]}
		super().__init__(app_type)


	def logger_check(self,o):
		if self.logger_obj['rows_returned'][o] == 100000 and (self.logger_obj['max_id'][o+1] > self.logger_obj['max_id'][o]):
			return True
		else:
			return False

	def current_rows(self):
		
		self.pg_client.execute('select COUNT(*) from "First_leads"')
		df = pd.DataFrame(self.pg_client.fetchall(),columns = [desc[0] for desc in self.pg_client.description])
		return int(df['count'])


	def first_leads_full(self):

		# calculating number of offsets
		iter_length = int(self.current_rows()/100000) + 1
		offset = [i*100000 for i in range(iter_length)]
	
		for o, value in enumerate(offset):

			# executing query against first leads table
			self.pg_client.execute('select * from "First_leads" ORDER BY "id" asc OFFSET %s LIMIT %s',[value,100000])
			df = pd.DataFrame(self.pg_client.fetchall(), columns= [desc[0] for desc in self.pg_client.description])
			total_records = self.current_rows()
			
			self.logger_obj['rows_returned'].append(df.shape[0])
			self.logger_obj['max_id'].append(df['id'].max())
			self.logger_obj['min_id'].append(df['id'].min())
			self.logger_obj['total_records'].append(int(total_records))

			if self.logger_check(o):
				df['data_warehouse_insert_time'] = datetime.now()
				# write file to s3
				self.putS3('turbo-debt-dev','postgres-sql/postgres-full/First_leads_{}.csv'.format(str(o)),df)
			
				# configure logger dataframe and write to s3
				if o == 0:
					delta = self.logger_obj['total_records'][1] - self.logger_obj['total_records'][0]
					self.logger_obj['total_records_delta'].append(delta)
					df_logger = pd.DataFrame(self.logger_obj)
				else:
					delta = self.logger_obj['total_records'][o+1] - self.logger_obj['total_records'][o]
					self.logger_obj['total_records_delta'].append(delta)
					df_logger = pd.DataFrame(self.logger_obj)
			
				self.putS3('turbo-debt-dev','postgres-sql/postgres-log/logger_extract_full.csv',df_logger)
			else: 
				print("Rows returned not satisfactory...")
			
		
		return self.logger_obj

	def check_point(self,check_point):

		
		offset = [i*100000 for i in range(checkpoint,170)]
		logger_obj = {'rows_returned':[],'max_id':[0],'min_id':[0],'s3_status':[]}

		for o, value in enumerate(offset):
			cursor_obj.execute('select * from "First_leads" ORDER BY "id" asc OFFSET %s LIMIT %s',[value,100000])
			df = pd.DataFrame(cursor_obj.fetchall(), columns= [desc[0] for desc in cursor_obj.description])

			logger_obj['rows_returned'].append(df.shape[0])
			logger_obj['max_id'].append(df['id'].max())
			logger_obj['min_id'].append(df['id'].min())

			if logger_check(logger_obj,o):
				df['data_warehouse_insert_time'] = datetime.now()
				status = self.putS3('turbo-debt-dev','postgres-sql/postgres-historical/First_leads_{}.csv'.format(str(o)),df)
		

			logger_obj['s3_status'].append(status)
			
		
		return logger_obj


	def first_leads_delta(self,delta):

	
		logger_obj = {'rows_returned':[],'max_id':[0],'min_id':[0],'s3_status':[]}
	
		try:
			self.pg_client.execute('select * from "First_leads" where "last_updated" >= (CURRENT_DATE-%s) ORDER BY "last_updated" ASC',[delta])
			df = pd.DataFrame(self.pg_client.fetchall(), columns= [desc[0] for desc in self.pg_client.description])
			self.logger_obj_delta['rows_returned'].append(df.shape[0])
			self.logger_obj_delta['max_id'].append(df['id'].max())
			self.logger_obj_delta['min_id'].append(df['id'].min())
			df['data_warehouse_insert_time'] = datetime.now()
			status = self.putS3('turbo-debt-dev','postgres-sql/postgres-delta/first_leads_delta.csv',df)

		except Exception as e:
			print("Error: ",e)
	

		
		# writiing logger .csv to s3
		df_logger = pd.DataFrame(self.logger_obj_delta)
		self.putS3('turbo-debt-dev','postgres-sql/postgres-log/logger_extract_delta.csv',df_logger)
		print(df)
		
		return self.logger_obj_delta

if __name__ == '__main__':
	
	#pg_full = Postgres('postgres')
	#pg_full.first_leads_full()
	

	pg_delta = Postgres('postgres')
	pg_delta.first_leads_delta(2)





	

	



