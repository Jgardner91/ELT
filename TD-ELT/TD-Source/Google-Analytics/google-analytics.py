from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import BatchRunReportsRequest
from datetime import date
from datetime import datetime, timedelta
import configparser
import os
import os.path
import pandas as pd
import boto3
import io
import sys 
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Utils') 
import Initialize
from Initialize import Secrets
from Initialize import Utils



class Google(Utils):

    def __init__(self,app_type):
        super().__init__(app_type)

    def gaReport(self,output=[]):

        dimensions = ['eventName']
        metrics    = ['eventCount','totalUsers', 'eventCountPerUser', 'totalRevenue']

        request    = RunReportRequest(property="properties/{}".format(self.ga_property),dimensions=[Dimension(name="eventName"),],
                                metrics=[Metric(name="eventCount"),Metric(name="totalUsers"),Metric(name="eventCountPerUser"),Metric(name="totalRevenue")],
                                date_ranges=[DateRange(start_date=str((datetime.today() - timedelta(days=28)).date()), end_date="today")],)
    
        # get raw response
        response = self.ga_client.run_report(request)

        # transform into DataFrame
        
        for row in response.rows:
            output.append({"EventName":row.dimension_values[0].value, "EventCount": row.metric_values[0].value,
                "TotalUsers": row.metric_values[1].value,"EventCountPerUser":str(row.metric_values[2]).split('"')[1],"TotalRevenue":str(row.metric_values[3]).split('"')[1]})
        df = pd.DataFrame(output)
        self.putS3('turbo-debt-dev','google-analytics/google_analytics_events.csv',df)
    
        return df 




if __name__ == '__main__':
    

  g = Google('google')
  g.gaReport()

  
   
