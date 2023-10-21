import sys
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Convoso')
from convoso_util import Convoso 
import pandas as pd
import requests
import json


if __name__ == '__main__':
	
	c = Convoso('convoso')
	end_points = ['call_log','leads']

	for pt in end_points:

		
		c.run(pt,'2023-08-01 00:00:00','2023-08-01 23:59:59')
		c.run(pt,'2023-08-02 00:00:00','2023-08-02 23:59:59')
		c.run(pt,'2023-08-03 00:00:00','2023-08-03 23:59:59')
		c.run(pt,'2023-08-04 00:00:00','2023-08-04 23:59:59')
		c.run(pt,'2023-08-05 00:00:00','2023-08-05 23:59:59')
		c.run(pt,'2023-08-06 00:00:00','2023-08-06 23:59:59')
		c.run(pt,'2023-08-07 00:00:00','2023-08-07 23:59:59')
		c.run(pt,'2023-08-08 00:00:00','2023-08-08 23:59:59')
		c.run(pt,'2023-08-10 00:00:00','2023-08-10 23:59:59')
		c.run(pt,'2023-08-11 00:00:00','2023-08-11 23:59:59')
		c.run(pt,'2023-08-12 00:00:00','2023-08-12 23:59:59')
		c.run(pt,'2023-08-13 00:00:00','2023-08-13 23:59:59')
		c.run(pt,'2023-08-14 00:00:00','2023-08-14 23:59:59')
		c.run(pt,'2023-08-15 00:00:00','2023-08-15 23:59:59')
		c.run(pt,'2023-08-16 00:00:00','2023-08-16 23:59:59')
		c.run(pt,'2023-08-17 00:00:00','2023-08-17 23:59:59')
		c.run(pt,'2023-08-18 00:00:00','2023-08-18 23:59:59')
		c.run(pt,'2023-08-19 00:00:00','2023-08-19 23:59:59')
		c.run(pt,'2023-08-20 00:00:00','2023-08-20 23:59:59')
		c.run(pt,'2023-08-21 00:00:00','2023-08-21 23:59:59')
		c.run(pt,'2023-08-22 00:00:00','2023-08-22 23:59:59')
		c.run(pt,'2023-08-23 00:00:00','2023-08-23 23:59:59')
		c.run(pt,'2023-08-24 00:00:00','2023-08-24 23:59:59')
		c.run(pt,'2023-08-25 00:00:00','2023-08-25 23:59:59')
		c.run(pt,'2023-08-26 00:00:00','2023-08-26 23:59:59')
		c.run(pt,'2023-08-27 00:00:00','2023-08-27 23:59:59')
		c.run(pt,'2023-08-28 00:00:00','2023-08-28 23:59:59')
		c.run(pt,'2023-08-29 00:00:00','2023-08-29 23:59:59')
		c.run(pt,'2023-08-30 00:00:00','2023-08-30 23:59:59')
		c.run(pt,'2023-08-31 00:00:00','2023-08-31 23:59:59')


	
	
	

	
	






