#!/usr/bin/python
# this script takes a job alert JSON file and converts it to a tsv

import os, json
import pandas as pd
import numpy as np
import logging
# import boto3
# import botocore
from boto.s3.connection import S3Connection

# finding only JSON files in the current directory and invoking them...
path_to_json = '/Users/douglas.kenter/Documents/job-alerts/json/dataQuality/prod/2017-11-29/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

# generating the columns...
prefix_cols = ['keyword','jobs','missedJobs','jsonfile','json_element_id']
colnames = []

# gathering names for all keys in JSON file(s)...
for index, js in enumerate(json_files):
	with open(os.path.join(path_to_json, js)) as json_file:
		json_text = json.load(json_file)
		try:
			for d in json_text['jobs']['java.util.ArrayList']:
				colnames.append(d.keys())
		except:
			pass
		try:
			for d in json_text['missedJobs']['java.util.ArrayList']:
				colnames.append(d.keys())
		except:
			pass

# consolidating unique names for all keys in JSON file(s)...
colnames = np.unique(colnames).tolist()

# referencing values from keys and building our python list...
content = []
for index, js in enumerate(json_files):
	with open(os.path.join(path_to_json, js)) as json_file:
		json_text = json.load(json_file)
		query = json_text['keywords']
		try:
			jobs = json_text['jobs']['java.util.ArrayList']
		except:
			jobs = []
		try:
			missed = json_text['missedJobs']['java.util.ArrayList']
		except:
			missed = []

		if len(jobs) > 0:
			jobs_rows = []
			for i, j in enumerate(jobs):
				jobs_row = [query, True, False, js, i]

				for cn in colnames:
					try:
						jobs_row.append(j[cn])
					except:
						jobs_row.append(None)
				jobs_rows.append(jobs_row)
			content.extend(jobs_rows)

		if len(missed) > 0:
			misses_rows = []
			for i, m in enumerate(jobs):
				miss_row = [query, False, True, js, i]

				for cn in colnames:
					try:
						miss_row.append(m[cn])
					except:
						miss_row.append(None)
				misses_rows.append(miss_row)
			content.extend(misses_rows)

# describing headers...
header = prefix_cols+colnames

# writing the python list to a Pandas DataFrame and sending it to a .tsv...
df = pd.DataFrame(content, columns=header)
df.to_csv('./2017-12-29-ja.tsv', encoding='utf-8',sep='\t')

# deleting keys in bucket
# for key in bucket.objects.all():
# key.delete()
