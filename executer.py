import json
from http.client import HTTPConnection
from urllib.parse import urlencode

from subprocess import check_output


class Tasker(object):
	def __init__(self, url):
		url_data = url.split('/', 1)
		self.host = url_data[0]
		self.path = url_data[1] if len(url_data) > 1 else ''

	def _get_connection(self):
		http = HTTPConnection(self.host)

		return http

	def fetch(self):
		http = self._get_connection()
		http.request("GET",
					 '/' + self.path + '/tasks/fetch')
		resp = http.getresponse()
		data = resp.read()

		task = json.loads(data.decode())

		return task

	def finish(self, id, execution_exit_status):
		http = self._get_connection()
		http.request("PUT",
					 '/' + self.path + '/tasks/finish',
					 headers={"Content-type": 'application/x-www-form-urlencoded'},
					 body=urlencode({"task_id": id,
								     "execution_exit_status": execution_exit_status}))
		resp = http.getresponse()
		resp.read()


def execute_task(task_content):
	with open('temp.bat', 'w') as file:
		file.write(task_content)

	check_output('temp.bat', shell=True)


def main(url):
	tasker = Tasker(url)

	while True:
		try:
			task = tasker.fetch()
		except ValueError:
			print('THERE ARE NO TASKS TO BE EXECUTED')
			continue

		try:
			print("EXECUTING TASK %i" % task["id"])
			execute_task(task["content"])
		except Exception as e:
			print(e)
			exit_status = 1
		else:
			exit_status = 0

		try:
			tasker.finish(task["id"], exit_status)
		except Exception as e:
			print("UNABLE TO FINISH THE TASK %i" % task["id"])
			print(e)


if __name__ == '__main__':
	import sys
	main(sys.argv[1])
