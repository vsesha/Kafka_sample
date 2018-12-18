#!/usr/bin/python3
from time import sleep
from json import dumps
from kafka import KafkaProducer

class Producer():
	
	def run(self):
		producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:dumps(x).encode('utf-8'),
						acks = 'all',
						retries = 0)

		for e in range(100):
			data = {'number' : e}
			print(data)
			producer.send('test', value=data)
			print("sent")
			sleep(5)
		producer.close()
	
def main():
	Producer().run()
	
main()