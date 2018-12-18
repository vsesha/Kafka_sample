#!/usr/bin/python3
from time import sleep
from json import dumps
from kafka import KafkaConsumer
import multiprocessing

class consumer(multiprocessing.Process):
    def __init__(self):
		multiprocessing.Process.__init__(self)
		self.stop_event = multiprocessing.Event()

    def run(self):
		consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
						consumer_timeout_ms=1000)

		consumer.subscribe(['test'])
		while not self.stop_event.is_set():
			for message in consumer:
				print(message)
				if self.stop_event.is_set():
					break
		
		consumer.close()
	

def main():
	consumer().run()
	
main()