import multiprocessing
from producers import fetch_and_produce
from consumers import consume_and_process

def run_producer():
    fetch_and_produce()

def run_consumer():
    consume_and_process()

if __name__ == "__main__":
    producer_process = multiprocessing.Process(target=run_producer)
    consumer_process = multiprocessing.Process(target=run_consumer)
    
    producer_process.start()
    consumer_process.start()
    
    producer_process.join()
    consumer_process.join()