import multiprocessing
from back import tweet_streamer
from time import sleep

if __name__=='__main__':
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=tweet_streamer, args=(queue,))
    process.daemon=True
    process.start()
    
    for i in range(20):
        print(queue.qsize())
        print(queue.get())
        sleep(0.5)