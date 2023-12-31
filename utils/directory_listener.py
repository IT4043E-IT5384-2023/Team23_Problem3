import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from config import (
    OUR_RAW_TWEETS_DIR, 
    DIR_LISTENER
)

if not os.path.exists(OUR_RAW_TWEETS_DIR):
    os.makedirs(OUR_RAW_TWEETS_DIR)

class Watcher:
    def __init__(self, directory_to_watch):
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(
            event_handler, 
            self.DIRECTORY_TO_WATCH, 
            recursive=True
        )
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            print(f"Received created event - {event.src_path}.")
            with open(DIR_LISTENER, 'a', encoding='utf-8') as f:
                f.write(event.src_path + '\n')


def file_listener():
    w = Watcher(OUR_RAW_TWEETS_DIR)  # set the directory here
    w.run()



if __name__ == "__main__":
    file_listener()
