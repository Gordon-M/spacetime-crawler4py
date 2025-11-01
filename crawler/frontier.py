import os
import shelve
from threading import Thread, RLock
from queue import Queue, Empty
from collections import defaultdict
from urllib.parse import urlparse
import time
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid
from scraper import unique_pages, page_word_counts, token_counts
from collections import Counter

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # uses queue instead of list for thread safety
        self.to_be_downloaded = Queue()

        # domain name : timestamp
        # timestamp defaults to 0.0
        self.domain_last_seen = defaultdict(float)
        self.lock = RLock()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.put(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            # wait 10 sec if queue is empty
            url = self.to_be_downloaded.get(timeout=10)
        except Empty:
            return None
        
        domain = urlparse(url).netloc

        with self.lock:
            cur_time = time.time()
            elapsed_time = cur_time - self.domain_last_seen[domain]
            if elapsed_time > self.config.time_delay:  # domain hasn't been accessed recently
                self.domain_last_seen[domain] = cur_time
                return url
            
            # sets new domain accessed time before
            # sleeping to delay other worker threads
            sleep_time = self.config.time_delay - elapsed_time
            self.domain_last_seen[domain] = cur_time + sleep_time

        time.sleep(sleep_time)
        with self.lock:
            self.domain_last_seen[domain] = time.time()
            return url

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()

    def print_crawl_stats(self):
        log_file = os.path.join("Logs", "crawl_stats.txt")

        with open(log_file, "w", encoding="utf-8") as f:
            f.write("-------------CRAWL STATS------------\n")
            
            total_pages = len(unique_pages)
            f.write(f"Total unique pages: {total_pages}\n")
            print(f"Total unique pages: {total_pages}")

            if page_word_counts:
                longest_page = max(page_word_counts, key=page_word_counts.get)
                most_words = page_word_counts[longest_page]
                f.write(f"Longest page: {longest_page} with {most_words} words\n")
                print(f"Longest page: {longest_page} with {most_words} words")

            top_words = Counter(token_counts).most_common(50)
            f.write("Top 50 most common words:\n")
            print("Top 50 most common words:")
            for word, count in top_words:
                f.write(f"{word}: {count}\n")
                print(f"{word}: {count}")

            subdomains = defaultdict(int)
            for url in unique_pages:
                netloc = urlparse(url).netloc.lower()
                if netloc.endswith(".uci.edu"):
                    subdomains[netloc] += 1

            f.write("Subdomains found in uci.edu:\n")
            print("Subdomains found in uci.edu:")
            for sub, count in sorted(subdomains.items()):
                f.write(f"{sub}, {count}\n")
                print(f"{sub}, {count}")

        print(f"Crawl stats saved to {log_file}")