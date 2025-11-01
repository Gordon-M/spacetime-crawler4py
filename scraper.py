import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import random
import hashlib
from collections import defaultdict
from nltk.stem import PorterStemmer
from threading import RLock

stemmer = PorterStemmer()
STOPWORDS = {
    "the", "is", "in", "at", "of", "on", "and", "a", "to", "for",
    "this", "that", "it", "as", "an", "by", "be", "from", "with",
    "or", "are", "was", "were", "but", "not", "can", "will", "has",
    "have", "had", "so", "if", "then", "when", "while", "which",
}

# (i, 16-bit chunk) : set of hashes with that chunk in the ith pos
# defaultdict creates empty set for new keys
simhash_buckets = defaultdict(set)
visited_urls = set()

unique_pages = {}
page_word_counts = {}
token_counts = {}

lock = RLock()

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

# removes HTML Tags, punctuation, whitespace, stopwords
# then stems and returns tokens
def parse_text(text):
    only_words = re.sub(r'[^\w\s]', '', text)

    tokens = only_words.lower().split()
    remove_stopwords = [t for t in tokens if t not in STOPWORDS]
    stemmed_tokens = [stemmer.stem(t) for t in remove_stopwords]

    return stemmed_tokens

# gets b-bit hash of text
def simhash(tokens, b=64):
    v = [0] * b

    for token in tokens:
        # get lower b-bits of 128-bit hash on individual token
        token_hash = int(hashlib.md5(token.encode('utf-8')).hexdigest(), 16)

        # update vector v
        for i in range(b):
            bitmask = 1 << i
            if token_hash & bitmask:
                v[i] += 1
            else:
                v[i] -= 1
    
    # convert v to binary b-bit fingerprint
    fingerprint = 0
    for i in range(b):
        if v[i] >= 0:
            fingerprint |= 1 << i
    
    return fingerprint

# uses similarity based on hamming distance
# between 2 simhashes
def is_near_simhash_duplicate(hash1, b=64):
    chunks = [(hash1) >> (16*i) & 0xFFFF for i in range(4)]

    for i, chunk in enumerate(chunks):
        for hash2 in simhash_buckets[(i, chunk)]:
            num_diff = bin(hash1 ^ hash2).count("1")
            similarity_score = 1 - num_diff / b
            if similarity_score >= 0.95:
                return True
    return False

def store_simhash_fingerprint(hash):
    for i in range(4):
        chunk = (hash >> (16*i) & 0xFFFF)
        simhash_buckets[(i, chunk)].add(hash)

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    defrag_url, fragment = urldefrag(url)
    with lock:
        if defrag_url in visited_urls:
            return []
        visited_urls.add(defrag_url)

    if resp.status != 200 or resp.raw_response == None:
        return []
    
    file_size_limit = 2500000
    if len(resp.raw_response.content) > file_size_limit:
        return []

    soup = BeautifulSoup(resp.raw_response.content, 'lxml')

    # remove noise from page content
    for tag in soup(['header', 'footer', 'nav', 'script', 'style', 'aside']):
        tag.decompose()
    
    text = soup.get_text(separator=' ', strip=True)
    if len(text.split()) < 20:
        return []
    words = re.findall(r'\w+', text)
    page_word_counts[defrag_url] = len(words)

    tokens = parse_text(text)
    hash = simhash(tokens)

    with lock:
        if is_near_simhash_duplicate(hash):
            return []
        store_simhash_fingerprint(hash)

        unique_pages[defrag_url] = True
        for token in tokens:
            token_counts[token] = token_counts.get(token, 0) + 1


    hyperlinks = []
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        if href:
            try:
                combined_url = urljoin(url, href)
                defrag_url, fragment = urldefrag(combined_url)
                hyperlinks.append(defrag_url)
            except ValueError:
                continue

    return hyperlinks

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    ignore_list = ["mediamanager.php", "eppstein/pix", "isg.ics.uci.edu/events/", "share=facebook", "share=twitter", "login", "redirect",
                    "grape.ics.uci.edu/wiki/public/timeline", "grape.ics.uci.edu/wiki/asterix/timeline", "ical=", "fano.ics.uci.edu/ca/rules",
                    "week", "month", "year", "calendar"]
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()
        if "grape.ics.uci.edu" in netloc and "action=diff&version=" in query:
            return False
        
        for item in ignore_list:
            if item in netloc or item in path or item in query:
                return False


        if not netloc.endswith((".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu")):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
        
    except TypeError:
        print ("TypeError for ", parsed)
        raise
