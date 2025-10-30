import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import random
import hashlib

fingerprints = {}

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def parseWords(text):
    #Returns only words. HTML Tags, punctuation, whitespace removed
    only_words = re.sub(r'[^\w\s]', '', text)
    remove_tags = re.sub(r'<.*?>', '', only_words)
    return remove_tags
    
def ngram_sort(text):
    #returns words grouped into 3-grams
    ngrams = []
    words = text.split(" ")

    for i in range(len(words) - 3 + 1): #groups of 3, starts at 0
        ngram = words[i:i+3]
        ngrams.append(" ".join(ngram))
    return ngrams

def hash_ngrams(ngrams):
    to_hash = random.sample(ngrams, min(len(ngrams), 100))
    hashed_ngrams = []
    for ngram in to_hash:
        hashed_ngram = hashlib.sha256(ngram.encode()).hexdigest()
        hashed_ngrams.append(hashed_ngram)
    return hashed_ngrams

def is_near_dup(hashed_ngrams):
    if not hashed_ngrams:
        return False

    duplicates = 0
    for ngram in hashed_ngrams:
        if ngram in fingerprints:
            duplicates += 1
    similarity_score = duplicates / len(hashed_ngrams)
    #print(similarity_score)
    return similarity_score > .9

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
    hyperlinks = []

    if resp.status != 200 or resp.raw_response == None:
        #print(f"Skipping URL {url} due to bad status or empty content.")
        return hyperlinks
    
    file_size_limit = 2500000
    if len(resp.raw_response.content) > file_size_limit:
        #print(f"Skipping URL {url} due to large file size.")
        return hyperlinks

    soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    text = soup.get_text(separator=' ', strip=True)
    if len(text.split()) < 60:
        #print(f"Skipping URL {url} due to insufficient text content.")

        return hyperlinks

    parsed_text = parseWords(text)
    ngrams = ngram_sort(parsed_text)
    #print(f"Extracted {len(ngrams)} n-grams from {url}.")  # Debugging the number of n-grams extracted
    hashed_ngrams = hash_ngrams(ngrams)
    #print(f"Hashed {len(hashed_ngrams)} n-grams from {url}.")  # Debugging the number of hashed n-grams

    # for fingerprint in hashed_ngrams:
    #     if fingerprint in fingerprints:
    #         print(f"Skipping URL {url} due to duplicate n-gram fingerprint.")  # Debug if we find a duplicate fingerprint
    #         return hyperlinks
    if is_near_dup(hashed_ngrams):
        return hyperlinks
    for ngram in hashed_ngrams:
        fingerprints[ngram] = True

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
    ignore_list = ["wics.ics", "ngs.ics", "/doku", "mediamanager.php", "eppstein/pix"]
    calendar_list = ["week", "month", "year", "calendar"]
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            #print(f"Rejected due to invalid scheme: {url}")
            return False
        netloc = parsed.netloc.lower()
        
        for item in ignore_list:
            if item in netloc or item in parsed.path.lower():
                #print(f"Rejected due to ignore list: {url}")
                return False
        for item in calendar_list:
            if item in parsed.path.lower():
                #print(f"Rejected due to calendar list: {url}")
                return False


        if not netloc.endswith((".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu")):
            #print(f"Rejected due to domain mismatch: {url}")
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
