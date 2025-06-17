import sys
import signal
import requests
from pymongo import MongoClient
import fnvhash
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import time
from urllib.parse import urljoin
import os
import json

load_dotenv() 

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise Exception("Brak zmiennej MONGO_URI w .env")

client = MongoClient(MONGO_URI)
db = client["searchengine"]
collection = db["pages"]

class Queue:
    def __init__(self) -> None:
        self.total_qued = 0
        self.number = 0
        self.urls = []

    def enque(self, url) -> None:
        self.total_qued += 1
        self.number += 1
        self.urls.append(url)

    def deque(self) -> str:
        self.number -= 1
        url = self.urls.pop(0)
        return url

    def size(self) -> int:
        return self.number

    def dump_to_json(self) -> None:
        filename = "queue.json"
        try:
            queue_data = {
                "total_qued": self.total_qued,
                "number": self.number,
                "urls": self.urls
            }
            
            with open(filename, "w", encoding="utf-8") as json_file:
                json.dump(queue_data, json_file, indent=4, ensure_ascii=False)
            
            print(f"Zapisano kolejkę do {filename} ({self.number} URL)")
            
        except Exception as e:
            print(f"Błąd podczas zapisywania kolejki: {e}")

    def load_from_json(self, filename="queue.json") -> bool:
        """Load queue state from JSON file"""
        try:
            with open(filename, "r", encoding="utf-8") as json_file:
                queue_data = json.load(json_file)
                
                self.total_qued = queue_data.get("total_qued", 0)
                self.number = queue_data.get("number", 0)
                self.urls = queue_data.get("urls", [])
                
                print(f"Załadowano kolejkę z {filename} ({self.number} URL)")
                return True
                
        except FileNotFoundError:
            print(f"Nie znaleziono pliku {filename}")
            return False
            

class CrawledSet:
    def __init__(self) -> None:
        self.data = {}  
        self.number = 0

    def add_url(self, url) -> None:
        hash_key = hash_url(url)
        if hash_key not in self.data:
            self.data[hash_key] = True 
            self.number += 1

    def contains_url(self, url) -> bool:
        return hash_url(url) in self.data

    def size(self) -> int:
        return self.number

    def dumptojson(self):
        json_data = {
            "data": {str(k): v for k, v in self.data.items()},
            "number": self.number
        }
        with open("crawled.json", "w", encoding="utf-8") as json_file:
            json.dump(json_data, json_file, indent=4, ensure_ascii=False)
        print(f"Zapisano {self.number} URL-i do crawled.json")

    def load_frjson(self) -> None:
        try:
            with open("crawled.json", "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                if isinstance(data.get("data"), dict):
                    self.data = {int(k): v for k, v in data["data"].items()}
                    self.number = data.get("number", 0)
                else:
                    self.data = {}
                    self.number = 0
                print(f"Załadowano {self.number} URL-i z crawled.json")
        except FileNotFoundError:
            print("Nie znaleziono pliku crawled.json - tworzę nowy plik")
            self._create_default_json()
    
    def _create_default_json(self):
        """Create a new JSON file with default structure"""
        default_data = {
            "data": {},
            "number": 0
        }
        
        with open("crawled.json", "w", encoding="utf-8") as json_file:
            json.dump(default_data, json_file, indent=2, ensure_ascii=False)
        
        self.data = {}
        self.number = 0
        print("Utworzono nowy plik crawled.json")

def hash_url(url) -> int:
    return fnvhash.fnv1a_64(url.encode('utf-8'))

def fetch_page(url) -> str:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        page = requests.get(url, headers=headers, timeout=10)
        if page.status_code != 200:
            return ""
        return page.text
    except requests.RequestException as e:
        print(f"Błąd pobierania {url}: {e}")
        return ""

def get_href(page, current_page) -> list[str]:
    result = []
    try:
        parser = BeautifulSoup(page, "html.parser")
        
        for a in parser.find_all("a"):
            href = a.get("href")
            
            if not href:
                continue
            
            full_url = urljoin(current_page, href)
            
            if not full_url.startswith("http"):
                continue
            
            result.append(full_url)
    except Exception as e:
        print(f"Błąd parsowania linków z {current_page}: {e}")
    
    return result

def prepare_document(url, html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return {
        "url": url,
        "text": text
    }

def insert_page(url, html):
    try:
        doc = prepare_document(url, html)
        collection.insert_one(doc)
        print(f"Added to db: {url}")
    except Exception as e:
        print(f"Błąd dodawania do bazy {url}: {e}")

def handle_exit(signum, frame):
    crawled_set.dumptojson()
    qe.dump_to_json() 
    sys.exit(0)


def start():
   if os.path.exists("queue.json"):
       inp = int(input("Type 1 to start over or 2 to continue: "))
   else:
       inp = 1
   
   if inp == 1:
       qe = Queue()
       crawled_set = CrawledSet()
       
       seed = input("Enter link to start crawling: ")
       qe.enque(seed)
       
   else:
       qe = Queue()
       crawled_set = CrawledSet()
       
       crawled_set.load_frjson()
       qe.load_from_json()
       
       print(f"Resuming with {qe.size()} URLs in queue")
   
   return qe, crawled_set

#--------------------
#  MAIN
#--------------------
qe = Queue()
qe.load_from_json()
crawled_set = CrawledSet()
crawled_set.load_frjson()

seed = input("enter link to start crawling: ")
qe.enque(seed)

# Fixed signal handling
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

print("Rozpoczynam crawlowanie...")

while qe.size() > 0 and crawled_set.size() < 5000:
    try:
        link = qe.deque()
        
        if crawled_set.contains_url(link):
            continue
            
        crawled_set.add_url(link)
        page = fetch_page(link)
        
        if page:
            insert_page(link, page) 
            hrefs = get_href(page, link)
            for href in hrefs:
                if not crawled_set.contains_url(href):
                    qe.enque(href)
        
        print(f"Przetworzono: {link}, łączna liczba stron: {crawled_set.size()}, w kolejce: {qe.size()}")
        
        time.sleep(0.1)
        
    except KeyboardInterrupt:
        handle_exit(signal.SIGINT, None)
    except Exception as e:
        print(f"Błąd podczas przetwarzania {link}: {e}")
        continue
crawled_set.dumptojson()
qe.dump_to_json()
