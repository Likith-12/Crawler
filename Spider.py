from urllib.request import urlopen
from link_finder import LinkFinder
from general import *
from pymongo import MongoClient

cluster = MongoClient("mongodb+srv://LikithPS:5EUyK6buL1xS1Spo@crawler.dacbdhe.mongodb.net/?retryWrites=true&w=majority")
db = cluster["Pages"]
collection = db["Pages1"]
print(db.hostinfo())

class Spider:
    project_name = ''
    base_url = ''
    domain_name = ''
    queue_file = ''
    crawled_file = ''
    queue_set = set()
    crawled_set = set()

    def __init__(self, project_name, base_url, domain_name):
        Spider.project_name = project_name
        Spider.base_url = base_url
        Spider.domain_name = domain_name
        Spider.queue_file = Spider.project_name + '/queue.txt'
        Spider.crawled_file = Spider.project_name + '/crawled.txt'
        self.boot()
        self.crawl_page('First spider', Spider.base_url)

    @staticmethod
    def boot():
        create_project(Spider.project_name)
        create_data(Spider.project_name, Spider.base_url)
        Spider.queue_set = file_to_set(Spider.queue_file)
        Spider.crawled_set = file_to_set(Spider.crawled_file)

    @staticmethod
    def crawl_page(thread_name, page_url):
        if page_url not in Spider.crawled_set:
            print(thread_name + ' now crawling ' + page_url)
            Spider.add_links_to_queue(Spider.gather_link(page_url))
            Spider.queue_set.remove(page_url)
            Spider.crawled_set.add(page_url)
            print('Queue ' + str(len(Spider.queue_set)) + '| Crawled ' + str(len(Spider.crawled_set)))
            Spider.update_files()

    @staticmethod
    def gather_link(page_url):
        html_string = ''
        try:
            response = urlopen(page_url)
            if 'text/html' in response.getheader('Content-type'):
                html_string = response.read().decode("utf-8")
            parts = html_string.split('</head>')
            body = parts[1].split('</body>')
            head = parts[0].split('<title>')
            titles = head[1].split('</')
            doc = {"title": titles[0].strip('<'), "body": body[0]}
            collection.insert_one(doc)
            finder = LinkFinder(Spider.base_url, page_url)
            finder.feed(html_string)
        except Exception as e:
            print(str(e))
            return set()
        return finder.page_links()

    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            if url in Spider.queue_set:
                continue
            if url in Spider.crawled_set:
                continue
            if Spider.domain_name not in url:
                continue
            Spider.queue_set.add(url)

    @staticmethod
    def update_files():
        set_to_file(Spider.queue_set, Spider.queue_file)
        set_to_file(Spider.crawled_set, Spider.crawled_file)
