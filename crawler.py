import requests
import lxml.html
import time
import pickle
import os
import concurrent.futures

# TO-DO: delete old pickle files - keep 5-10 most recent
# TO-DO: check if (de)serialization was successful, add error handling
# TO-DO: add MIME types and swap between them
# TO-DO: changes queued_urls to queue/deque ?
# TO-DO: output article differences into a file


class Crawler:

    def __init__(self):
        self.landing_page = 'https://podpora.financnasprava.sk/'
        self.categories_xpath = './/a[starts-with(@class, "CategoryLink") or starts-with(@class, "ItemLink ' \
                                'SubcategoryLink")]/@href '
        self.articles_xpath = './/a[contains(@class, "ArticleLink")]/@href'
        self.results_folder = 'results'
        self.queued_urls = []
        self.unscraped_articles = set()
        self.scraped_urls = set()
        self.failed_scrapes = []
        self.new_articles = {}
        self.old_articles = {}
        self.changed_articles = {}
        self.timeout = 15
        self.thread_sleep = 1
        self.crawl_sleep = 0.8
        self.threads = None

    # initial scrape
    def initial_scrape(self) -> None:
        """Scrape relevant URLs from landing webpage"""
        try:
            _html = requests.get(self.landing_page, timeout=60)
            _doc = lxml.html.fromstring(_html.content)
            _categories = _doc.xpath(self.categories_xpath)
            for _category in _categories:
                if _category[:2] == '//':
                    _category = 'https:' + _category
                _category = _category.strip()
                if _category not in self.queued_urls and _category not in self.scraped_urls:
                    self.queued_urls.append(_category)
            _articles = _doc.xpath(self.articles_xpath)
            for _article in _articles:
                if _article[:2] == '//':
                    _article = 'https:' + _article
                _article = _article.strip()
                if _article not in self.unscraped_articles:
                    self.unscraped_articles.add(_article)
            self.scraped_urls.add(self.landing_page)
        except Exception as err:
            print('error in: "initial_scrape" - ', err)

    def extract_hyperlinks(self, p_url: str) -> None:
        """Scrape relevant hyperlinks from anchor tags on a webpage"""
        try:
            html = requests.get(p_url, timeout=self.timeout)
            doc = lxml.html.fromstring(html.content)
            categories = doc.xpath(self.categories_xpath)
            for category in categories:
                category = category.strip()
                if category[:2] == '//':
                    category = 'https:' + category
                if category not in self.queued_urls and category not in self.scraped_urls:
                    self.queued_urls.append(category)
            articles = doc.xpath(self.articles_xpath)
            for article in articles:
                article = article.strip()
                if article[:2] == '//':
                    article = 'https:' + article
                if article not in self.unscraped_articles:
                    self.unscraped_articles.add(article)
            self.scraped_urls.add(p_url)
        except Exception as err:
            print('error in: "extract_hyperlinks" - ', err)

    def scrape_article(self, p_url) -> str:
        """Scrape and return the text content of given article"""
        if self.thread_sleep < 0:
            self.thread_sleep = 0.5
        time.sleep(self.thread_sleep)
        try:
            html = requests.get(p_url, timeout=self.timeout)
            doc = lxml.html.fromstring(html.text)
            title_class = doc.find_class('PageTitle')
            article_class = doc.find_class('ArticleContent')
            # check if object is selected successfully
            if article_class:
                article_text = article_class[0].text_content().strip()
            else:
                article_text = ''
            if title_class:
                title_text = title_class[0].text_content().strip()
            else:
                title_text = ''
            article = title_text + '\n' + article_text
            return article
        except Exception as err:
            print('error in: "scrape_articles" - ', err)

    def crawl_urls(self) -> None:
        """Crawl through the website URLs in queue"""
        # crawl initially scraped hyperlinks and expand list
        if self.crawl_sleep < 0:
            self.crawl_sleep = 0.5
        for url in self.queued_urls:
            if url not in self.scraped_urls:
                time.sleep(self.crawl_sleep)
                # print(url[:40])
                self.extract_hyperlinks(p_url=url)

    def start_scraper_workers(self) -> None:
        """Assign scraping workload to threads and return results as a dictionary"""
        with concurrent.futures.ThreadPoolExecutor(self.threads) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.scrape_article, url): url for url in self.unscraped_articles}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f'{url} generated an exception: {exc}')
                else:
                    print(f'{url[:40]} page is {len(data)} bytes')
                    # save article content to dictionary
                    if url not in self.new_articles:
                        self.new_articles[url] = data
                    if not data:
                        print('FAILED ' + url)
                        self.failed_scrapes.append(url)
            # save a text file of unscraped pages
            # with open('failed_scrapes.txt', 'w') as f:
            #     for item in self.failed_scrapes:
            #         f.write(f'{item}\n')
            #     f.close()

    def serialize_new_results(self) -> None:
        """Save results of run into pickle file"""
        file_name = 'articles ' + time.strftime('%Y_%m_%d-%H_%M_%S') + '.pickle'
        dir_path = os.path.dirname(os.path.realpath(__file__))
        save_path = os.path.join(dir_path, self.results_folder, file_name)
        with open(save_path, 'wb') as handle:
            pickle.dump(self.new_articles, handle, protocol=pickle.DEFAULT_PROTOCOL)
        # if os.path.isfile(save_path):
        #     return True
        # else:
        #     throw error

    def deserialize_previous_results(self) -> None:
        """Load results of previous program run into dictionary for comparison"""
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.join(dir_path, self.results_folder)
        files = []
        for dir_paths, dir_names, file_names in os.walk(dir_path):
            files.extend(file_names)
            break
        most_recent_file = max(files)
        if most_recent_file:
            save_path = os.path.join(dir_path, most_recent_file)
            if os.path.isfile(save_path):
                with open(save_path, 'rb') as handle:
                    old_articles = pickle.load(handle)
                self.old_articles = old_articles
            else:
                self.old_articles = {}
        else:
            print('No previous results to load.')

    def compare_results(self) -> None:
        """Compare differences of current run results against previous"""
        for key, new_val in self.new_articles.items():
            if key in self.old_articles:
                if new_val != self.old_articles.get(key):
                    self.changed_articles[key] = new_val
            else:
                self.changed_articles[key] = new_val
        if self.changed_articles:
            print(f'{len(self.changed_articles.keys())} changes.')
        else:
            print('No changes.')

    def main(self):
        """Program starter"""
        t0 = time.time()
        cat0 = time.time()
        self.initial_scrape()
        self.crawl_urls()
        cat1 = time.time()
        s0 = time.time()
        self.start_scraper_workers()
        s1 = time.time()
        if self.new_articles:
            self.serialize_new_results()
            self.deserialize_previous_results()
            if self.old_articles:
                t1 = time.time()
                print(f'URLs extracted in {cat1 - cat0} seconds.')
                print(f'Articles scraped in: {s1 - s0} seconds.')
                print(f'Completed in {t1 - t0} seconds.')
                self.compare_results()


if __name__ == '__main__':
    print('######################################## Start ########################################')
    app = Crawler()
    app.main()
    print('######################################## Done ########################################')
