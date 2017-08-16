from gevent import monkey
monkey.patch_all()
from gevent import pool
from gevent import joinall
import requests
from lxml.etree import HTML
from urllib.parse import urljoin
from functools import partial
import sqlite3


class BaseSpider():
    def __init__(self, coroutine_num):
        self.pool = pool.Pool(coroutine_num)
        self.greenlets = []
        self.main_url = 'https://forum.xda-developers.com/'
        self.db_connect()

    def db_connect(self):
        self.con = sqlite3.connect('spi.db')
        self.cur = self.con.cursor()
        create_table_sql = """create table if not exists spider
        (id serial primary key,
        title text,
        url text);
        """
        self.cur.execute(create_table_sql)
        self.con.commit()

    def article_parse(self, response):
        html = HTML(response.text)
        url = response.url
        h1 = html.xpath('//h1').extract()[0]
        sql = "insert into spider (title, url) values(?, ?);"
        self.cur.execute(sql, (h1, url))

    def phone_forum_parse(self, response):
        html = HTML(response.text)
        article_urls = html.xpath("//div[@class='forum-childforum']/div/div[contains(@class,'thread-row')]/div/div/a/@href").extract()
        article_urls = map(lambda x: urljoin(self.main_url, x), article_urls)
        self.start_requests(article_urls, callback=self.article)

    def parse_main_page(self, response):
        html = HTML(response.text)
        phone_url_forums = html.xpath("//div[@class='bd']/ul/li/h3/a/@href").extract()
        phone_url_forums = map(lambda x: urljoin(self.main_url, x), phone_url_forums)
        self.start_requests(phone_url_forums, callback=self.phone_forum_parse)

    def start_requests(self, urls, callback):
        greenlet = self.pool.map_cb(requests.get,
            urls, callback=callback)
        greenlet.start()
        self.greenlet = greenlet

    def wait_until_finish(self):
        self.pool.join()
        self.greenlet.join()
        self.con.commit()
        self.con.close()
        # self.greenlet.join()
        # joinall(self.greenlets)


if __name__ == '__main__':
    urls = ["https://forum.xda-developers.com/"]
    spider = BaseSpider(50)
    # for i in range(50):
    #     spider.start_requests(url, spider.parse)
    # for url in urls:
    spider.start_requests(urls, spider.parse_main_page)
    spider.wait_until_finish()
