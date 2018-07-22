# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3

class DoubanmoviePipeline(object):
    def __init__(self, sqlite_file, sqlite_table):
        self.sqlite_file = sqlite_file
        self.sqlite_table = sqlite_table

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlite_file=crawler.settings.get('SQLITE_FILE'),  # 从 settings.py 提取
            sqlite_table=crawler.settings.get('SQLITE_TABLE', 'items')
        )

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.sqlite_file)
        self.cur = self.conn.cursor()
        self.cur.execute('CREATE TABLE IF NOT EXISTS {0} (num text(20) primary key, movie text(20), star text(20), introduce text(20), evaluate text(20), describe text(100))'.format(self.sqlite_table))

    def close_spider(self, spider):
        self.conn.close()

    #将数据写入sqlite3数据库
    def process_item(self, item, spider):
        data = dict(item)
        insert_sql = "insert into {0}(num, movie, star, introduce,evaluate, describe) values ('{1}','{2}','{3}','{4}','{5}','{6}')".format(self.sqlite_table, data['serial_number'], data['movie_name'], data['star'], data['introduce'], data['evaluate'], data['describe'] )
        print(insert_sql)
        self.cur.execute(insert_sql)
        self.conn.commit()

        return item
