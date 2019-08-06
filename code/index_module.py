# -*- coding: utf-8 -*-


from os import listdir
import jieba
import sqlite3
import configparser
import json
import docx


class Doc:
    docid = 0
    date_time = ''
    tf = 0
    ld = 0

    def __init__(self, docid, date_time, tf, ld):
        self.docid = docid
        self.date_time = date_time
        self.tf = tf  # 该词项出现的次数，即词项频率(tf)
        self.ld = ld  # 该文档的长度(ld)

    def __repr__(self):
        return (str(self.docid) + '\t' + str(self.date_time) + '\t' + str(self.tf) + '\t' + str(self.ld))

    def __str__(self):
        return (str(self.docid) + '\t' + str(self.date_time) + '\t' + str(self.tf) + '\t' + str(self.ld))


class IndexModule:
    stop_words = set()
    postings_lists = {}

    config_path = ''
    config_encoding = ''

    def __init__(self, config_path, config_encoding):
        self.config_path = config_path
        self.config_encoding = config_encoding
        config = configparser.ConfigParser()
        config.read(config_path, config_encoding)
        f = open(config['DEFAULT']['stop_words_path'], encoding=config['DEFAULT']['stop_words_encoding'])
        words = f.read()
        self.stop_words = set(words.split('\n'))

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def clean_list(self, seg_list):
        '''

        :param seg_list:
        :return:
        '''
        cleaned_dict = {}
        n = 0
        for i in seg_list:
            i = i.strip().lower()
            if i != '' and not self.is_number(i) and i not in self.stop_words:
                n = n + 1
                if i in cleaned_dict:
                    cleaned_dict[i] = cleaned_dict[i] + 1
                else:
                    cleaned_dict[i] = 1
        return n, cleaned_dict

    def write_postings_to_db(self, db_path):
        '''
        写入数据库
        :param db_path:
        :return:
        '''
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute('''DROP TABLE IF EXISTS postings''')
        c.execute('''CREATE TABLE postings
                     (term TEXT PRIMARY KEY, df INTEGER, docs TEXT)''')

        for key, value in self.postings_lists.items():
            doc_list = '\n'.join(map(str, value[1]))
            t = (key, value[0], doc_list)
            c.execute("INSERT INTO postings VALUES (?, ?, ?)", t)

        conn.commit()
        conn.close()

    def construct_postings_lists(self):
        '''

        :return:
        '''
        config = configparser.ConfigParser()
        config.read(self.config_path, self.config_encoding)
        files = listdir(config['DEFAULT']['doc_dir_path'])  # 所有的网页文件
        AVG_L = 0
        # TODO 可改为json读
        with open('D:\\Work\IR\\news-search-engine-master\\code\\data_new.json', encoding='utf-8') as fin:
            read_results = [json.loads(line.strip()) for line in fin.readlines()]
        for i in read_results:
            title = i['title']
            body = i['parapraghs']
            docid = i['id']
            url = i['url']
            date_time = '-'.join(url.split('/')[-4:-1])
            seg_list = jieba.lcut(title + '。' + body, cut_all=False)  # 分词，标题+正文
            ld, cleaned_dict = self.clean_list(seg_list)

            AVG_L = AVG_L + ld
            for key, value in cleaned_dict.items():
                d = Doc(docid, date_time, value, ld)
                if key in self.postings_lists:  # 词项在不同文档中出现的次数，即文档频率(df)
                    self.postings_lists[key][0] = self.postings_lists[key][0] + 1  # df++
                    self.postings_lists[key][1].append(d)
                else:
                    self.postings_lists[key] = [1, [d]]  # [df, [Doc]]

        # for i in files:
        #     root = ET.parse(config['DEFAULT']['doc_dir_path'] + i).getroot()
        #     title = root.find('title').text  # 标题
        #     body = root.find('body').text  # 正文
        #     docid = int(root.find('id').text)  # 编号
        #     date_time = root.find('datetime').text  # 时间
        #     seg_list = jieba.lcut(title + '。' + body, cut_all=False)  # 分词，标题+正文
        #
        #     ld, cleaned_dict = self.clean_list(seg_list)
        #
        #     AVG_L = AVG_L + ld
        #
        #     for key, value in cleaned_dict.items():
        #         d = Doc(docid, date_time, value, ld)
        #         if key in self.postings_lists:
        #             self.postings_lists[key][0] = self.postings_lists[key][0] + 1  # df++
        #             self.postings_lists[key][1].append(d)
        #         else:
        #             self.postings_lists[key] = [1, [d]]  # [df, [Doc]]
        AVG_L = AVG_L / len(read_results)
        config.set('DEFAULT', 'N', str(len(read_results)))
        config.set('DEFAULT', 'avg_l', str(AVG_L))
        with open(self.config_path, 'w', encoding=self.config_encoding) as configfile:
            config.write(configfile)
        self.write_postings_to_db(config['DEFAULT']['db_path'])

    def construct_news_lists(self, db_path):
        '''
        news数据库表格
        :param db_path:
        :return:
        '''
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute('''DROP TABLE IF EXISTS news''')
        c.execute('''CREATE TABLE news
                     (id INTEGER PRIMARY KEY, title TEXT, parapraghs TEXT, url VARCHAR, file_name TEXT,rank INTEGER )''')

        with open('D:\\Work\IR\\Lab3_search_engine\\code\\data_new.json', encoding='utf-8') as fin:
            read_results = [json.loads(line.strip()) for line in fin.readlines()]
        rank = 1
        for item in read_results:
            if int(item['id']) < 300:
                rank = 1
            elif int(item['id']) < 600:
                rank = 2
            elif int(item['id']) < 900:
                rank = 3
            elif int(item['id']) < 1200:
                rank = 4
            t = (item['id'], item['title'], item['parapraghs'], item['url'], '\n'.join(item['file_name']), rank)
            c.execute("INSERT INTO news VALUES (?, ?, ?, ?, ?,?)", t)
        # for key, value in self.postings_lists.items():
        #     doc_list = '\n'.join(map(str, value[1]))
        #     t = (key, value[0], doc_list)
        #     c.execute("INSERT INTO postings VALUES (?, ?, ?)", t)

        conn.commit()
        conn.close()

    def construct_users(self, db_path):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute('''DROP TABLE IF EXISTS users''')
        c.execute('''CREATE TABLE users
                     (username VARCHAR PRIMARY KEY, password VARCHAR, rank INTEGER)''')
        t = ('111', '111', 1)
        c.execute("INSERT INTO users VALUES (?, ?, ?)", t)
        t = ('222', '222', 2)
        c.execute("INSERT INTO users VALUES (?, ?, ?)", t)
        t = ('333', '333', 3)
        c.execute("INSERT INTO users VALUES (?, ?, ?)", t)
        t = ('444', '444', 4)
        c.execute("INSERT INTO users VALUES (?, ?, ?)", t)
        conn.commit()
        conn.close()


if __name__ == "__main__":
    im = IndexModule('../config.ini', 'utf-8')
    # im.construct_postings_lists()
    # config = configparser.ConfigParser()
    im.construct_news_lists('D:\\Work\\IR\\Lab3_search_engine\\data\\ir.db')
