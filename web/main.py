#!congding = utf-8

# from flask import Flask, render_template, request
from flask import Flask, render_template, request, redirect, url_for, session, g, send_from_directory, make_response, \
    flash
from search_engine import SearchEngine
from wtforms import Form, TextField, PasswordField, BooleanField, validators
from passlib.hash import sha256_crypt
import gc
import sqlite3
import configparser
import time
import json

import jieba

app = Flask(__name__)

doc_dir_path = ''
db_path = ''
global page
global keys
app.config['SECRET_KEY'] = '123456'
global rank  # 角色
global current_username


def init():
    config = configparser.ConfigParser()
    config.read('../config.ini', 'utf-8')
    global dir_path, db_path
    dir_path = config['DEFAULT']['doc_dir_path']
    db_path = config['DEFAULT']['db_path']


@app.route('/')
def main():
    init()
    return render_template('search.html', error=True)


# 读取表单数据，获得doc_ID
@app.route('/search/', methods=['POST'])
def search():
    try:
        global keys
        global checked
        checked = ['checked="true"', '', '']
        keys = request.form['key_word']
        # print(keys)
        if keys not in ['']:
            print(time.clock())
            flag, page = searchidlist(keys)
            if flag == 0:
                return render_template('search.html', error=False, username=current_username)
            docs = cut_page(page, 0)
            print(time.clock())
            return render_template('high_search.html', checked=checked, key=keys, docs=docs, page=page,
                                   error=True)
        else:
            return render_template('search.html', error=False, username=current_username)

    except:
        print('search error')


def searchidlist(key, selected=0):
    global page
    global doc_id
    se = SearchEngine('../config.ini', 'utf-8')
    flag, id_scores = se.search(key, selected)
    # 返回docid列表
    doc_id = [i for i, s in id_scores]

    # TODO 根据用户等级过滤掉部分数据
    global dir_path, db_path, rank
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    doc_id_rank = []
    for id in doc_id:
        c.execute("SELECT * FROM news WHERE id=?", (id,))
        fetch = c.fetchone()
        if fetch[5] < rank:
            doc_id_rank.append(id)
    doc_id.clear()
    doc_id = doc_id_rank.copy()
    print("**rank**: ", rank)
    print("**doc_id**: ", doc_id)
    page = []
    for i in range(1, (len(doc_id) // 10 + 2)):
        page.append(i)
    return flag, page


def cut_page(page, no):
    docs = find(doc_id[no * 10:page[no] * 10])
    return docs


# 将需要的数据以字典形式打包传递给search函数
def find(docid, extra=False):
    docs = []
    global dir_path, db_path
    # TODO 返回相关文档
    # with open('D:\\Work\\IR\\news-search-engine-master\\code\\data_new.json', encoding='utf-8') as fin:
    #     read_results = [json.loads(line.strip()) for line in fin.readlines()]
    # TODO 从数据库返回附件文档
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    for id in docid:
        c.execute("SELECT * FROM news WHERE id=?", (id,))
        fetch = c.fetchone()
        url = fetch[3]
        title = fetch[1]
        body = fetch[2]
        snippet = body[0:120] + '……'
        if len(url) > 1:
            datetime = '-'.join(url.split('/')[-4:-1])
            time = datetime
        else:
            datetime = ''
            time = datetime
            extra = False
        doc = {'url': url, 'title': title, 'snippet': snippet, 'datetime': datetime, 'time': time, 'body': body,
               'id': id, 'extra': []}
        if extra:
            temp_doc = get_k_nearest(db_path, id)
            for i in temp_doc:
                # TODO
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("SELECT * FROM news WHERE id=?", (i,))
                fetch = c.fetchone()
                title = fetch[1]
                doc['extra'].append({'id': i, 'title': title})
        docs.append(doc)

    return docs


@app.route('/search/page/<page_no>/', methods=['GET'])
def next_page(page_no):
    try:
        page_no = int(page_no)
        docs = cut_page(page, (page_no - 1))
        return render_template('high_search.html', checked=checked, key=keys, docs=docs, page=page,
                               error=True)
    except:
        print('next error')


@app.route('/search/<key>/', methods=['POST'])
def high_search(key):
    try:
        selected = int(request.form['order'])
        for i in range(3):
            if i == selected:
                checked[i] = 'checked="true"'
            else:
                checked[i] = ''
        flag, page = searchidlist(key, selected)
        if flag == 0:
            return render_template('search.html', error=False, username=current_username)
        docs = cut_page(page, 0)
        return render_template('high_search.html', checked=checked, key=keys, docs=docs, page=page,
                               error=True)
    except:
        print('high search error')


@app.route('/search/<id>/', methods=['GET', 'POST'])
def content(id):
    try:
        doc = find([id], extra=True)
        return render_template('content.html', doc=doc[0])
    except:
        print('content error')


def get_k_nearest(db_path, docid, k=5):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT * FROM knearest WHERE id=?", (docid,))
    docs = c.fetchone()
    conn.close()
    return docs[1: 1 + (k if k < 5 else 5)]  # max = 5


#########################################
# 注册登录
@app.route('/login/', methods=['POST', 'GET'])
def login():
    try:
        error = None
        if request.method == 'POST':

            username = request.form['username']
            password = request.form['password']

            conn = sqlite3.connect(db_path)
            c = conn.cursor()

            passwd_hash_tuple = c.execute(
                'SELECT password FROM users WHERE username=?', [username]).fetchone()  # return a tuple

            global rank
            rank = c.execute(
                'SELECT rank FROM users WHERE username=?', [username]).fetchone()[0]

            if not passwd_hash_tuple:
                error = 'Invalid username'
            elif not sha256_crypt.verify(password, passwd_hash_tuple[0]):
                error = 'Invalid password'
            else:
                flash('Hey %s, you are in' % username)
                session['logged_in'] = True
                session['username'] = username
                global current_username
                current_username = username
                return render_template('search.html', error=False, username=current_username)

        gc.collect()
        return render_template('login.html', error=error)

    except Exception as e:
        return str(e)


class RegistrationForm(Form):
    username = TextField('Username', [validators.Length(min=4, max=20)])
    rank = TextField('rank')
    password = PasswordField('Password',
                             [validators.Required(), validators.EqualTo('confirm', message='Passwords must match.')])
    confirm = PasswordField('Password Again')
    accept_tos = BooleanField("<small>I accept it</small>", [validators.Required()])


@app.route('/register/', methods=['POST', 'GET'])
def register():
    try:
        form = RegistrationForm(request.form)

        if request.method == 'POST' and form.validate():
            username = form.username.data
            rank = form.rank.data
            password = sha256_crypt.encrypt(
                str(form.password.data))  # 对密码加密,生成一个hash值[每次调用生成不同的hash]（pip install passlib)

            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            x = c.execute(
                'SELECT * FROM users WHERE username = ?', [username])

            if x.fetchall():
                flash("That username is already taken, please choose another")
                return render_template('register.html', form=form)

            else:
                c.execute("INSERT INTO users (username, password, rank) VALUES(?,?,?)", [
                    username, password, rank])

                conn.commit()
                conn.close()
                gc.collect()  # collect garbage

                session['logged_in'] = True
                session['username'] = username

                return redirect(url_for('login'))

        return render_template('register.html', form=form)

    except Exception as e:
        return str(e)


@app.route('/logout/')
def logout():
    # session.pop('user_id')
    # del session('user_id')
    session.clear()
    return redirect(url_for('login'))


if __name__ == '__main__':
    jieba.initialize()  # 手动初始化（可选）
    app.run()
