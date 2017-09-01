# encoding: utf-8
# !/usr/bin/env python
# author qinwei

import time
import requests
import re
import MySQLdb


#头部信息
headers = {
    "Host": "www.zhihu.com",
    "Referer": "https://www.zhihu.com/",
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87'
}


#获取某个话题下热门问题的ID
def question_id():
    url1 = "https://www.zhihu.com/topic/19579632/top-answers"
    pattern = re.compile(r'<a class="question_link" href="/question/(.*?)" '
                         r'target="_blank" data-id="(.*?)" data-za-element-name="Title">(.*?)</a>', re.S)
    r = session.get("https://www.zhihu.com/topic/19579632/top-answers", headers=headers)
    items = re.findall(pattern, r.text)
    with open("id.txt", "w") as f:
        for item in items:
            f.write(item[0].encode("utf-8"))
            f.write("\n")
        time.sleep(5)
        for i in range(2, 28, 1):
            url = url1 + "?page=" + str(i)
            r = session.get(url, headers=headers)
            items = re.findall(pattern, r.text)
            for item in items:
                f.write(item[0].encode("utf-8"))
                f.write("\n")
            time.sleep(5)
        f.close()
    print "写入完成"

#初始化session
def init():
    ua = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit'
                        '/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    s = requests.Session()
    s.headers.update(ua)
    #_ = s.get(url, verify=False)
    s.headers.update({"authorization": "oauth c3cef7c66a1843f8b3a9e6a1e3160e20"})
    return s

request_retry = requests.adapters.HTTPAdapter(max_retries=3)
session = init()
session.mount('https://', request_retry)


#获取答案，设置偏移，获取答案个数；
def fetch_answer(s, qid, limit, offset):
    params = {
        'sort_by': 'default',
        'include': 'data[*].is_normal,is_collapsed,annotation_action,annotation_detail,collapse_reason,is_sticky,'
                   'collapsed_by,suggest_edit,comment_count,can_comment,'
                   'content,editable_content,voteup_count,reshipment_settings,comment_permission,mark_infos,'
                   'created_time,updated_time,review_info,relationship.is_authorized,is_author,voting,is_thanked,'
                   'is_nothelp,upvoted_followees;data[*].author.follower_count,badge[?(type=best_answerer)].topics',
        'limit': limit,
        'offset': offset
    }
    url = "https://www.zhihu.com/api/v4/questions/"+qid+"/answers"
    respones = s.get(url, params=params, verify=False)
    return respones

#获取答案
def fetch_all_answers(url):
    q_id = url.split('/')[-1]
    offset = 0
    limit = 20
    answers = []
    is_end = False
    while not is_end:
        ret = fetch_answer(session, q_id, limit, offset)
        answers += ret.json()['data']
        is_end = ret.json()['paging']['is_end']
        offset += limit
    return answers


#获取评论，设置偏移，获取评论个数；
def get_comment(s, aid, limit, offset):
    params = {
        'include': 'data[*].author,collapsed,reply_to_author,disliked,content,voting,vote_count,is_parent_author,is_author',
        'order': 'normal',
        'limit': limit,
        'offset': offset,
        'status': 'open'
     }
    url = "https://www.zhihu.com/api/v4/answers/"+str(aid)+"/comments"
    response = s.get(url, params=params, verify=False)
    return response

#获取评论
def get_all_comment(answer_id):
    offset = 0
    limit = 20
    comment = []
    is_end = False
    while not is_end:
        ret = get_comment(session, answer_id, limit, offset)
        comment += ret.json()['data']
        is_end = ret.json()['paging']['is_end']
        #print "c_Offset: ", offset
        #print "c_is_end: ", is_end
        offset += limit
    return comment


#将获取到的答案写入数据库
def main():
    db = MySQLdb.connect("localhost", "root", "1234", "zhihu", charset="utf8" )
    cursor = db.cursor()
    with open("id4.txt", "r") as f:
        lines = f.readlines()
    f.close()
    for line in lines:
        line = line.strip()
        ids = line
        url = "https://www.zhihu.com/question/" + ids
        print "正在爬取问题：",url
        flags = []
        answers = []
        try:
            answers = fetch_all_answers(url)

            pattern = re.compile(r'<.*?>')
            flag = True
            #写入回答
            print "正在写入回答id:",ids
            count = 0
            for answer in answers:
                if len(answer) > 0:
                    r = session.get(url, headers=headers)
                    pattern = re.compile(r'class="List-headerText"><span>(.*?)</span></h')
                    match = re.search(pattern, r.text)
                    pattern = re.compile(r'\d+')
                    match = re.search(pattern, match.group())
                    if match:
                        answer_count = match.group()
                    if flag:
                        localtime = time.localtime(answer["question"]["created"])
                        created_time = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
                        sql = "INSERT INTO question(question_id, question, author, time, answer_count) \
                        VALUES (%d, '%s', '%s', '%s', %d)" % \
                        (int(ids), answer['question']['title'], answer["question"]["author"]["name"],
                         created_time, int(answer_count))
                        try:
                            # 执行sql语句
                            cursor.execute(sql)
                            # 提交到数据库执行
                            db.commit()

                        except MySQLdb.Error, e:
                            #发生错误时回滚
                            print "数据库错误，原因%d: %s" % (e.args[0], e.args[1])
                            db.rollback()

                        localtime = time.localtime(answer['updated_time'])
                        created_time = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
                        sql = "INSERT INTO answer(question_id, answer_id, answer_time, answer_author, votes \
                              , answer, comment_count) \
                        VALUES (%d, %d, '%s', '%s', '%s', '%s', %d)" % \
                        (int(ids), int(answer["id"]), created_time,
                          answer["author"]["name"], answer["voteup_count"], re.sub(pattern, "", answer["content"]),
                         int(answer["comment_count"]))
                        try:
                             # 执行sql语句
                            cursor.execute(sql)
                            # 提交到数据库执行
                            db.commit()
                            flags.append(True)
                            count = count + 1
                        except MySQLdb.Error, e:
                            #发生错误时回滚
                            flags.append(False)
                            count = count + 1
                            print "数据库错误，原因%d: %s" % (e.args[0], e.args[1])
                            db.rollback()
                        flag = False
                    else:
                        localtime = time.localtime(answer['updated_time'])
                        created_time = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
                        sql = "INSERT INTO answer(question_id, answer_id, answer_time, answer_author, votes \
                              , answer, comment_count) \
                        VALUES (%d, %d, '%s', '%s', '%s', '%s', %d)" % \
                        (int(ids), int(answer["id"]), created_time,
                          answer["author"]["name"], answer["voteup_count"], re.sub(pattern, "", answer["content"]),
                         int(answer["comment_count"]))
                        try:
                            # 执行sql语句
                            cursor.execute(sql)
                            # 提交到数据库执行
                            db.commit()
                            flags.append(True)
                            count = count + 1
                        except MySQLdb.Error, e:
                            #发生错误时回滚
                            flags.append(False)
                            count = count + 1
                            print "数据库错误，原因%d: %s" % (e.args[0], e.args[1])
                            db.rollback()
                    print "正在写入第%d个回答，共%s个" % (count, str(answer_count))
        except requests.exceptions.ConnectionError:
            print "链接超时",url
        #写入评论
        print len(flags)
        print "正在写入评论..."
        i = 0
        if len(answers) > 0:
            for answer in answers:
                if flags[i] and int(answer["comment_count"] > 0):
                    try:
                        comments = get_all_comment(answer['id'])
                        time.sleep(6)
                        for comment in comments:
                            #print comment
                            localtime = time.localtime(comment["created_time"])
                            created_time = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
                            sql = "INSERT INTO comment(question_id, answer_id, comment_time, comment_author, \
                                  comment, vote) VALUES(%d, %d, '%s', '%s', '%s', %d) "%\
                                  (int(ids), int(answer['id']), created_time,comment["author"]["member"]["name"]
                                   , comment["content"], comment["vote_count"])
                            try:
                               # 执行sql语句
                                cursor.execute(sql)
                               # 提交到数据库执行
                                db.commit()
                            except MySQLdb.Error, e:
                                #发生错误时回滚
                                print "数据库错误，原因%d: %s" % (e.args[0], e.args[1])
                                db.rollback()
                    except requests.exceptions.ConnectionError:
                        print "评论链接超时id:", answer['id']
                i = i + 1
            print "问题%s写入完成" % ids
        time.sleep(30)

if __name__ == '__main__':
    main()
