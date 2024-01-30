import string
from collections import defaultdict
import time as _time
import openai
import requests
from konlpy.tag import Okt
import pymysql
import dotenv
import os
import json
from datetime import datetime, time, timedelta
import platform

from tf_idf_extract import OktTFIDFExtract

# 서버에서 실행시(UCT 기준)
# today = (datetime.now() + timedelta(hours=9)) - timedelta(days=1)
# 로컬에서 실행 시
# today = datetime.now() - timedelta(days=1)

# server
# with open('/home/ubuntu/movieboard-data/SentiWord_info.json', 'r', encoding='utf-8') as f:
# local
# with open('./SentiWord_info.json', 'r', encoding='utf-8') as f:
#     sentiDict = json.load(f)

stopword_file_path = './stopword.txt'

# 실행 서버간 자동화
if 'aws' in platform.platform():
    print('RUN IN AWS SERVER')
    today = (datetime.now() + timedelta(hours=9)) - timedelta(days=1)
    sentiroot = '/home/ubuntu/movieboard-data/SentiWord_info.json'
    stopword_file_path = '/home/ubuntu/movieboard-data/stopword.txt'
else:
    print('RUN IN LOCAL SERVER')
    today = datetime.now() - timedelta(days=1)
    sentiroot = './SentiWord_info.json'
    stopword_file_path = './stopword.txt'

with open(sentiroot, 'r', encoding='utf-8') as f:
    sentiDict = json.load(f)

str_today = today.strftime('%Y-%m-%d')
reportType = ['daily', 'weekly', 'monthly']

keywordFilter = ['Noun', 'Adjective', 'Verb']

portalNames = ['naver_blog',
               'naver_cafe',
               'naver_kin',
               'naver_news',
               ]

communityNames = ['community_mlbpark',
                  'community_ilbe',
                  'community_ruliweb',
                  'community_ygosu',
                  'community_fmkorea',
                  'community_dogdrip',
                  'community_dmain',
                  'community_powder',
                  'community_feko',
                  'community_remonterrace',
                  'community_girlstime',
                  'community_nate',
                  'community_bobaedream',
                  'community_extmovie',
                  'community_inven',
                  'community_theqoo',
                  ]

snsNames = ['twitter', 'instagram']

communityTotalNames = ['naver_blog',
                       'naver_cafe',
                       'naver_kin',
                       'naver_news',
                       'community_mlbpark',
                       'community_ilbe',
                       'community_ruliweb',
                       'community_ygosu',
                       'community_fmkorea',
                       'community_dogdrip',
                       'community_dmain',
                       'community_powder',
                       'community_feko',
                       'community_remonterrace',
                       'community_girlstime',
                       'community_nate',
                       'community_bobaedream',
                       'community_extmovie',
                       'community_inven',
                       'community_theqoo',
                       ]


# DB 연결
def db_connect():
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

    # color_board
    # color_env = '.colorenv'
    # dotenv.load_dotenv(dotenv_path=color_env)

    # db = pymysql.connect()
    if os.environ['DB_SW'] == 'movie':
        print('movieboard conn')
        conn = pymysql.connect(host=os.environ['dbhost'],
                               user=os.environ['dbuser'],
                               password=os.environ['dbpassword'],
                               db=os.environ['dbname'],
                               )
    elif os.environ['DB_SW'] == 'color':
        print('colorboard conn')
        conn = pymysql.connect(host=os.environ['color_db_host'],
                               user=os.environ['color_db_user'],
                               password=os.environ['color_db_passwd'],
                               db=os.environ['color_db_database'],
                               )

    return conn


# 키워드 큐 데이터 가져오기
def get_keywordQueues():
    conn = db_connect()
    cur = conn.cursor()

    # sql = 'SELECT id, keyword, start_datetime, end_datetime FROM keywordQueue WHERE status != "end"'
    sql = 'SELECT PRO.id, KQ.keyword, KQ.start_datetime, KQ.end_datetime, PRO.type, PRO.reportKeywords FROM keywordQueue AS KQ JOIN project AS PRO ON KQ.id = PRO.keywordQueueId WHERE KQ.status != "end"'
    cur.execute(sql)
    keywords = cur.fetchall()

    return keywords


# 키워트 큐에서 endtime 지난 큐 status end로 변경
# 리턴값으로 키워드 작업 필요한 큐만 리턴
def keywordQueue_check(queues):
    conn = db_connect()
    cur = conn.cursor()
    except_idx = []
    result = []

    # today = datetime.now()

    # for queue in queues:
    for idx, queue in enumerate(queues):
        ed_datetime = queue[3]

        date_diff = today - ed_datetime
        if (date_diff.days > 0):
            # print(queue[0], '번 키워드 큐가 종료되었습니다.')
            sql = 'UPDATE keywordQueue SET status = "end" WHERE id = %s'
            # print(sql % (queue[0]))
            except_idx.append(idx)

            cur.execute(sql, queue[0])
    conn.commit()

    for i in range(len(queues)):
        if i not in except_idx:
            result.append(queues[i])

    return result


# 키워드 추출을 위한 contents 가져오기
def get_contents(keyword, st_datetime, ed_datetime):
    # print('KEYWORD : ', keyword)
    conn = db_connect()
    cur = conn.cursor()
    keyword = tuple(keyword)
    sql = 'SELECT title, content, detail_content, write_datetime, keyword, type, kind\
           FROM keywordCollection\
           WHERE keyword IN %s AND (write_datetime BETWEEN %s AND %s)'
    cur.execute(sql, (keyword, st_datetime, ed_datetime))
    contents = cur.fetchall()

    return contents


def get_snscontents(keyword, st_datetime, ed_datetime):
    conn = db_connect()
    cur = conn.cursor()
    keyword = tuple(keyword)
    sql = 'SELECT title, content, detail_content, write_datetime, keyword, type, kind\
           FROM snsCollection\
           WHERE keyword IN %s AND (write_datetime BETWEEN %s AND %s)'
    cur.execute(sql, (keyword, st_datetime, ed_datetime))
    contents = cur.fetchall()

    return contents


# 데이터 DB 저장
# 필요 컬럼 "keyword, type, date, total, portal, community, twitter, insta"
def push_totalCount(keyword, type, totalCount, gpt_data):
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_totalCount(keyword, type, date, total, portal, community, twitter, insta, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (
        keyword, type, str_today, totalCount['total'], totalCount['portal'], totalCount['community'],
        totalCount['twitter'],
        totalCount['insta'],
        gpt_data))
    conn.commit()

    print('totalCount DONE')


# 필요 컬럼 "keyword, type, date, data"
def push_totalGraph(keyword, type, result, gpt_data):
    # mysql 에서 json 형태로 저장하기 위해 dumps 사용
    dump = json.dumps(result)

    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_totalGraph(keyword, type, date, data, gptData)\
           VALUES(%s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword, type, str_today, dump, gpt_data))
    conn.commit()

    print('totalGraph DONE')


# 필요 컬럼 "keyword, type, date, news, blog, kin, cafe"
def push_portalAnalysis(keyword, type, portalAnalysis, gpt_data):
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_portalAnalysis(keyword, type, date, news, blog, kin, cafe, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword,
                      type,
                      str_today,
                      json.dumps(portalAnalysis['naver_news']),
                      json.dumps(portalAnalysis['naver_blog']),
                      json.dumps(portalAnalysis['naver_kin']),
                      json.dumps(portalAnalysis['naver_cafe']),
                      gpt_data
                      )
                )
    conn.commit()
    print('portalAnalysis DONE')


# 필요 칼럼 "keyword, type, date, siteCount, keywordCount, keywordCommunity, keywordRelation"
def push_keywordAnalysis(keyword, type, siteCount, keywordCount, keywordCommunity, keywordRelation, gpt_data):
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_keywordAnalysis(keyword, type, date, siteCount, keywordCount, keywordCommunity, keywordRelation, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword,
                      type,
                      str_today,
                      json.dumps(siteCount),
                      json.dumps(keywordCount),
                      json.dumps(keywordCommunity),
                      json.dumps(keywordRelation),
                      gpt_data
                      )
                )
    conn.commit()

    print('keywordAnalysis DONE')


# 필요 칼럼 "keyword, type, date, keywordCount"
def push_snsAnalysis(keyword, type, snsAnalysis, gpt_data):
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_snsAnalysis(keyword, type, date, twitter, instagram, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword,
                      type,
                      str_today,
                      json.dumps(snsAnalysis['twitter']),
                      json.dumps(snsAnalysis['instagram']),
                      gpt_data
                      )
                )
    conn.commit()

    print('snsAnalysis DONE')


# 필요 칼럼 "keyword, type, date, graph, table"
def push_compare(keyword, type, compare_graph, compare_table, gpt_data):
    graph = json.dumps(compare_graph, ensure_ascii=False)
    table = json.dumps(compare_table, ensure_ascii=False)
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_compare(keyword, type, date, graph, tableCount, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword,
                      type,
                      str_today,
                      graph,
                      table,
                      gpt_data))
    conn.commit()

    print('compare DONE')


# 필요 칼럼 "keyword, type, date, keywordList, keywordCount, gpt_data"
def push_keywordCount(keyword, type, keywordList, keywordCount, gpt_data):
    # today = datetime.now().date()
    conn = db_connect()
    cur = conn.cursor()
    sql = 'INSERT INTO report_v2_keywordCount(keyword, type, date, keywordList, keywordCount, gptData)\
           VALUES(%s, %s, %s, %s, %s, %s)'
    cur.execute(sql, (keyword,
                      type,
                      str_today,
                      json.dumps(keywordList),
                      json.dumps(keywordCount),
                      gpt_data))
    conn.commit()

    print('keywordCount DONE')


# 데이터 처리
# 리포트 '전체 버즈량 분석' 게시글 카운트
def make_totalCount(project, contentList, sns_contentList, type):
    totalCount = {'total': 0, 'portal': 0, 'community': 0, 'twitter': 0, 'insta': 0}
    for content in contentList:
        if content[-2] in portalNames:
            totalCount['total'] += 1
            totalCount['portal'] += 1
        elif content[-2] in communityNames:
            totalCount['total'] += 1
            totalCount['community'] += 1

    for sns_content in sns_contentList:
        if sns_content[-2] == 'twitter':
            totalCount['total'] += 1
            totalCount['twitter'] += 1
        elif sns_content[-2] == 'instagram':
            totalCount['total'] += 1
            totalCount['insta'] += 1

    gpt_data = openai_get_response('[여기서는 감정 분석 말고 언급량만 고려해줘 / 한글 150자 내로 답해줘] %s 기준 버즈량 분석 데이터 : %s' % (
    type, json.dumps(totalCount, ensure_ascii=False)))

    return push_totalCount(project[1], type, totalCount, gpt_data)


# 리포트 '전체 버즈량 분석' 그래프
def meke_totalGraph(project, contentList, sns_contentList, st_datetime, ed_datetime, type):
    result = {}

    if type == 'daily':
        for time in range(0, 21, 4):
            time_idx = str(time) + '~' + str(time + 3)
            result[time_idx] = {'instagram': 0, 'twitter': 0, 'community': 0, 'portal': 0}

        for content in contentList:
            time_idx = str(int(content[3].hour / 4) * 4) + '~' + str(int(content[3].hour / 4) * 4 + 3)
            if content[-2] in portalNames:
                result[time_idx]['portal'] += 1
            elif content[-2] in communityNames:
                result[time_idx]['community'] += 1

        for sns_content in sns_contentList:
            time_idx = str(int(sns_content[3].hour / 4) * 4) + '~' + str(int(sns_content[3].hour / 4) * 4 + 3)
            result[time_idx][sns_content[-2]] += 1

    elif type == 'weekly':
        for date in range(0, 7):
            time_idx = str(st_datetime.date() + timedelta(days=date))
            result[time_idx] = {'instagram': 0, 'twitter': 0, 'community': 0, 'portal': 0}

        for content in contentList:
            time_idx = str(content[3].date())
            if content[-2] in portalNames:
                result[time_idx]['portal'] += 1
            elif content[-2] in communityNames:
                result[time_idx]['community'] += 1

        for sns_content in sns_contentList:
            time_idx = str(sns_content[3].date())
            result[time_idx][sns_content[-2]] += 1

    elif type == 'monthly':
        for week in range(1, 5):
            time_idx = str(week) + '주차'
            result[time_idx] = {'instagram': 0, 'twitter': 0, 'community': 0, 'portal': 0}

        for content in contentList:
            time_dix = str(int(content[3].day // 8) + 1) + '주차'
            if content[-2] in portalNames:
                result[time_dix]['portal'] += 1
            elif content[-2] in communityNames:
                result[time_idx]['community'] += 1

        for sns_content in sns_contentList:
            time_idx = str(int(sns_content[3].day // 8) + 1) + '주차'
            result[time_idx][sns_content[-2]] += 1

    gpt_data = openai_get_response(
        '[여기서는 감정 분석 말고 언급량만 고려해줘] %s 기준 버즈량 분석 데이터 : %s' % (type, json.dumps(result, ensure_ascii=False)))

    # print(result)
    return push_totalGraph(project[1], type, result, gpt_data)


# 리포트 '포털 통계 분석'
def make_portalAnalysis(project, contentList, type):
    portalAnalysis = {}
    kind = {0: 'neg', 1: 'neu', 2: 'pos'}
    for site in portalNames:
        portalAnalysis[site] = {'total': 0, 'pos': 0, 'neu': 0, 'neg': 0}

    for content in contentList:
        if content[-2] in portalNames and content[-1] <= 2:
            portalAnalysis[content[-2]]['total'] += 1
            portalAnalysis[content[-2]][kind[content[-1]]] += 1

    gpt_data = openai_get_response(
        '[neg = 부정, neu = 중립, pos = 긍정, total = 전체] 포털 통계 분석 : %s' % (json.dumps(portalAnalysis, ensure_ascii=False)))

    # print(portalAnalysis)
    return push_portalAnalysis(project[1], type, portalAnalysis, gpt_data)


# 리포트 커뮤니티 통계 분석
def make_keywordAnalysis(project, contentList, type):
    okt = Okt()
    # 프로젝트별 저장될 키워드 결과값
    keyword_dic = {}
    # 부정키워드 9개 추출
    negKeyword_dic = {}
    # 사이트별 악성글 현황
    negSite_count = {}
    negKeyword_community = {}
    for site in communityTotalNames:
        negSite_count[site] = 0
        negKeyword_community[site] = {}

    # 게시글 별 형태소 분석
    for contents in contentList:
        if contents[2] is None:
            keywordAnalysis = okt.pos(contents[1], norm=True, stem=True)
        else:
            keywordAnalysis = okt.pos(contents[2], norm=True, stem=True)

        # 필요 키워드 추출 후 갯수 카운트
        for keyword in keywordAnalysis:
            if keyword[1] in keywordFilter:
                if keyword[0] in keyword_dic:
                    keyword_dic[keyword[0]] += 1
                else:
                    keyword_dic[keyword[0]] = 1

    keyword_dic = sorted(keyword_dic.items(), key=lambda x: x[1], reverse=True)

    # 부정 키워드 10개 추출
    for keyword in keyword_dic:
        idx = next((i for i, item in enumerate(sentiDict) if item["word"] == keyword[0]), -1)
        if idx != -1 and int(sentiDict[idx]['polarity']) < 0:
            negKeyword_dic[keyword[0]] = keyword[1]
        # 최대 9개 부정 키워드 추출
        if len(negKeyword_dic) == 9:
            break

    # 사이트별 악성글 현황
    for contents in contentList:
        if contents[-1] == 0:
            negSite_count[contents[-2]] += 1
    ## 부정글 갯수 카운트 정렬
    negSite_count = sorted(negSite_count.items(), key=lambda x: x[1], reverse=True)

    # 키워드별 악성글 현황
    ## 키워드별 악성글 현황 딕셔너리 생성
    negKeyword_count = dict.fromkeys(negKeyword_dic.keys(), 0)
    ## 악성키워드-커뮤니티 딕셔너리 생성
    for site in negKeyword_community:
        negKeyword_community[site] = dict.fromkeys(negKeyword_dic.keys(), 0)

    ## 키워드 관련 악성 키워드 수집
    negKeyword_relation_dic = {}
    negKeyword_relation_dump = {}
    for (negKeyword, x) in negKeyword_dic.items():
        negKeyword_relation_dic[negKeyword] = {}
        negKeyword_relation_dump[negKeyword] = {}
    for contents in contentList:
        if contents[2] is None:
            keywordAnalysis = okt.pos(contents[1], norm=True, stem=True)
        else:
            keywordAnalysis = okt.pos(contents[2], norm=True, stem=True)

        # 사이트별 악성글 현황 카운트
        for (keyword, x) in keywordAnalysis:
            if keyword in negKeyword_count:
                negKeyword_count[keyword] += 1
            # 키워드-커뮤니티별 악성글 현황 카운트
            if keyword in negKeyword_dic:
                negKeyword_community[contents[-2]][keyword] += 1

        # 키워드별 악성글 현황 카운트
        keywordAnalysis_dump = []
        for (x, y) in keywordAnalysis:
            if y in keywordFilter:
                keywordAnalysis_dump.append(x)

        for (negKeyword, x) in negKeyword_dic.items():
            if negKeyword in keywordAnalysis_dump:
                for keyword in keywordAnalysis_dump:
                    # 부정 키워드 확인
                    idx = next((i for i, item in enumerate(sentiDict) if item["word"] == keyword[0]), -1)
                    if idx == -1 or int(sentiDict[idx]['polarity']) >= 0:
                        continue
                    try:
                        negKeyword_relation_dump[negKeyword][keyword] += 1
                    except:
                        negKeyword_relation_dump[negKeyword][keyword] = 1

    # 부정키워드 관련 키워드 재 정렬
    for (negKeyword, values) in negKeyword_relation_dump.items():
        # print(negKeyword)
        dump = sorted(values.items(), key=lambda x: x[1], reverse=True)
        # print(dump[:5])
        negKeyword_relation_dic[negKeyword] = dump[:3]

    gpt_data = openai_get_response(
        '사이트 카운트: %s, 부정 키워드 카운트: %s, 사이트 별 부정 키워드: %s, 부정 키워드 관계: %s' % (
        json.dumps(negSite_count, ensure_ascii=False), json.dumps(negKeyword_count, ensure_ascii=False),
        json.dumps(negKeyword_community, ensure_ascii=False), json.dumps(negKeyword_relation_dic, ensure_ascii=False)))

    return push_keywordAnalysis(project[1], type, negSite_count, negKeyword_count, negKeyword_community,
                                negKeyword_relation_dic, gpt_data)


# 리포트 SNS 통계 분석
def make_snsAnalysis(project, sns_contentList, type):
    snsAnalysis = {}
    kind = {0: 'neg', 1: 'neu', 2: 'pos'}
    for site in snsNames:
        snsAnalysis[site] = {'total': 0, 'pos': 0, 'neu': 0, 'neg': 0}

    for content in sns_contentList:
        if content[-1] not in kind.keys():
            continue
        snsAnalysis[content[-2]]['total'] += 1
        snsAnalysis[content[-2]][kind[content[-1]]] += 1

    gpt_data = openai_get_response(
        '[neg = 부정, neu = 중립, pos = 긍정, total = 전체] SNS 통계 분석 : %s' % (json.dumps(snsAnalysis, ensure_ascii=False)))

    return push_snsAnalysis(project[1], type, snsAnalysis, gpt_data)


# 리포트 경쟁사 비교 분석
def make_compare(project, contentList, sns_contentList, st_datetime, ed_datetime, type):
    table_columns = ['portal', 'community', 'sns', 'news']

    # 경쟁작 키워드 수집
    conn = db_connect()
    cur = conn.cursor()

    sql = 'SELECT keyword FROM keywordQueue WHERE id in (SELECT keywordQueueId FROM project WHERE relatedProject = %s AND type = "movie")'
    cur.execute(sql, project[0])
    keywords = cur.fetchall()
    if len(keywords) == 0:
        return push_compare(project[1], type, {"sns": -1}, {"compare": -1}, None)
    keywords = [x for x, in keywords]
    # 경쟁작 게시글 수집
    compare_contentList = get_contents(keywords, st_datetime, ed_datetime)
    compare_sns_contentList = get_snscontents(keywords, st_datetime, ed_datetime)

    # for content in compare_contentList:
    #     if content[-2] in portalNames[0:3]:
    #         print(content)
    # exit()

    # 경쟁작 게시글 카운트
    compare_graph = {'total': {project[1]: 0, },
                     'portal': {project[1]: 0, },
                     'community': {project[1]: 0, },
                     'sns': {project[1]: 0, },
                     'news': {project[1]: 0, }
                     }

    for keyword in keywords:
        compare_graph['total'][keyword] = 0
        compare_graph['portal'][keyword] = 0
        compare_graph['community'][keyword] = 0
        compare_graph['sns'][keyword] = 0
        compare_graph['news'][keyword] = 0

    # 해당 프로젝트 갯수 체크
    compare_graph['total'][project[1]] = len(contentList) + len(sns_contentList)
    for content in contentList:
        if content[-2] in portalNames[0:3]:
            compare_graph['portal'][project[1]] += 1
        elif content[-2] in communityNames:
            compare_graph['community'][project[1]] += 1
        elif content[-2] in portalNames[3]:
            compare_graph['news'][project[1]] += 1
    compare_graph['total'][project[1]] += len(sns_contentList)
    compare_graph['sns'][project[1]] = len(sns_contentList)

    # 경쟁작 갯수 체크
    for content in compare_contentList:
        compare_graph['total'][content[-3]] += 1
        if content[1] in portalNames[0:3]:
            compare_graph['portal'][content[-3]] += 1
        elif content[1] in communityNames:
            compare_graph['community'][content[-3]] += 1
        elif content[1] in portalNames[3]:
            compare_graph['news'][content[-3]] += 1
    for snscontent in compare_sns_contentList:
        compare_graph['total'][snscontent[-3]] += 1
        compare_graph['sns'][snscontent[-3]] += 1

    # 테이블 데이터 생성
    compare_table = {}
    contentList = sorted(contentList, key=lambda x: x[3])
    sns_contentList = sorted(sns_contentList, key=lambda x: x[3])

    movies = [project[1]] + keywords
    # 시간(daily) 혹은 일자(weekly, monthly)별 카운트
    if type == 'daily':
        for time in range(0, 21, 4):
            time_idx = str(time) + '~' + str(time + 3)
            compare_table[time_idx] = {}
            for movie in movies:
                compare_table[time_idx][movie] = {}
                for table_column in table_columns:
                    compare_table[time_idx][movie][table_column] = 0

        # 해당 프로젝트 갯수 체크
        for content in contentList:
            time_idx = str(int(content[3].hour / 4) * 4) + '~' + str(int(content[3].hour / 4) * 4 + 3)
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in sns_contentList:
            time_idx = str(int(content[3].hour / 4) * 4) + '~' + str(int(content[3].hour / 4) * 4 + 3)
            compare_table[time_idx][content[-3]]['sns'] += 1

        # 경쟁작 갯수 체크
        for content in compare_contentList:
            time_idx = str(int(content[3].hour / 4) * 4) + '~' + str(int(content[3].hour / 4) * 4 + 3)
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in compare_sns_contentList:
            time_idx = str(int(content[3].hour / 4) * 4) + '~' + str(int(content[3].hour / 4) * 4 + 3)
            compare_table[time_idx][content[-3]]['sns'] += 1

    elif type == 'weekly':
        for date in range(0, 7):
            time_idx = str(st_datetime.date() + timedelta(days=date))
            compare_table[time_idx] = {}
            for movie in movies:
                compare_table[time_idx][movie] = {}
                for table_column in table_columns:
                    compare_table[time_idx][movie][table_column] = 0

        # 해당 프로젝트 갯수 체크
        for content in contentList:
            time_idx = str(content[3].date())
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in sns_contentList:
            time_idx = str(content[3].date())
            compare_table[time_idx][content[-3]]['sns'] += 1

        # 경쟁작 갯수 체크
        for content in compare_contentList:
            time_idx = str(content[3].date())
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in compare_sns_contentList:
            time_idx = str(content[3].date())
            compare_table[time_idx][content[-3]]['sns'] += 1

    elif type == 'monthly':
        for week in range(1, 5):
            time_idx = str(week) + '주차'
            compare_table[time_idx] = {}
            for movie in movies:
                compare_table[time_idx][movie] = {}
                for table_column in table_columns:
                    compare_table[time_idx][movie][table_column] = 0

        # 해당 프로젝트 갯수 체크
        for content in contentList:
            time_delta = (content[3] - st_datetime).days
            time_idx = str(time_delta // 8 + 1) + '주차'
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in sns_contentList:
            time_delta = (content[3] - st_datetime).days
            time_idx = str(time_delta // 8 + 1) + '주차'
            compare_table[time_idx][content[-3]]['sns'] += 1

        # 경쟁작 갯수 체크
        for content in compare_contentList:
            time_delta = (content[3] - st_datetime).days
            time_idx = str(time_delta // 8 + 1) + '주차'
            if content[-2] in portalNames[0:3]:
                compare_table[time_idx][content[-3]]['portal'] += 1
            elif content[-2] in communityNames:
                compare_table[time_idx][content[-3]]['community'] += 1
            elif content[-2] in portalNames[3]:
                compare_table[time_idx][content[-3]]['news'] += 1

        for content in compare_sns_contentList:
            time_delta = (content[3] - st_datetime).days
            time_idx = str(time_delta // 8 + 1) + '주차'
            compare_table[time_idx][content[-3]]['sns'] += 1

    gpt_data = openai_get_response('경쟁작 비교분석 데이터: %s / %s' % (
    json.dumps(compare_graph, ensure_ascii=False), json.dumps(compare_table, ensure_ascii=False)))

    return push_compare(project[1], type, compare_graph, compare_table, gpt_data)


# 리포트 배우, 영화 부정 키워드 워드 클라우드 데이터 생성
def make_wordCloud_movie(project_info, id, keyword, content_list, st_date, ed_date, type, report_type):
    # 영화 정보 불러오기 URL
    # MovieBoard BE REST API
    MOVIE_INFO_URL = "https://api.movieboard.co.kr/projects/detail/%d"

    # 만약 프로젝트 타입이 영화 일 경우
    if type == 'movie':
        # 워드 클라우드 데이터 생성 쿼리 원본
        origin_sql = 'SELECT title, content, detail_content, write_datetime, keyword, type, kind\
           FROM keywordCollection\
           WHERE keyword = %s AND (write_datetime BETWEEN %s AND %s)'

        # DB 연결
        conn = db_connect()
        cur = conn.cursor()

        # 키워드(배우) 배열 생성
        keyword_actor_list = []
        for project in project_info:
            if project[4] == 'actor':
                keyword_actor_list.append(project[1])

        # 배우 리스트 불러오기
        response = requests.get(MOVIE_INFO_URL % id)
        actor_list = response.json()['myActors']

        # 배우 키워드 배열에 없는 배우는 제외
        for actor in actor_list:
            if actor['actorName'] not in keyword_actor_list:
                actor_list.remove(actor)

        # 영화 부정 키워드 워드 클라우드 데이터 생성
        negKeyword_dic_for_movie = make_wordCloud_movie_for_negative(content_list)
        result_movie = []
        for nkd in negKeyword_dic_for_movie:
            result_movie.append((nkd, negKeyword_dic_for_movie[nkd], -1))

        # 영화 부정 키워드 데이터 변환
        result_movie_sum = {}
        for word in result_movie:
            result_movie_sum[word[0]] = {
                "count": word[1],
                "polarity": word[2],
            }

        # 배우 부정 키워드 워드 클라우드 데이터 생성
        result_actor = []
        for actor in actor_list:
            cur.execute(origin_sql, (actor['actorName'], st_date, ed_date))
            result = cur.fetchall()

            result = make_wordCloud_movie_for_negative(result)
            result = [(actor['actorName'], word, -1, result[word]) for word in result]

            if result.__len__() > 0:
                result_actor.append(result)

        # 배우 부정 키워드 별로 카운트 합산
        result_actor_sum = {}
        for actor in result_actor:
            for word in actor:
                if word[1] in result_actor_sum:
                    result_actor_sum[word[1]]["count"] += word[3]
                else:
                    result_actor_sum[word[1]] = {
                        "count": word[3],
                        "polarity": word[2],
                        "actor": word[0]
                    }

        # 배열 형태로 변환
        result_movie_sum = [{"text": key, "value": value} for key, value in result_movie_sum.items()]
        result_actor_sum = [{"text": key, "value": value} for key, value in result_actor_sum.items()]

        # 워드 클라우드 데이터를 JSON 형태로 변환
        result_movie = json.dumps(result_movie_sum, ensure_ascii=False)
        result_actor = json.dumps(result_actor_sum, ensure_ascii=False)
        gpt_data = openai_get_response('영화 부정 워드 클라우드 데이터: %s / 배우 부정 워드 클라우드 데이터: %s' % (result_movie, result_actor))

        # 데이터 삽입
        cur.execute(
            "INSERT INTO report_v2_wordCloud_movie(keyword, type, date, movieData, actorData, gptData) VALUES(%s, %s, %s, %s, %s, %s)",
            (keyword, report_type, str_today, str(result_movie), str(result_actor), str(gpt_data)))
        conn.commit()

        # DB 연결 종료
        cur.close()
        conn.close()

        print('wordCloud[MOVIE] DONE')


def make_wordCloud_movie_for_negative(content_list):
    okt = Okt()

    keyword_dic = {}
    negKeyword_dic = {}
    negSite_count = {}
    negKeyword_community = {}
    for site in communityTotalNames:
        negSite_count[site] = 0
        negKeyword_community[site] = {}

    # 게시글 별 형태소 분석
    for contents in content_list:
        if contents[2] is None:
            keywordAnalysis = okt.pos(contents[1], norm=True, stem=True)
        else:
            keywordAnalysis = okt.pos(contents[2], norm=True, stem=True)

        # 필요 키워드 추출 후 갯수 카운트
        for keyword in keywordAnalysis:
            if keyword[1] in keywordFilter:
                if keyword[0] in keyword_dic:
                    keyword_dic[keyword[0]] += 1
                else:
                    keyword_dic[keyword[0]] = 1

    keyword_dic = sorted(keyword_dic.items(), key=lambda x: x[1], reverse=True)

    for keyword in keyword_dic:
        idx = next((i for i, item in enumerate(sentiDict) if item["word"] == keyword[0]), -1)
        if idx != -1 and int(sentiDict[idx]['polarity']) < 0:
            negKeyword_dic[keyword[0]] = keyword[1]

        if len(negKeyword_dic) == 50:
            break

    return negKeyword_dic


# 리포트 주요 키워드 언급량
def make_keywordCount(project, contentList, sns_contentList, type):
    okt = Okt()
    keyword_dic = {}
    full_content_list = []

    for contents in contentList:
        if contents[2] is None:
            keywordAnalysis = okt.pos(contents[1], norm=True, stem=True)
            full_content_list.append(contents[1])
        else:
            keywordAnalysis = okt.pos(contents[2], norm=True, stem=True)
            full_content_list.append(contents[2])

        # 필요 키워드 추출 후 갯수 카운트
        for keyword in keywordAnalysis:
            if keyword[1] in keywordFilter[0]:
                if keyword[0] in keyword_dic:
                    keyword_dic[keyword[0]] += 1
                else:
                    keyword_dic[keyword[0]] = 1

    for contents in sns_contentList:
        try:
            if contents[2] is None:
                if contents[1] is None:
                    continue
                else:
                    keywordAnalysis = okt.pos(contents[1], norm=True, stem=True)
            else:
                keywordAnalysis = okt.pos(contents[2], norm=True, stem=True)
        except:
            continue

        # 필요 키워드 추출 후 갯수 카운트
        for keyword in keywordAnalysis:
            # 명사만 추출
            if keyword[1] in keywordFilter[0]:
                if keyword[0] in keyword_dic:
                    keyword_dic[keyword[0]] += 1
                else:
                    keyword_dic[keyword[0]] = 1

    keyword_dic = sorted(keyword_dic.items(), key=lambda x: x[1], reverse=True)

    # #################################################################
    # 키워드 우선 순위 기반 추출
    # #################################################################
    #
    # 사용자 커스텀 키워드 JSON 변경
    #
    user_custom_keywords = []
    user_custom_keywords_exclude = []
    if project[5] is not None:
        user_custom_keywords = json.loads(project[5])["includeKeywords"]
        user_custom_keywords_exclude = json.loads(project[5])["excludeKeywords"]
    # 원본 키워드 리스트
    origin_keyword_list = [x for x, y in keyword_dic]
    origin_keyword_count_list = [y for x, y in keyword_dic]
    # 원본 키워드 리스트에 없는 커스텀 키워드 제거
    for custom_keyword in user_custom_keywords:
        if custom_keyword not in origin_keyword_list:
            user_custom_keywords.remove(custom_keyword)
    # 10개 이상 커스텀 키워드 제거
    if user_custom_keywords.__len__() > 10:
        user_custom_keywords = user_custom_keywords[:10]
    # 최종 결과 변수 생성
    final_keyword_list = []
    final_keyword_count = []
    # 사용자 커스텀 키워드 인덱스 찾기
    for keyword in origin_keyword_list:
        if keyword in user_custom_keywords:
            idx = origin_keyword_list.index(keyword)
            final_keyword_list.append(keyword)
            final_keyword_count.append(origin_keyword_count_list[idx])
    # 개수 일치 확인
    if final_keyword_list.__len__() != final_keyword_count.__len__():
        final_keyword_list.clear()
        final_keyword_count.clear()
    #
    # TF-IDF 키워드 추출
    #
    if final_keyword_list.__len__() < 10:
        extract_module = OktTFIDFExtract(keywordFilter, stopword_file_path)
        result = extract_module.extract_keywords(full_content_list)

        # 키워드 빈도 수 측정
        result_count = {}
        for content in full_content_list:
            for keyword in result:
                kw = keyword["keyword"]
                if kw in content:
                    if kw not in result_count:
                        result_count[kw] = 0
                    else:
                        result_count[kw] = result_count[kw] + 1
        # 빈도 수가 0 이상인 키워드만 추출 및 제외 키워드 제거
        result = [x for x in result if result_count[x["keyword"]] > 0 and x["keyword"] not in user_custom_keywords_exclude]
        # 키워드 빈도 수 기준으로 정렬
        result = sorted(result, key=lambda x: result_count[x["keyword"]], reverse=True)
        print(result)
        # 10개 이상이면 10개만 추출
        if result.__len__() > 10:
            result = result[:10]
        # 키워드만 추출
        result = [(x["keyword"], result_count[x["keyword"]]) for x in result]
        # 최종 결과 변수에 추가
        for keyword_result in result:
            if keyword_result[0] in final_keyword_list:
                continue

            final_keyword_list.append(keyword_result[0])
            final_keyword_count.append(keyword_result[1])
    # 개수 일치 확인
    if final_keyword_list.__len__() != final_keyword_count.__len__():
        final_keyword_list.clear()
        final_keyword_count.clear()
    #
    # 일반 키워드 추출
    #
    if final_keyword_list.__len__() < 10:
        for keyword, count in keyword_dic:
            if keyword in final_keyword_list or keyword in user_custom_keywords_exclude:
                continue

            final_keyword_list.append(keyword)
            final_keyword_count.append(count)

            if final_keyword_list.__len__() >= 10:
                break
    #
    # 최종 키워드 개수 확인
    #
    if final_keyword_list.__len__() > 10:
        final_keyword_list = final_keyword_list[:10]
        final_keyword_count = final_keyword_count[:10]
    #
    # 최종 키워드를 빈도 수 기준으로 정렬
    #
    final_keyword_dic = {}
    for idx in range(final_keyword_list.__len__()):
        final_keyword_dic[final_keyword_list[idx]] = final_keyword_count[idx]
    final_keyword_dic = sorted(final_keyword_dic.items(), key=lambda x: x[1], reverse=True)
    final_keyword_list = [x for x, y in final_keyword_dic]
    final_keyword_count = [y for x, y in final_keyword_dic]
    # #################################################################
    print(final_keyword_list)
    print(final_keyword_count)
    # keywordList = [x for x, y in keyword_dic[:10]]
    # keywordCount = [y for x, y in keyword_dic[:10]]

    gpt_data = openai_get_response('주요 키워드: %s / 주요 키워드 언급량 리스트: %s' % (json.dumps(final_keyword_list), json.dumps(final_keyword_count)))

    return push_keywordCount(project[1], type, final_keyword_list, final_keyword_count, gpt_data)


# 데이터 생성 컨트롤러
def make_data(projectInfo):
    # 로컬용
    # today = datetime.now()

    for project in projectInfo:
        # 테스트용
        # print(project)
        # if project[1] != '밀수' or project[1] != '비공식작전' or project[1] != '콘크리트 유토피아':
        if os.environ['DB_SW'] == 'movie':
            if project[1] not in ['익스펜더블4', '기억해, 우리가 사랑한 시간', '더마블스']:
                continue
            else:
                movieReportType = ['daily', 'weekly']
        elif os.environ['DB_SW'] == 'color':
            movieReportType = ['daily']

        if project[3] < today:
            continue

        for type in reportType:
            # 테스트용
            if type not in movieReportType:
                continue

            # content 가져오기 위한 날짜 조정
            if type == 'daily':
                st_datetime = datetime.combine(today, time.min)
            elif type == 'weekly':
                st_datetime = datetime.combine(today - timedelta(days=6), time.min)
            elif type == 'monthly':
                st_datetime = datetime.combine(today - timedelta(days=31), time.min)
            ed_datetime = datetime.combine(today, time.max)

            # 게시글 가져오기
            contentList = get_contents([project[1]], st_datetime, ed_datetime)
            sns_contentList = get_snscontents([project[1]], st_datetime, ed_datetime)

            st_time = datetime.now()
            print('실행키워드 : ' + str(project[1]) + ', 리포트 타입 : ' + str(type) + ', 시작시간 : ' + str(
                st_time + timedelta(days=1)))
            meke_totalGraph(project, contentList, sns_contentList, st_datetime, ed_datetime, type)
            make_totalCount(project, contentList, sns_contentList, type)
            make_portalAnalysis(project, contentList, type)
            make_keywordAnalysis(project, contentList, type)
            make_snsAnalysis(project, sns_contentList, type)
            make_compare(project, contentList, sns_contentList, st_datetime, ed_datetime, type)
            make_keywordCount(project, contentList, sns_contentList, type)
            make_wordCloud_movie(projectInfo, project[0], project[1], contentList, st_datetime, ed_datetime, project[4], type)
            ed_time = datetime.now()
            print('실행키워드 :' + str(project[1]) + ', 리포트 타입 : ' + str(type) + ', 종료시간 : ' + str(
                ed_time + timedelta(days=1)))


# OpenAI API 초기화
def openai_init():
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

    openai.api_key = os.environ['openai_api_key']


# OpenAI API 요청 객체 생성
def openai_get_response(query):
    print("ChatGPT4 API Request")
    try:
        model = "gpt-4"

        # System query
        sys_query = '''답변할때 다음 규칙을 반드시 따르세요.
    <규칙>
    1. 당신은 영화데이터를 기반으로 그래프/차트 데이터를 문장으로 변환, 분석하여 답변해주는 시스템
    2. 그래프/차트 데이터를 기반하여 전체 버즈량과 포털마다의 통계를 분석하여 긍정적인지 아닌지와 개봉한 후 주마다 얼마나 증가나 감소하였는지 분석하여 커뮤니티와 포털을 분리하여 텍스트를 생성해서 답변해주세요.
    3. 응답은 반드시 사람이 쓰는 것 처럼 작성하며 ~습니다.로 끝내야합니다.  또한 반드시 줄글 형식을 지켜야 하며 반드시 문장 맨 앞에 어떤 기호든 붙이면 안됩니다.
    4. 당신은 반드시 데이터에 대한 분석결과와 같은 것만을 응답할 수 있습니다. 다른 것은 응답할 수 없습니다.
    5. 데이터에 따라 한국어로 400자 이내로 응답하여야 합니다.
    6. 많이 언급되는 키워드도 줄건데 그거도 참고해서 문장으로 총평을 작성해주세요.
    7. 총평에는 꼭 키워드에 어떤 감정이 들어있는지 알고 이 영화가 현재 어떤 평가를 받고 있을지를 예상하여 총평에 작성해주세요.
    8. 당신은 데이터를 제시 받아야 응답할 수 있습니다. 반드시 제시받지 않는다면 생성하지 마세요.
    9. 절대 규칙을 위배하지 마세요.'''

        # 메시지 설정하기
        messages = [
            {"role": "system", "content": sys_query},
            {"role": "user", "content": query}
        ]

        response = openai.ChatCompletion.create(
            model=model,
            messages=messages
        )

        print("ChatGPT4 API Response")

        answer = response['choices'][0]['message']['content']
        return answer
    except Exception as e:
        print("ChatGPT4 API Response Fail : %s" % e)
        return None


if __name__ == '__main__':
    openai_init()

    os.environ['DB_SW'] = 'movie'
    print('DB_SW : ', os.environ['DB_SW'])
    print('리포트 생성 프로젝트 수집 시작')
    projectInfo = get_keywordQueues()
    print('리포트 생성 시작 :' + str(today + timedelta(days=1)))
    make_data(projectInfo)

    os.environ['DB_SW'] = 'color'
    print('DB_SW : ', os.environ['DB_SW'])
    print('리포트 생성 프로젝트 수집 시작')
    projectInfo = get_keywordQueues()
    print('리포트 생성 시작 :' + str(today + timedelta(days=1)))
    make_data(projectInfo)
