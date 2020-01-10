import requests
from bs4 import BeautifulSoup
import json
import datetime
import urllib.robotparser as urobot

# to extract:
#posts = {idx: url, title}
#comments = {idx: list of (reg_date, name, ip or user_id, memo)}

def check_robots(url):
    rp = urobot.RobotFileParser()
    rp.set_url("https://gall.dcinside.com/robots.txt")
    rp.read()
    return rp.can_fetch('*', url)


gall_id = 'dbd' # gallery id 
n_page = 1 # number of board pages to scrap
posts = {}

t_start = datetime.datetime.now()
for page in range(1, n_page + 1):
    if page > 1: 
        print('\r', end='')
    print(f'fetching posts - page {page}/{n_page}', end='')

    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
    if not check_robots(url_board):
        print(f'request blocked by robots.txt: {url}')
        continue

    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
    }
    try:
        req = requests.get(url_board, headers=headers)
        html = req.text

        soup = BeautifulSoup(html, 'lxml')
        data = soup.find('table','gall_list').find_all('tr', {'class':'ub-content us-post'})

        for post in data:
            if(post['data-type']=='icon_notice'):
                continue
            idx = post['data-no']
            a = post.find('td', {'class':'gall_tit ub-word'}).a
            post_url = 'https://gall.dcinside.com/' + a['href']
            post_title = a.get_text()
            post_reg_date = post.find('td', {'class':'gall_date'})['title']
            posts[idx] = {'url': post_url, 'title': post_title, 'reg_date': post_reg_date}
            #print(posts[idx])
    except:
        continue
t_end = datetime.datetime.now()
print(f'\rfetching posts - Done. {t_end - t_start} elapsed.')


t_start = datetime.datetime.now()
comments = {}
for i, idx in enumerate(posts):
    if i > 0: 
        print('\r', end='')
    print(f'fetching comments - post {i + 1}/{len(posts)}', end='')
    
    post_title = posts[idx]['title']
    post_url = posts[idx]['url']

    comment_url = 'https://gall.dcinside.com/board/comment/'
    comment_page = 1 # 댓글수 50개 넘어가는 건 그냥 무시
    data = f'id={gall_id}&no={idx}&cmt_id={gall_id}&cmt_no={idx}&e_s_n_o=3eabc219ebdd65f53b&comment_page={comment_page}&sort='
    headers = {
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Content-Length':str(len(data.encode("utf-8"))),
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'X-Requested-With':'XMLHttpRequest'
    }
    try:
        req = requests.post(comment_url, data = data, headers=headers)
        json_text = req.text
        data = json.loads(json_text)

        def fix_date(reg_date):
            if reg_date[:2] != '20':
                reg_date = '2020.' + reg_date
            return reg_date.replace('.', '-')
        comments[idx] = [{
            'reg_date': fix_date(comment['reg_date']), 
            'name': comment['name'], 
            'user_id': comment['user_id'], 
            'ip': comment['ip'],
            'memo': comment['memo']
            } for comment in data['comments']]
        #print(comments[idx])
    except:
        comments[idx] = []

t_end = datetime.datetime.now()
print(f'\rfetching comments - Done. {t_end - t_start} elapsed.')

with open('data.json', 'w', encoding='UTF-8-sig') as f_data:
    json.dump({'posts': posts, 'comments': comments}, f_data, indent=4, sort_keys=True, ensure_ascii=False)

