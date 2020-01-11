import requests
from bs4 import BeautifulSoup
import urllib.robotparser as urobot
import json
import datetime
import re
import os
from time import sleep
import random

# to extract:
# posts_chunk = {idx: url,  reg_date, name, ip or user_id, title, content, view, voteup, votedown, {comments}}
# comments = {idx: list of (reg_date, name, ip or user_id, content)}

gall_id = 'dbd' # gallery id 
n_post = 1000 # number of posts to scrap
request_delay = lambda: random.uniform(3, 6) # request delay in seconds x
skip_downloaded = True # if True, ignore already downloaded 
get_posts_from_board = False
page_max = 1000 # max pages on board 


# ignore 
idx_ignore = []
folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
if not os.path.exists(folder):
    os.makedirs(folder)

if skip_downloaded:
    filenames = [os.path.join(folder, f) for f in os.listdir(folder) 
        if re.match(rf'{gall_id}_[0-9]+\.json', f) or f == f'{gall_id}_deleted.json']
    for fn in filenames:
        with open(fn, 'r', encoding='UTF-8-sig') as f_data:
            post_chunk = json.load(f_data)
            idx_ignore.extend(post_chunk.keys())
    idx_ignore.sort(key=int, reverse=True)

idx_deleted = {}
fn = os.path.join(folder, f'{gall_id}_deleted.json') 
if os.path.isfile(fn):
    with open(fn, 'r', encoding='UTF-8-sig') as f_data:
        idx_deleted = json.load(f_data)

idx_ignore = set(idx_ignore)

# check robots.txt 
try:
    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}"
    rp = urobot.RobotFileParser()
    rp.set_url("https://gall.dcinside.com/robots.txt")
    rp.read()
    if not rp.can_fetch('*', url_board):
        raise Exception
except: 
    msg = f'request blocked by robots.txt: {url_board}'
    raise Exception(msg)


# get post urls from board
t_start = datetime.datetime.now()
urls = {}

for page in range(1, page_max + 1):
    if len(urls) > n_post:
        break
    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
    if page > 1: 
        print('\r', end='')
    print(f'fetching post urls - post {len(urls)}/{n_post}, {url_board}', end='')

    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
    }
    try:
        req = requests.get(url_board, headers=headers)
        soup = BeautifulSoup(req.text, 'lxml')
        data_list = soup.find('table','gall_list').find_all('tr', {'class': 'ub-content us-post'})
        for data in data_list:
            if(data['data-type']=='icon_notice'):
                continue
            idx = data['data-no']
            a = data.find('td', {'class':'gall_tit ub-word'}).a
            url_post = 'https://gall.dcinside.com/' + a['href']
            if skip_downloaded and (idx in idx_ignore):
                continue
            if len(urls) >= n_post or not get_posts_from_board:
                idx_base = idx
                break
            urls[idx] = url_post
        sleep(request_delay())
        if not get_posts_from_board: 
            print('!')
            break
    except Exception as e:
        print(' ' + str(e))
        pass
if not get_posts_from_board:
    for idx in range(int(idx_base), -1, -1):
        if skip_downloaded and (str(idx) in idx_ignore):
            continue
        url_post = f"https://gall.dcinside.com/mgallery/board/view/?id={gall_id}&no={idx}"
        urls[idx] = url_post
        if len(urls) >= n_post:
            break
t_end = datetime.datetime.now()
print(f'\rfetching post urls - post {len(urls)}/{n_post}, Done. {t_end - t_start} elapsed.'.ljust(100))

# group urls into chunks  
t_start = datetime.datetime.now()
url_chunks = {}
for idx in urls:
    idx_hash = str((int(idx) // 1000) * 1000)
    if idx_hash not in url_chunks:
        url_chunks[idx_hash] = {}
    url_chunks[idx_hash][idx] = urls[idx]


# fetch post data 
# then save each post chunk to file
i = 1
n_failed = 0
for idx_hash in sorted(url_chunks, key=int, reverse=True):
    post_chunk = {}
    fn =  os.path.join(os.path.dirname(os.path.abspath(__file__)), f'data/{gall_id}_{idx_hash}.json')
    if os.path.isfile(fn):
        with open(fn, 'r', encoding='UTF-8-sig') as f_data:
            post_chunk = json.load(f_data)

    for idx in sorted(url_chunks[idx_hash], key=int, reverse=True):
        url = urls[idx]
        if i > 0: 
            print('\r', end='')
        print(f'fetching post info - post {i - n_failed}/{len(urls) - n_failed}, {url}', end='')
        i += 1

        # open post url and extract info 
        headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
        }
        try:
            req = requests.get(url, headers=headers)
            soup = BeautifulSoup(req.text, 'lxml')
            if 'location.replace("/derror/deleted' in req.text:
                print(f', post deleted: {idx}')
                idx_deleted[idx] = ""
                continue          
            post = {}
            head = soup.find('div', {'class': 'gallview_head clear ub-content'})
            post['title'] = head.find('span', {'class': 'title_subject'}).get_text()
            post['reg_date'] = head.find('span', {'class': 'gall_date'})['title']
            data_name = head.find('div', {'class': 'gall_writer ub-writer'})
            post['name'] = data_name['data-nick']
            try:
                post['ip'] = data_name['ip']
                post['user_id'] = ''
            except:
                post['ip'] = ''
                post['user_id'] = data_name['data-uid']
            post['view'] = int(head.find('span', {'class': 'gall_count'}).get_text()[2:])
            post['content'] = str(soup.find('div', {'class': 'writing_view_box'}).find('div', {'style':'overflow:hidden;'}))
            rec = soup.find('div', {'class': 'btn_recommend_box clear'})
            post['voteup'] = int(rec.find('p', {'class': 'up_num font_red'}).get_text())
            post['votedown'] = int(rec.find('p', {'class': 'down_num'}).get_text())
            # print(post)
        except Exception as e: 
            n_failed += 1
            continue

        post['comments'] = []
        comment_url = 'https://gall.dcinside.com/board/comment/'
        comment_page = 1
        max_comment_page = 1 
        while comment_page <= max_comment_page:
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
                data = json.loads(req.text)

                if comment_page == 1:
                    re_match = re.compile(r'viewComments\((\d),').findall(data['pagination'])
                    if re_match:
                        max_comment_page = int(re_match[-1])

                def fix_date(reg_date):
                    if reg_date[:2] != '20':
                        reg_date = str(datetime.datetime.now().year) +'.' + reg_date
                    return reg_date.replace('.', '-')
                post['comments'].extend(list({
                    'reg_date': fix_date(comment['reg_date']), 
                    'name': comment['name'], 
                    'user_id': comment['user_id'], 
                    'ip': comment['ip'],
                    'content': comment['memo']
                    } for comment in data['comments'])[::-1])
            except:
                pass
            #print(post['comments'])
            comment_page += 1
            sleep(request_delay())
        post_chunk[idx] = post

    # save chunk to file
    fn =  os.path.join(os.path.dirname(os.path.abspath(__file__)), f'data/{gall_id}_{idx_hash}.json')
    with open(fn, 'w', encoding='UTF-8-sig') as f_data:
        json.dump(post_chunk, f_data, indent=4, ensure_ascii=False)
fn =  os.path.join(os.path.dirname(os.path.abspath(__file__)), f'data/{gall_id}_deleted.json')
with open(fn, 'w', encoding='UTF-8-sig') as f_data:
    json.dump(idx_deleted, f_data, indent=4, ensure_ascii=False)
t_end = datetime.datetime.now()
print(f'\rfetching comments - post {i - n_failed}/{len(urls)}, Done. {t_end - t_start} elapsed.'.ljust(200))
