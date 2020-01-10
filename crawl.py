import requests
from bs4 import BeautifulSoup
import urllib.robotparser as urobot
import json
import datetime
import re
import os
from time import sleep

# to extract:
# posts_chunk = {idx: url,  reg_date, name, ip or user_id, title, content, view, voteup, votedown, {comments}}
# comments = {idx: list of (reg_date, name, ip or user_id, content)}

gall_id = 'dbd' # gallery id 
n_page = 1000 # number of board pages to scrap
request_delay = 0.1 # request delay in seconds x


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

for page in range(1, n_page + 1):
    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
    if page > 1: 
        print('\r', end='')
    print(f'fetching posts - page {page}/{n_page}, {url_board}', end='')

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
            urls[idx] = 'https://gall.dcinside.com/' + a['href']
        sleep(request_delay)
    except:
        continue
t_end = datetime.datetime.now()
print(f'\rfetching posts - Done. {t_end - t_start} elapsed.'.ljust(100))


# group urls into chunks  
t_start = datetime.datetime.now()
url_chunks = {}
for idx in urls:
    idx_hash = f'{(int(idx) // 1000) * 1000}'
    if idx_hash not in url_chunks:
        url_chunks[idx_hash] = {}
    url_chunks[idx_hash][idx] = urls[idx]


# fetch post data 
# then save each post chunk to file
i = 1
for idx_hash in sorted(url_chunks)[::-1]:
    post_chunk = {}
    for idx in sorted(url_chunks[idx_hash])[::-1]:
        url = urls[idx]
        if i > 0: 
            print('\r', end='')
        print(f'fetching comments and additional info - post {i}/{len(urls)}, {url}', end='')
        i += 1

        # open post url and extract info 
        headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
        }
        try:
            req = requests.get(url, headers=headers)
            soup = BeautifulSoup(req.text, 'lxml')
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
            sleep(request_delay)
        post_chunk[idx] = post

    # save chunk to file
    filename =  os.path.join(os.path.dirname(os.path.abspath(__file__)), f'data/{gall_id}_{idx_hash}.json')
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    print(f', {filename}')
    with open(filename, 'w', encoding='UTF-8-sig') as f_data:
        json.dump(post_chunk, f_data, indent=4, sort_keys=True, ensure_ascii=False)
t_end = datetime.datetime.now()
print(f'fetching comments - Done. {t_end - t_start} elapsed.'.ljust(200))
