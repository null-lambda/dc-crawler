import requests
from bs4 import BeautifulSoup
import urllib.robotparser as urobot
import json
import datetime
import re
import os
import time

# to extract:
#   posts = {idx: url, title, reg_date, content, view, voteup, votedown}
#   comments = {idx: list of (reg_date, name, ip or user_id, content)}

def check_robots(url):
    rp = urobot.RobotFileParser()
    rp.set_url("https://gall.dcinside.com/robots.txt")
    rp.read()
    return rp.can_fetch('*', url)


gall_id = 'dbd' # gallery id 
n_page = 1 # number of board pages to scrap
posts = {}
sleep = 0.1

# get post url and info from board
t_start = datetime.datetime.now()
for page in range(1, n_page + 1):
    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
    #if page > 1: 
    #    print('\r', end='')
    print(f'fetching posts - page {page}/{n_page}, {url_board}', end='')

    url_board = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
    if not check_robots(url_board):
        print(f'request blocked by robots.txt: {url_board}')
        continue
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
            post = {}
            post['url'] = 'https://gall.dcinside.com/' + a['href']
            post['title'] = a.get_text()
            post['reg_date'] = data.find('td', {'class': 'gall_date'})['title']
            post['view'] = int(data.find('td', {'class': 'gall_count'}).get_text())
            post['voteup'] = int(data.find('td', {'class': 'gall_recommend'}).get_text())
    
            # open post url and get info 
            try:
                req = requests.get(post['url'], headers=headers)
                soup = BeautifulSoup(req.text, 'lxml')
                post['content'] = str(soup.find('div', {'class': 'writing_view_box'}).find('div', {'style':'overflow:hidden;'}))
                post['votedown'] = int(soup.find('p', {'class': 'down_num'}).get_text())
                posts[idx] = post
            except:
                continue
        time.sleep(sleep)
    except:
        continue



t_end = datetime.datetime.now()
print(f'\rfetching posts - Done. {t_end - t_start} elapsed.'.ljust(100))


# group posts into chunks  
t_start = datetime.datetime.now()
post_chunks = {}
for idx in posts:
    idx_hash = int(idx) // 1000
    if idx_hash not in post_chunks:
        post_chunks[idx_hash] = {}
    post_chunks[idx_hash][idx] = posts[idx]


# get comments and save each chunk(posts, comments) to file
i = 1
for idx_hash, post_chunk in sorted(post_chunks.items())[::-1]:
    comments = {}
    for idx, post in sorted(post_chunk.items())[::-1]:
        if i > 0: 
            print('\r', end='')
        print(f'fetching comments - post {i}/{len(posts)}, {post["url"]}, {post["title"]}', end='')
        i += 1

        comments[idx] = []
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
                comments[idx].extend(list({
                    'reg_date': fix_date(comment['reg_date']), 
                    'name': comment['name'], 
                    'user_id': comment['user_id'], 
                    'ip': comment['ip'],
                    'content': comment['memo']
                    } for comment in data['comments'])[::-1])
            except:
                pass
            #print(comments[idx])
            comment_page += 1
            time.sleep(sleep)
    # save chunk to file
    filename =  f'data/{gall_id}_{idx_hash}xxx.json'
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    with open(filename, 'w', encoding='UTF-8-sig') as f_data:
        json.dump({'posts': post_chunk, 'comments': comments}, f_data, indent=4, sort_keys=True, ensure_ascii=False)

t_end = datetime.datetime.now()
print(f'\rfetching comments - Done. {t_end - t_start} elapsed.'.ljust(200))
