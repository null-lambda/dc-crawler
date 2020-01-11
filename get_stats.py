import json
import re
import datetime as dt
import os
from bs4 import BeautifulSoup
from collections import Counter
from konlpy.tag import Twitter
import csv 
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

nlp = Twitter()
print('nlp loaded')

# data format: 
#   posts = {idx: url, title, reg_date, name, ip or user_id, content, view, voteup, votedown}
#   comments = list of (reg_date, name, ip or user_id, content)
#
# to extract: 
#   names = {name_hash: name}
#   user_stats = {name_hash: alias,
#     num_posts, num_comments(ignore deleted comments), total_char(text only), view, voteup, votedown,
#     keywords(top 10)}
#   post_stats = {post: num_comments, view, voteup, votedown}

gall_id = 'dbd' 
# month = '2020-01'

names = {}
user_stats = {}
post_stats = {}

t_start = dt.datetime.now()
def filter_post(post):
    post_time = dt.datetime.strptime(post['reg_date'], '%Y-%m-%d %H:%M:%S')
    # print(post_time)
    if not (dt.datetime(2019, 12, 1) <= post_time):
        return False
    return True


def update_user_stats(data):
    # find name hash
    name, ip, user_id = data['name'], data['ip'], data['user_id']
    if not ip and not user_id: # invalid name or advertisement
        return
    h = f'{name}({ip})' if ip else user_id # name hash

    # init 
    if h not in names:
        names[h] = Counter() # temporary, replace with list or text
        user_stats[h] = {
            'alias': h, # temporary for fixed names 
            'total_char':0, 'keywords': Counter(), # temporary, replace with list or text
            'num_posts': 0, 'num_comments': 0, 'view':0, 'voteup':0, 'votedown': 0
            }
            
    # data: post, comments
    if user_id:
        names[h][name] += 1

    text = BeautifulSoup(data['content'], 'lxml').get_text()
    user_stats[h]['total_char'] += len(text)
    nouns = nlp.nouns(text)
    user_stats[h]['keywords'] += Counter([word for word in nouns if len(word)>=2])

    # data: post only
    if 'view' in data:
        user_stats[h]['num_posts'] += 1
        user_stats[h]['view'] += data['view']
        user_stats[h]['voteup'] += data['voteup']
        user_stats[h]['votedown'] += data['votedown']

    # data: comment only
    else:
        user_stats[h]['num_comments'] += 1


folder = os.path.dirname(os.path.abspath(__file__))
folder_data = os.path.join(folder, 'data')
filenames = [fn for fn in os.listdir(folder_data) if re.match(rf'{gall_id}_[0-9]+\.json', fn)]
filenames.sort(key=lambda fn: int(re.match(rf'{gall_id}_([0-9]+)\.json', fn).group(1)), reverse=True)
for fn in filenames:
    print(f'reading {fn}')
    with open(os.path.join(folder_data, fn), 'r', encoding='UTF-8-sig') as f_data:
        post_chunk = json.load(f_data)
    for idx in sorted(post_chunk, key=int, reverse=True):
        post = post_chunk[idx]
        if not filter_post(post):
            continue
        post_stats[idx] = {
            'num_comments': len(post['comments']), 
            'view': post['view'], 
            'voteup': post['voteup'], 
            'votedown': post['votedown']}
        update_user_stats(post)
        for comment in post['comments']:
            update_user_stats(comment)


# format Counter data into text
for h in names:
    if len(names[h]) == 0: # non-fixed id
        continue
    name_combined = ', '.join(c[0] for c in names[h].most_common(3))
    if len(names[h]) > 3:
        name_combined += ', ...'
    user_stats[h]['alias'] = f'{name_combined}({h})'

global_keywords = sum((user_stats[h]['keywords'] for h in user_stats), Counter()).most_common()

for h in user_stats:
    user_stats[h]['keywords'] = ' '.join(c[0] for c in user_stats[h]['keywords'].most_common(12))
    
t_end = dt.datetime.now()
print(f'Done. {t_end - t_start} elapsed.'.ljust(100))


# write to file
fn = os.path.join(folder, f'data/{gall_id}_stats.json')
with open(fn, 'w', encoding='UTF-8-sig') as f_data:
    json.dump({'user_stats': user_stats, 'post_stats': post_stats, 'keywords': global_keywords}, f_data, indent=4, ensure_ascii=False)

fn = os.path.join(folder, f'{gall_id}_users.csv')
with open(fn, 'w', encoding='UTF-8-sig', newline='') as f_data:
    user_stats = list(user_stats.values())
    if user_stats:
        keys = user_stats[0].keys()
        writer = csv.DictWriter(f_data, keys)
        writer.writeheader()
        writer.writerows(user_stats)

fn = os.path.join(folder, f'{gall_id}_posts.csv')
with open(fn, 'w', encoding='UTF-8-sig', newline='') as f_data:
    for idx in post_stats:
        post_stats[idx] = idx
    post_stats = list(post_stats.values())
    if post_stats:
        keys = post_stats[0].keys()
        writer = csv.DictWriter(f_data, keys)
        writer.writeheader()
        writer.writerows(user_stats)
