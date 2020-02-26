import json
import os
import random
import urllib.parse as urlparse
import urllib.robotparser as urobot
from datetime import datetime
from time import sleep
from itertools import groupby

import requests
from bs4 import BeautifulSoup


def check_robots(url, url_robot="https://gall.dcinside.com/robots.txt"):
    rp = urobot.RobotFileParser()
    rp.set_url(url_robot)
    rp.read()
    return rp.can_fetch("*", url)


def extract_idx(url):
    parsed = urlparse.urlparse(url)
    return int(urlparse.parse_qs(parsed.query)["no"][0])


def read_board(url):
    post_urls = []

    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        }
        req = requests.get(url, headers=headers, allow_redirects=False)
        soup = BeautifulSoup(req.text, "lxml")
        data_table = soup.find("table", "gall_list")
        data_list = data_table.find_all("tr", {"class": "ub-content us-post"})
        for data in data_list:
            if data["data-type"] == "icon_notice":
                continue
            a = data.find("td", {"class": "gall_tit ub-word"}).a
            post_urls.append("https://gall.dcinside.com/" + a["href"])
    except (requests.exceptions.RequestException, ValueError, AttributeError) as e:
        print(e, "\n")
        return None

    return post_urls


def get_post_lists(url_base, n_post, page_max=10000, delay=0.5):
    urls = []

    t_start = datetime.now()
    for page in range(1, page_max + 1):
        if len(urls) > n_post:
            urls = sorted(urls, reverse=True, key=extract_idx)[:n_post]
            break

        url_board = f"{url_base}&page={page}"
        new_urls = read_board(url_board)
        if not new_urls:
            break

        urls.extend(new_urls)
        sleep(delay)

        print(f"fetching post urls - post {len(urls)}/{n_post}, {url_board}")

    t_end = datetime.now()
    print(f"\rfetching post urls - Done. {t_end - t_start} elapsed.".ljust(100))
    return urls


def read_post(url):
    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        }
        req = requests.get(url, headers=headers)
        soup = BeautifulSoup(req.text, "lxml")
        if 'location.replace("/derror/deleted' in req.text:
            print(f", post deleted: {extract_idx(url)}")
            return None
        post = {}
        head = soup.find("div", {"class": "gallview_head clear ub-content"})
        post["title"] = head.find("span", {"class": "title_subject"}).get_text()
        post["reg_date"] = head.find("span", {"class": "gall_date"})["title"]
        data_name = head.find("div", {"class": "gall_writer ub-writer"})
        post["name"] = data_name["data-nick"]
        post["ip"] = data_name["data-ip"]
        post["user_id"] = data_name["data-uid"]
        post["view"] = int(head.find("span", {"class": "gall_count"}).get_text()[2:])
        content = soup.find("div", {"class": "writing_view_box"}).find(
            "div", {"style": "overflow:hidden;"}
        )
        post["text"] = content.get_text()
        post["images"] = []
        rec_box = soup.find("div", {"class": "btn_recommend_box clear"})
        post["voteup"] = int(rec_box.find("p", {"class": "up_num font_red"}).get_text())
        post["votedown"] = int(rec_box.find("p", {"class": "down_num"}).get_text())
        return post
    except (requests.exceptions.RequestException, ValueError, AttributeError) as e:
        print(e, "\n")
        return None


def main():
    gall_id = "dbd"  # gallery id
    n_post = 1000  # number of posts to scrap
    request_delay = lambda: random.uniform(0.5, 1)  # request delay in seconds x
    page_max = 1000  # max pages on board
    url_base = "https://gall.dcinside.com/mgallery/board/lists/?id=dngks&search_head=120&list_num=100"

    if not check_robots(url_base):
        msg = f"request blocked by robots.txt: {url_base}"
        raise Exception(msg)

    urls = get_post_lists(url_base, n_post)

    # group urls into chunks
    urls = [(extract_idx(url), url) for url in urls]
    idx_hash = lambda t: (int(t[0]) // 10000) * 10000
    url_chunks = {k: dict(g) for k, g in groupby(urls, key=idx_hash)}

    # fetch post data, then save each post chunk to file
    t_start = datetime.now()
    n, n_success, n_failed = len(urls), 0, 0
    for idx_hash in sorted(url_chunks):
        post_chunk = {}
        fn = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), f"data/{gall_id}_{idx_hash}.json"
        )
        if os.path.isfile(fn):
            with open(fn, "r", encoding="UTF-8-sig") as f_data:
                post_chunk = json.load(f_data)

        for idx, url in sorted(url_chunks[idx_hash].items()):
            post = read_post(url)
            sleep(0.5)
            if post:
                post_chunk[idx] = post
                n_success += 1
            else:
                n_failed += 1
            print(f"fetching post info - post {n_success}/{n - n_failed}")

        # save chunk to file
        f_dir = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(f_dir, f"data/{gall_id}_{idx_hash}.json")
        with open(fn, "w", encoding="UTF-8-sig") as f_data:
            json.dump(post_chunk, f_data, indent=4, ensure_ascii=False)
        fn = os.path.join(f_dir, f"data/{gall_id}_deleted.json")

    t_end = datetime.now()
    print(f"fetching post info - Done. {t_end - t_start} elapsed.".ljust(200))


if __name__ == "__main__":
    main()
