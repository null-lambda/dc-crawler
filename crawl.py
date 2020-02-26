import argparse
import json
import os
import random
import re
import urllib.parse as urlparse
import urllib.robotparser as urobot
from datetime import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup


def check_robots(url, url_robot="https://gall.dcinside.com/robots.txt"):
    rp = urobot.RobotFileParser()
    rp.set_url(url_robot)
    rp.read()
    return rp.can_fetch("*", url_robot)


def extract_url_idx(url):
    parsed = urlparse.urlparse(url)
    return int(urlparse.parse_qs(parsed.query)["no"][0])


def read_board(url_board):
    post_urls = []

    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        }
        req = requests.get(url_board, headers=headers, allow_redirects=False)

        soup = BeautifulSoup(req.text, "lxml")

        data_table = soup.find("table", "gall_list")
        data_list = data_table.find_all("tr", {"class": "ub-content us-post"})
        for data in data_list:
            if data["data-type"] == "icon_notice":
                continue
            a = data.find("td", {"class": "gall_tit ub-word"}).a
            post_urls.append("https://gall.dcinside.com/" + a["href"])
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"{e}\n")
        return []

    return post_urls


def get_post_lists(url_base, n_post, page_max=10000, delay=0.5):
    urls = []

    t_start = datetime.now()
    for page in range(1, page_max + 1):
        if len(urls) > n_post:
            urls = sorted(urls, reverse=True)[:n_post]
            break

        url_board = f"{url_base}&page={page}"
        new_urls = read_board(url_board)
        if not new_urls:
            break

        urls.extend(new_urls)
        sleep(delay)

        print(f"fetching post urls - post {len(urls)}/{n_post}, {url_board}")

    t_end = datetime.now()
    print(
        f"\rfetching post urls - post {len(urls)}/{n_post}, Done. {t_end - t_start} elapsed.".ljust(
            100
        )
    )
    return urls


def main():
    # to extract:
    # posts_chunk = {idx: url,  reg_date, name, ip or user_id, title, content, view, voteup, votedown, {comments}}
    # comments = {idx: list of (reg_date, name, ip or user_id, content)}

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
    t_start = datetime.now()
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
    for idx_hash in sorted(url_chunks, key=int):
        post_chunk = {}
        fn = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), f"data/{gall_id}_{idx_hash}.json"
        )
        if os.path.isfile(fn):
            with open(fn, "r", encoding="UTF-8-sig") as f_data:
                post_chunk = json.load(f_data)

        for idx in sorted(url_chunks[idx_hash], key=int):
            url = urls[idx]
            print(
                f"fetching post info - post {i - n_failed}/{len(urls) - n_failed}, {url}",
                end="",
            )
            i += 1

            # open post url and extract info
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            }
            try:
                req = requests.get(url, headers=headers)
                sleep(request_delay())
                soup = BeautifulSoup(req.text, "lxml")
                if 'location.replace("/derror/deleted' in req.text:
                    print(f", post deleted: {idx}")
                    idx_deleted[idx] = ""
                    continue
                post = {}
                head = soup.find("div", {"class": "gallview_head clear ub-content"})
                post["title"] = head.find("span", {"class": "title_subject"}).get_text()
                post["reg_date"] = head.find("span", {"class": "gall_date"})["title"]
                data_name = head.find("div", {"class": "gall_writer ub-writer"})
                post["name"] = data_name["data-nick"]
                post["ip"] = data_name["data-ip"]
                post["user_id"] = data_name["data-uid"]
                post["view"] = int(
                    head.find("span", {"class": "gall_count"}).get_text()[2:]
                )
                post["content"] = str(
                    soup.find("div", {"class": "writing_view_box"}).find(
                        "div", {"style": "overflow:hidden;"}
                    )
                )
                rec = soup.find("div", {"class": "btn_recommend_box clear"})
                post["voteup"] = int(
                    rec.find("p", {"class": "up_num font_red"}).get_text()
                )
                post["votedown"] = int(rec.find("p", {"class": "down_num"}).get_text())
                # print(post)
            except Exception as e:
                n_failed += 1
                continue

            # post["comments"] = []
            # comment_url = "https://gall.dcinside.com/board/comment/"
            # comment_page = 1
            # max_comment_page = 1
            # while comment_page <= max_comment_page:
            #     data = f"id={gall_id}&no={idx}&cmt_id={gall_id}&cmt_no={idx}&e_s_n_o=3eabc219ebdd65f53b&comment_page={comment_page}&sort="
            #     headers = {
            #         "Accept": "application/json, text/javascript, */*; q=0.01",
            #         "Content-Length": str(len(data.encode("utf-8"))),
            #         "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            #         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
            #         "X-Requested-With": "XMLHttpRequest",
            #     }
            #     try:
            #         req = requests.post(comment_url, data=data, headers=headers)
            #         sleep(request_delay())
            #         data = json.loads(req.text)

            #         if comment_page == 1:
            #             re_match = re.compile(r"viewComments\((\d),").findall(
            #                 data["pagination"]
            #             )
            #             if re_match:
            #                 max_comment_page = int(re_match[-1])

            #         def fix_date(reg_date):
            #             if reg_date[:2] != "20":
            #                 reg_date = str(datetime.now().year) + "." + reg_date
            #             return reg_date.replace(".", "-")

            #         post["comments"].extend(
            #             list(
            #                 {
            #                     "reg_date": fix_date(comment["reg_date"]),
            #                     "name": comment["name"],
            #                     "user_id": comment["user_id"],
            #                     "ip": comment["ip"],
            #                     "content": comment["memo"],
            #                 }
            #                 for comment in data["comments"]
            #             )[::-1]
            #         )
            #     except:
            #         pass
            #     # print(post['comments'])
            #     comment_page += 1
            # post_chunk[idx] = post

        # save chunk to file
        fn = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), f"data/{gall_id}_{idx_hash}.json"
        )
        with open(fn, "w", encoding="UTF-8-sig") as f_data:
            json.dump(post_chunk, f_data, indent=4, ensure_ascii=False)
        fn = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), f"data/{gall_id}_deleted.json"
        )
        with open(fn, "w", encoding="UTF-8-sig") as f_data:
            json.dump(idx_deleted, f_data, ensure_ascii=False)
    t_end = datetime.now()
    print(
        f"\rfetching post info - post {i - n_failed}/{len(urls) - n_failed}, {url}, Done. {t_end - t_start} elapsed.".ljust(
            200
        )
    )


if __name__ == "__main__":
    main()
