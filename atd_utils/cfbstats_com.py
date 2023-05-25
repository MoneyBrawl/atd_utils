# flake8: noqa
import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
from typing import List, Union, Tuple
import json
from os.path import join
from urllib.parse import urlparse

# from lxml import etree

DOMAIN_NAME = "cfbstats.com"
BASE_URL = "http://" + DOMAIN_NAME
START_SEARCH = [BASE_URL]
SLEEP = 0
seen = set(START_SEARCH)


def get_soup(
    url: str, headers: dict, return_response: bool = False
) -> Union[BeautifulSoup, Tuple[BeautifulSoup, requests.Response]]:

    global SLEEP
    time.sleep(SLEEP)
    try:
        response = requests.get(url, headers=headers)
    except requests.exceptions.TooManyRedirects:
        return None
    except ConnectionResetError:
        if SLEEP == 0:
            SLEEP = 1
        else:
            SLEEP *= 2
        print(f"!!!!! SLEEP NOW AT {SLEEP}")
        return ""
    soup = BeautifulSoup(response.content, "html.parser")
    if return_response:
        return soup, response
    else:
        return soup


def clean_url(url: str) -> str:
    if url[0] == "/" and len(url) <= 9:
        return ""
    elif url[0] == "/" and url[:9] == "/stories/":
        url = BASE_URL + url
    elif url[0] != "h":
        return ""

    parsed_url = urlparse(url)
    clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

    if DOMAIN_NAME not in parsed_url.netloc:
        clean_url = ""
    return clean_url


def keep_link(url: str) -> bool:
    base_len = len(BASE_URL)
    url_len = len(url)

    if url in seen:
        return False
    if url_len < base_len:
        return False
    elif len(url) < 36:
        return False
    elif url[:36] != "https://www.producthunt.com/stories/":
        return False
    else:
        return True


def get_article_links(soup: BeautifulSoup) -> List[str]:
    teams_dict = {
        x.find("a").text: x.find("a", href=True)["href"]
        for x in soup.find_all("li", {"class": "sub1"})
    }
    clean_links = [
        clean_url(elem.get("href"))
        for elem in soup.find_all("a")
        if elem.get("href") != "#" and elem.get("href") and len(elem.get("href")) > 0
    ]
    new_links = [link for link in clean_links if keep_link(link)]
    return new_links


def scrape():
    ua = UserAgent()
    to_search = set(START_SEARCH)
    url = to_search.pop()
    soup = get_soup(url, {"User-Agent": ua.random})
    new_links = get_article_links(soup)
    to_search.update(new_links)
    while len(to_search) > 0:
        print(f"Num urls still in search queue: {len(to_search)}")
        url = to_search.pop()
        print(url)
        soup, response = get_soup(url, {"User-Agent": ua.random}, return_response=True)
        if soup == "":
            to_search.append(url)
            continue
        seen.add(url)
        if soup is None:
            continue
        new_links = get_article_links(soup)
        to_search.update(new_links)

        category_prefix = "https://www.producthunt.com/stories/category/"
        if url in START_SEARCH or (
            len(url) >= len(category_prefix)
            and url[: len(category_prefix)] == category_prefix
        ):
            continue

        article_title = soup.find("h1")
        if article_title is not None:
            article_title = article_title.text.strip()

        main_div = [
            x
            for x in soup.find_all("div", attrs={"class": "layoutContainer"})
            if len(x["class"]) == 1
        ]
        if len(main_div) == 1:
            divs = [
                x
                for x in main_div[0].find_all("div")
                if "class" in x.attrs.keys()
                and "color-darker-grey" in x["class"]
                and "hover-undefined" not in x["class"]
            ]
        else:
            print("")

        article_content = ""
        for div in divs:
            article_content += div.text.strip() + "\n"

        if (
            article_content is None
            or article_content == ""
            or "crypto" in article_content.lower()
            or "nft" in article_content.lower()
        ):
            continue

        if len(article_content.strip()) > 0:
            ret = {"title": article_title}
            ret["content"] = article_content.strip()
            ret["url"] = url
            yield ret

        seen.add(url)


if __name__ == "__main__":
    for ix, article in enumerate(scrape()):
        jsonl_data += "\n" + json.dumps(article)
        if ix % 100 == 0:
            try:
                blob.upload_from_string(jsonl_data)
            except ConnectionError:
                blob = bucket.get_blob(BLOB_NAME)
                blob.upload_from_string(jsonl_data)
            print(ix)
blob.upload_from_string(jsonl_data)
