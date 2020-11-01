from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os
import pprint
import time

import pickle
import hashlib
from urllib.parse import urlparse
import requests
from requests import Response
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from typing import Dict, Optional, Union, Set
import re
from os import listdir
from os.path import isfile, join

import copy

from threading import Lock

from models import Module, ModuleGroup, ModuleLevel, Qualification

from random import shuffle

# constants
host = "https://www.unisa.ac.za"
starting_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"
request_headers = {
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB, en-US",
}


class CachedRequester(object):
    def __init__(self):
        self.cache: Dict[str, Response] = {}
        self.lock = Lock()
        self.load_cache()
        self.cache_update_count = 0
        self.cache_dump_at_updates = 64
        self.queue: [str] = []

    def dump_cache(self):
        if self.lock.locked():
            print(f"Dumping cache with {len(self.cache)} items")
            with open("response_cache.pkl", "wb") as f:
                to_dump = copy.deepcopy(self.cache)
                pickle.dump(to_dump, f)
        else:
            with self.lock:
                self.dump_cache()

    def load_cache(self):
        with self.lock:
            if os.path.isfile("response_cache.pkl"):
                with open("response_cache.pkl", "rb") as f:
                    self.cache = pickle.load(f)
                print(f"Loaded {len(self.cache)} cache items from file-system")

    def cached_request(self, url: str) -> Response:
        # trivial, url is cached so return data
        if url in self.cache:
            return self.cache[url]
        # url is not in cache
        else:
            print("Cache miss")
            if url in self.queue:
                while url not in self.cache:
                    print(f"[{threading.get_ident()}] Waiting for {url[-5:]}")
                    time.sleep(1)
                self.queue = list(filter(lambda a: a != url, self.queue))
                return self.cache[url]
            # manually do request and cache
            self.queue.append(url)
            resp: Response = requests.get(url, headers=request_headers)
            self.queue.remove(url)
            self.cache[url] = resp
            with self.lock:
                self.cache_update_count += 1
                if self.cache_update_count >= self.cache_dump_at_updates:
                    self.dump_cache()
                    self.cache_update_count = 0
            return resp


class UnisaScraperV2(object):
    def __init__(self):
        self.issues: [str] = []
        self.lock = Lock()
        self.modules: Dict[str, Module] = {}
        self.cached_requester = CachedRequester()

    @staticmethod
    def get_headings(qualifications: [Qualification]) -> [str]:
        headings: [str] = []
        for qualification in qualifications:
            lvl_count = 0
            for lvl in qualification.module_levels:
                lvl_count += 1
                grp_cnt = 0
                for group in lvl.module_groups:
                    grp_cnt += 1
                    if group.heading.strip() == ".":
                        print(lvl_count, grp_cnt)
                    headings.append(group.heading)
        return headings

    def cache_module(self, module: Module):
        if self.get_cached_module(module.url) is None:
            with self.lock:
                self.modules[module.url] = module

    def get_cached_module(self, url: str) -> Module:
        if url in self.modules:
            result = self.modules[url]
            return result

    @staticmethod
    def get_max_threads():
        return min(32, os.cpu_count() + 4)

    def __get_all_qualification_links(self) -> [str]:
        results: [str] = []
        raw_list_page = self.cached_requester.cached_request(f"{host}{starting_link}")
        parsed_list_html = BeautifulSoup(raw_list_page.content, 'html.parser')

        all_links: ResultSet = parsed_list_html.find_all('a')

        for q_link in all_links:
            href: str = q_link.get("href")
            if href is not None and href.startswith(starting_link):
                results.append(f"{host}{href}")
        print(f"Extracted {len(results)} links")

        return results

    def get_modules(self) -> [Module]:
        assert len(self.modules.values()) != 0
        return self.modules.values()

    def get_qualifications(self) -> [Qualification]:
        links = self.__get_all_qualification_links()
        futures = []

        q_count = 0

        qualifications: [Qualification] = []
        max_workers = self.get_max_threads()
        print(f"[Qualification] Starting ThreadPoolExecutor with max_workers={max_workers}")
        shuffle(links)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in links:
                future = executor.submit(self.__get_qualification_data, link)
                futures.append(future)

            for future in as_completed(futures):
                q: Qualification = future.result()
                progress = round(float(q_count) / float(len(links)) * 100.0, 1)
                print(f"Parsed ({q_count}/{len(links)} ~ {progress}%): {q.code} [Issues: {len(self.issues)}]")
                q_count += 1
                qualifications.append(future.result())

            print(f"Done! Processed {q_count} links")
            if len(self.issues) > 0:
                print("Issues:", len(self.issues))
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(self.issues)

        self.cached_requester.dump_cache()
        return qualifications

    # for each
    def __get_qualification_data(self, qualification_link: str) -> Qualification:
        response: Response = self.cached_requester.cached_request(qualification_link)

        html: BeautifulSoup = BeautifulSoup(response.content, "html.parser")

        try:
            url: str = qualification_link
            name: str = html.find("title").text

            # info should be first table on page
            info_table = html.find("tbody")
            info_rows: [Tag] = info_table.find_all("tr")

            stream: str = ""
            code: str = ""
            nqf_level: int = 0
            total_credits: int = 0
            saqa_id: str = ""
            aps_as: int = 0
            purpose: str = ""
            rules: str = ""

            for info_row in info_rows:
                data: [Tag] = info_row.find_all("td")

                if data[0].text == "Qualification stream:":
                    stream = data[1].text.strip()
                    stream = re.sub(r"^\((?P<srm>.*)\)$", "\g<srm>", stream)
                elif data[0].text == "Qualification code:":
                    code = data[1].text.strip()
                elif data[0].text == "NQF level:":
                    nqf_level = int(data[1].text.strip())
                elif data[0].text == "Total credits:":
                    total_credits = int(data[1].text.strip())
                elif data[0].text == "SAQA ID:":
                    saqa_id = data[1].text.strip()
                elif data[0].text == "APS/AS:":
                    aps_as = int(data[1].text.strip())
                elif "Purpose statement:" in data[0].text:
                    purpose = data[0].text.replace("Purpose statement:", "", 1).strip()
                elif "Rules:" in data[0].text:
                    rules = data[0].text.replace("Rules:", "", 1).strip()

            name = name.replace(f"({code})", "").strip()
            if name.count(stream) > 1:
                name = name.replace(stream, "", 1).replace("()", "").strip()

            # build module link list
            mod_levels: [ModuleLevel] = self.__get_module_levels_from(html)
            # add module links to self dict
            # add ref to qualification, for future reference

            return Qualification(
                url=url,
                name=name,
                stream=stream,
                code=code,
                nqf_level=nqf_level,
                total_credits=total_credits,
                saqa_id=saqa_id,
                aps_as=aps_as,
                purpose=purpose,
                rules=rules,
                module_levels=mod_levels,
            )
        except AttributeError as error:
            self.issues.append(error)
            print(error)

    def __get_module_levels_from(self, page: Tag) -> [ModuleLevel]:
        results: [ModuleLevel] = []

        tables = page.find_all(class_="table-responsive")

        for table in tables:
            groups = self.__get_module_groups_from(table)
            results.append(ModuleLevel(module_groups=groups))

        return results

    @staticmethod
    def normalize_heading(heading: str) -> str:
        result: str = heading.strip()
        # "(?i)(compulsory+\.?)", "Compulsory"
        result = re.sub(r"compulsory?\.?", "Compulsory", result, flags=re.IGNORECASE)
        # "(?i)one", "1"
        result = re.sub(r"one", "1", result, flags=re.IGNORECASE)
        # "(?i)two", "2"
        result = re.sub(r"two", "2", result, flags=re.IGNORECASE)
        # "(?i)three", "3"
        result = re.sub(r"three", "3", result, flags=re.IGNORECASE)
        # "(?i)four", "4"
        result = re.sub(r"four", "4", result, flags=re.IGNORECASE)
        # "(?i)five", "5"
        result = re.sub(r"five", "5", result, flags=re.IGNORECASE)
        # "(?i)six", "6"
        result = re.sub(r"six", "6", result, flags=re.IGNORECASE)
        # "(?i)seven", "7"
        result = re.sub(r"seven", "7", result, flags=re.IGNORECASE)
        # "(?i)eight", "8"
        result = re.sub(r"eight", "8", result, flags=re.IGNORECASE)
        # "(?i)nine", "9"
        result = re.sub(r"nine", "9", result, flags=re.IGNORECASE)
        # "(?i)Select", "Choose"
        result = re.sub(r"select", "Choose", result, flags=re.IGNORECASE)
        # "^\.", "Compulsory "
        result = re.sub(r"^\.", "Compulsory", result, flags=re.IGNORECASE)
        # "[\.:;]$", ""
        result = re.sub(r"[\.:;]$", "", result, flags=re.IGNORECASE)
        # "Group ([A-Z])$", "Group $1."
        result = re.sub(r"Group (?P<grp>[A-Z])$", "Group \g<grp>", result, flags=re.IGNORECASE)
        # "from the list below", "from the following"
        result = result.replace("from the list below", "from the following")
        # "( ", "("
        result = result.replace("( ", "(")
        # " )", ")"
        result = result.replace(" )", ")")
        # "the following module$", "the following modules"
        result = re.sub(r"the following module$", "the following modules", result, flags=re.IGNORECASE)
        # "Choose any", "Choose"
        result = result.replace("Choose any", "Choose")
        # "Group ([A-Z]):", "Group $1."
        result = re.sub(r"Group (?P<grp>[A-Z]):", "Group \g<grp>.", result, flags=re.IGNORECASE)
        # "Choose ([0-9]) of the following", "Choose $1 from the following"
        result = re.sub(r"Choose (?P<num>[0-9]) of the following", "Choose \g<num> from the following", result, flags=re.IGNORECASE)
        # "Choose ([0-9]) modules? from the following", "Choose $1 from the following"
        result = re.sub(r"Choose (?P<num>[0-9]) modules? from the following", "Choose \g<num> from the following", result, flags=re.IGNORECASE)
        # "Choose ([0-9]) from the following modules", "Choose $1 from the following"
        result = re.sub(r"Choose (?P<num>[0-9]) from the following modules", "Choose \g<num> from the following", result, flags=re.IGNORECASE)
        # "Choose ([0-9]) from the following (groups of modules|subjects)", "Choose $1 from the following"
        result = re.sub(r"Choose (?P<num>[0-9]) from the following (groups of modules|subjects)", "Choose \g<num> from the following", result, flags=re.IGNORECASE)
        # "Group ([A-Z]). Compulsory Choose ALL modules (from|under) this group$", "Group $1. Compulsory"
        result = re.sub(r"Group (?P<grp>[A-Z])\. Compulsory Choose ALL modules (from|under) this group$", "Group \g<grp>. Compulsory", result, flags=re.IGNORECASE)
        # "(?i)Compulsory Modules$", "Compulsory"
        result = re.sub(r"Compulsory Modules$", "Compulsory", result, flags=re.IGNORECASE)
        # "(i?)Compulsory modules to major in ([A-z ]*)$", "Compulsory for $2 major"
        result = re.sub(r"Compulsory modules to major in (?P<mjr>[A-z ]*)$", "Compulsory for \g<mjr> major", result, flags=re.IGNORECASE)
        # "(?i)chooseed", "chosen"
        result = re.sub(r"choose+d", "chosen", result, flags=re.IGNORECASE)
        # "(\.+)", "."
        result = re.sub(r"(\.+)", ".", result, flags=re.IGNORECASE)
        # "^([A-Z])\.", "Group $1."
        result = re.sub(r"^(?P<grp>[A-Z])\.", "Group \g<grp>.", result, flags=re.IGNORECASE)
        return result

    def __get_module_groups_from(self, table: Tag) -> [ModuleGroup]:
        results: [ModuleGroup] = []
        tbody = table.find("tbody")
        if tbody is None:
            issue = "Couldn't find <tbody>"
            self.issues.append(issue)
            print(issue)
            return results

        rows: [Tag] = tbody.find_all("tr")
        rows.pop(0)

        heading: str = ""

        links: [(str, str)] = []

        for row in rows:
            tr: Tag = row
            if tr.attrs.get("class") is None:
                link = tr.find("td").find("a")
                href = link.get("href")
                name = link.text
                links.append((name, f"{host}{href}"))
            else:
                group_heading = self.normalize_heading(tr.find("td").text)
                if heading != "":
                    modules = self.__get_modules_from_links(links)
                    results.append(ModuleGroup(heading=heading, modules=modules))
                    assert len(modules) > 0
                    links = []
                heading = group_heading

        modules = self.__get_modules_from_links(links)
        results.append(ModuleGroup(heading=heading, modules=modules))
        return results

    def __get_modules_from_links(self, links: [(str, str)]) -> [Module]:
        futures = []

        modules: [Module] = []
        min_workers = len(links) if len(links) > 0 else 1
        max_workers = min(self.get_max_threads(), min_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in links:
                if (cached := self.get_cached_module(link)) is not None:
                    modules.append(cached)
                    continue
                future = executor.submit(self.__get_module_data, link)
                futures.append(future)

            for future in as_completed(futures):
                mod: Module = future.result()
                if mod is not None:
                    modules.append(mod)

        # print(f"[Module] Collected {len(modules)} modules.")
        return modules

    # for each module in self dict
    def __get_module_data(self, module_link: (str, str)) -> Optional[Module]:
        name, url = module_link
        response: Response = self.cached_requester.cached_request(url)
        if response.status_code == 404:
            module = Module(url=url, name=name)
            self.issues.append(f"Module {name} does not exist")
            self.cache_module(module)
            return module

        html: BeautifulSoup = BeautifulSoup(response.content, "html.parser")

        title = html.find("h1").text.rsplit("-", maxsplit=1)
        name = title[0].strip()
        code = title[1].strip()
        info_table = html.find("table").find("tbody")
        rows = info_table.find_all("tr")

        basic_info = rows.pop(0).find_all("td")

        levels: [str] = []
        duration: str = "Unspecified"
        nqf_lvl: int = 0
        creds: int = 0
        try:
            levels_str = basic_info[0].text
            duration_str = basic_info[1].text.strip()
            nqf_str = basic_info[2].text[-1:].strip()
            creds_str = basic_info[3].text.split(": ")[1]

            levels = levels_str.split(",") if levels_str != "" else []
            duration = duration_str if duration_str != "" else "Unspecified"
            nqf_lvl = int(nqf_str) if nqf_str != "" else 0
            creds = int(creds_str) if creds_str != "" else 0

        except ValueError:
            self.issues.append(f"Error for module {name}")

        purpose = ""
        pre_requisite = ""
        co_requisite = ""
        recommendation = ""

        for row in rows:
            data = row.find_all("td")
            for data_point in data:
                if "Pre-requisite:" in data_point.text:
                    pre_requisite = data_point.text
                elif "Co-requisite:" in data_point.text:
                    co_requisite = data_point.text
                elif "Recommendation:" in data_point.text:
                    recommendation = data_point.text
                elif "Purpose:" in data_point.text:
                    purpose = data_point.text

        module = Module(
            url=url,
            name=name,
            code=code,
            levels=levels,
            duration=duration,
            nqf_level=nqf_lvl,
            credits=creds,
            purpose=purpose,
            pre_requisite=pre_requisite,
            co_requisite=co_requisite,
            recommendation=recommendation,
        )
        self.cache_module(module)
        return module
