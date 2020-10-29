from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pprint

import pickle
import requests
from requests import Response
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from typing import Dict, Optional

from threading import Lock

from models import Module, ModuleGroup, ModuleLevel, Qualification

# constants
host = "https://www.unisa.ac.za"
starting_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"
request_headers = {
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB, en-US",
}


class UnisaScraperV2(object):
    def __init__(self):
        self.issues: [str] = []
        self.heading_list: [str] = []
        self.lock = Lock()
        self.modules: Dict[str, Module] = {}
        if os.path.isfile("modules.pkl"):
            with open("modules.pkl", 'rb') as f:
                self.modules = pickle.load(f)

        self.dump_lock = Lock()
        self.dump_count = 0
        self.dump_freq = 256

    def get_headings(self):
        return self.heading_list

    def get_cached_module(self, url: str) -> Module:
        if url in self.modules:
            result = self.modules[url]
            return result

    def dump_module_list(self):
        try:
            print("Dumping list to pickle file...")
            with open("modules.pkl", 'wb') as f:
                pickle.dump(self.modules, f)
        except Exception as e:
            print(e)

    def add_module(self, module: Module):
        self.lock.acquire()
        self.modules[module.url] = module

        with self.dump_lock:
            self.dump_count += 1
            if self.dump_count >= self.dump_freq:
                self.dump_module_list()
                self.dump_count = 0

        self.lock.release()

    # start with root link
    # get all qualification links
    @staticmethod
    def __get_all_qualification_links() -> [str]:
        results: [str] = []
        raw_list_page = requests.get(f"{host}{starting_link}")
        parsed_list_html = BeautifulSoup(raw_list_page.content, 'html.parser')

        all_links: ResultSet = parsed_list_html.find_all('a')

        for q_link in all_links:
            href: str = q_link.get("href")
            if href is not None and href.startswith(starting_link):
                results.append(f"{host}{href}")
        print(f"Extracted {len(results)} links")

        return results

    def get_qualifications(self) -> [Qualification]:
        links = self.__get_all_qualification_links()
        futures = []

        q_count = 0

        qualifications: [Qualification] = []
        max_workers = min(32, os.cpu_count() + 4)
        print(f"[Qualification] Starting ThreadPoolExecutor with max_workers={max_workers}")
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

        return qualifications

    # for each
    def __get_qualification_data(self, qualification_link: str) -> Qualification:
        # get basic data
        response: Response = requests.get(qualification_link, headers=request_headers)
        html: BeautifulSoup = BeautifulSoup(response.content, "lxml")

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
                    stream = data[1].text
                elif data[0].text == "Qualification code:":
                    code = data[1].text
                elif data[0].text == "NQF level:":
                    nqf_lvl = int(data[1].text)
                elif data[0].text == "Total credits:":
                    total_credits = int(data[1].text)
                elif data[0].text == "SAQA ID:":
                    saqa_id = data[1].text
                elif data[0].text == "APS/AS:":
                    aps_as = int(data[1].text)
                elif "Purpose statement:" in data[0].text:
                    purpose = data[0].text
                elif "Rules:" in data[0].text:
                    rules = data[0].text

            name = name.replace(code, "")

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
                group_heading = tr.find("td").text
                if heading != "":
                    modules = self.__get_modules_from_links(links)
                    results.append(ModuleGroup(heading=heading, modules=modules))
                    assert len(modules) > 0
                    self.heading_list.append(heading)
                    links = []
                heading = group_heading

        modules = self.__get_modules_from_links(links)
        results.append(ModuleGroup(heading=heading, modules=modules))
        return results

    def __get_modules_from_links(self, links: [(str, str)]) -> [Module]:
        futures = []

        modules: [Module] = []
        min_workers = len(links) if len(links) > 0 else 1
        max_workers = min(32, os.cpu_count() + 4, min_workers)
        # print(f"[Module] Starting ThreadPoolExecutor with max_workers={max_workers}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in links:
                future = executor.submit(self.__get_module_data, link)
                futures.append(future)

            for future in as_completed(futures):
                mod: Module = future.result()
                if mod is not None:
                    modules.append(mod)

        # print(f"[Module] {max_workers} workers finished")
        return modules

    # for each module in self dict
    def __get_module_data(self, module_link: (str, str)) -> Optional[Module]:
        name, url = module_link
        if (cached := self.get_cached_module(url)) is not None:
            # print("Returning pre-parsed Module from cache")
            return cached
        # get basic data
        response: Response = requests.get(url)
        if response.status_code == 404:
            module = Module(url=url, name=name)
            self.issues.append(f"Module {name} does not exist")
            self.add_module(module)
            return module

        html: BeautifulSoup = BeautifulSoup(response.content, "lxml")

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
                elif "Purpose statement:" in data_point.text:
                    purpose = data_point.text

        module = Module(
            url=module_link,
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
        self.add_module(module)
        return module
