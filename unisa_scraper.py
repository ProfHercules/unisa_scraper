from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pprint

import requests
from requests import Response
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException
from typing import Dict

from models import Module, ModuleGroup, ModuleLevel, Qualification

# constants
host = "https://www.unisa.ac.za"
starting_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"
request_headers = {
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB, en-US",
}


class ChromeDriverManager(object):
    def __init__(self, driver_count: int = 8):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.headless = True
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--no-sandbox")
        # self.chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
        self.drivers: [WebDriver] = []

        self.issues = []

    def __del__(self):
        for driver in self.drivers:
            driver.quit()

    def get_driver(self) -> WebDriver:
        driver = webdriver.Chrome("./chromedriver", chrome_options=self.chrome_options)
        self.drivers.append(driver)
        return driver


class UnisaScraperV2(object):
    def __init__(self):
        self.issues: [str] = []
        self.heading_list: [str] = []

    def get_headings(self):
        return self.heading_list

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
        max_workers = 1  # min(32, os.cpu_count() + 4)
        print(f"Starting ThreadPoolExecutor with max_workers={max_workers}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in links:
                future = executor.submit(self.__get_qualification_data, link)
                futures.append(future)

            for future in as_completed(futures):
                q: Qualification = future.result()
                progress = round(float(q_count) / float(len(links)) * 100.0, 1)
                print(f"Parsed ({q_count}/{len(links)} ~ {progress}%): {q.code} [Issues: {len(self.issues)}]")
                pp = pprint.PrettyPrinter(indent=2)
                pp.pprint(q.to_print())
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
        modules = []

        for row in rows:
            tr: Tag = row
            if tr.attrs.get("class") is None:
                link = tr.find("td").find("a")
                href = link.get("href")
                module = self.__get_module_data(f"{host}{href}")
                modules.append(module)
            else:
                group_heading = tr.find("td").text
                if heading != "":
                    results.append(ModuleGroup(heading=heading, modules=modules))
                    modules = []
                heading = group_heading

        results.append(ModuleGroup(heading=heading, modules=modules))
        return results

    # for each module in self dict
    @staticmethod
    def __get_module_data(module_link: str) -> Module:
        # get basic data
        response: Response = requests.get(module_link)
        html: BeautifulSoup = BeautifulSoup(response.content, "lxml")

        title = html.find("h1").text.rsplit("-", maxsplit=1)
        name = title[0]
        code = title[1]
        info_table = html.find("table").find("tbody")
        rows = info_table.find_all("tr")

        basic_info = rows.pop(0).find_all("td")

        levels = basic_info[0].text.split(",")
        duration = basic_info[1].text
        nqf_lvl = int(basic_info[2].text[-1:])
        creds = int(basic_info[3].text.split(": ")[1])

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

        return Module(
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