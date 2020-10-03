from concurrent.futures import ThreadPoolExecutor, wait

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException

from models import Qualification, Module

# constants
host = "https://www.unisa.ac.za"
max_workers = 8


class UnisaScraper(object):
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.headless = True
        self.drivers: [WebDriver] = []

        self.issues = []

    def __del__(self):
        for driver in self.drivers:
            driver.quit()

    def get_driver(self) -> WebDriver:
        driver = webdriver.Chrome("./chromedriver", options=self.chrome_options)
        self.drivers.append(driver)
        return driver

    def get_qualifications(self, base_link: str) -> [Qualification]:
        qualification_links = self.get_all_qualification_links(base_link)
        futures = []

        q_count = 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in qualification_links:
                print(f"Getting qualification #{q_count}")
                future = executor.submit(self.get_qualification, link)
                futures.append(future)
                q_count += 1

        print(f"Done! Processed {q_count - 1} links")

        wait(futures)

        qualifications = []
        for future in futures:
            q: Qualification = future.result()
            print(f"Parsed: {q.code}")
            qualifications.append(future.result())

        print("Issues:", len(self.issues))
        return qualifications

    @staticmethod
    def get_all_qualification_links(link: str) -> [str]:
        raw_list_page = requests.get(f"{host}{link}")
        parsed_list_html = BeautifulSoup(raw_list_page.content, 'html.parser')

        maybe_qual_links = parsed_list_html.find_all('a')

        q_links = []

        for q_link in maybe_qual_links:
            href = q_link.get("href")
            if href is not None and href[0:161] == link:
                q_links.append(f"{host}{href}")

        return q_links[2:3]

    def get_qualification(self, url: str) -> Qualification:
        driver = self.get_driver()
        driver.get(url)
        title_end = driver.title.rfind("(")
        name = driver.title[0:title_end]

        info_table = driver.find_element_by_class_name("table").find_element_by_tag_name("tbody")
        rows = info_table.find_elements_by_tag_name("tr")

        stream: str = ""
        code: str = ""
        nqf_lvl: int = 0
        total_credits: int = 0
        saqa_id: str = ""
        aps_as: int = 0
        purpose: str = ""

        for row in rows:
            data = row.find_elements_by_tag_name("td")
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

        module_links = self.get_module_links(driver)
        mods = self.get_modules(module_links)

        return Qualification(
            url=url,
            name=name,
            stream=stream,
            code=code,
            nqf_level=nqf_lvl,
            total_credits=total_credits,
            saqa_id=saqa_id,
            aps_as=aps_as,
            purpose_statement=purpose,
            modules=mods,
        )

    def get_modules(self, links: [str]) -> [Module]:
        mods: [Module] = []
        m_cnt = 1
        skips = 0
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for link in links:
                future = executor.submit(self.get_module, link)
                futures.append(future)
                m_cnt += 1

        wait(futures)

        for future in futures:
            m: Module = future.result()
            print(f"Parsed: {m.code}")
            mods.append(m)

        return mods

    def get_module_links(self, dvr: WebDriver) -> [str]:
        links = []
        tables = dvr.find_elements_by_class_name("table-responsive")
        for table in tables:
            rows = table.find_elements_by_tag_name("tr")

            for row in rows:
                columns = row.find_elements_by_tag_name("td")
                if len(columns) > 0 and columns[0].text[0:5] != "Group" and len(columns[0].text) > 6:
                    try:
                        url = columns[0].find_element_by_tag_name("a").get_property("href")
                        links.append(url)
                    except NoSuchElementException as e:
                        self.issues.append(e)

        return links

    def get_module(self, url: str) -> Module:
        driver = self.get_driver()
        driver.get(url)
        name, code = driver.title.split(" - ", maxsplit=2)

        info_table = driver.find_element_by_class_name("table").find_element_by_tag_name("tbody")
        rows = info_table.find_elements_by_tag_name("tr")
        basic_info = rows[0].find_elements_by_tag_name("td")
        levels = basic_info[0].text.split(",")
        duration = basic_info[1].text
        nqf_lvl = int(basic_info[2].text[-1:])
        creds = int(basic_info[3].text.split(": ")[1])
        purpose: str = rows[2].find_element_by_tag_name("td").text.replace("Purpose: ", "")

        return Module(
            url=url,
            name=name,
            code=code,
            levels=levels,
            duration=duration,
            nqf_level=nqf_lvl,
            credits=creds,
            purpose=purpose,
        )
