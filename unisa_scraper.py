import os

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException

from models import Qualification, Module

host = "https://www.unisa.ac.za"
all_qual_link = "/sites/corporate/default/Register-to-study-through-Unisa/Undergraduate-&-honours-qualifications/Find-your-qualification-&-choose-your-modules/All-qualifications/"


class UnisaScraper(object):
    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")

        chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
        self.driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"),
                                       chrome_options=chrome_options)

    def __del__(self):
        self.driver.quit()

    def get_qualifications(self) -> [Qualification]:
        qualifications = []
        q_links = self.get_all_qualification_links()

        q_count = 1
        skips = 0
        for link in q_links:
            print(f"Getting qualification #{q_count}")
            try:
                q = self.get_qualification(self.driver, link)
                qualifications.append(q)
            except NoSuchElementException:
                skips += 1
                print(f"NoSuchElementException! Skipping {link}")

            q_count += 1
        print(f"Done! Processed {q_count - 1} links. Skipped {skips}")

        return qualifications

    @staticmethod
    def get_all_qualification_links() -> [str]:
        raw_list_page = requests.get(f"{host}{all_qual_link}")
        parsed_list_html = BeautifulSoup(raw_list_page.content, 'html.parser')

        maybe_qual_links = parsed_list_html.find_all('a')

        q_links = []

        for q_link in maybe_qual_links:
            href = q_link.get("href")
            if href is not None and href[0:161] == all_qual_link:
                q_links.append(f"{host}{href}")

        return q_links

    def get_qualification(self, dvr: WebDriver, url: str) -> Qualification:
        dvr.get(url)
        title_end = dvr.title.rfind("(")
        name = dvr.title[0:title_end]

        info_table = dvr.find_element_by_class_name("table").find_element_by_tag_name("tbody")
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

            if data[0].text == "Qualification code:":
                code = data[1].text

            if data[0].text == "NQF level:":
                nqf_lvl = int(data[1].text)

            if data[0].text == "Total credits:":
                total_credits = int(data[1].text)

            if data[0].text == "SAQA ID:":
                saqa_id = data[1].text

            if data[0].text == "APS/AS:":
                aps_as = int(data[1].text)

            if "Purpose statement:" in data[0].text:
                purpose = data[0].text

        module_links = self.get_module_links(dvr)

        mods: [Module] = []
        m_cnt = 1
        skips = 0
        for module_link in module_links:
            print(f"Getting module #{m_cnt}")
            try:
                module = self.get_module(dvr, module_link)
                mods.append(module)
            except ValueError:
                print("ValueError! Skipping...")
                skips += 1
            m_cnt += 1
        print(f"Got {m_cnt - 1} modules, skipped {skips} modules")

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

    @staticmethod
    def get_module_links(dvr: WebDriver) -> [str]:
        links = []
        tables = dvr.find_elements_by_class_name("table-responsive")
        for table in tables:
            rows = table.find_elements_by_tag_name("tr")

            for row in rows:
                columns = row.find_elements_by_tag_name("td")
                if len(columns) > 0 and columns[0].text[0:5] != "Group":
                    url = columns[0].find_element_by_tag_name("a").get_property("href")
                    links.append(url)
        return links

    @staticmethod
    def get_module(dvr: WebDriver, url: str) -> Module:
        dvr.get(url)
        name, code = dvr.title.split(" - ", maxsplit=2)

        info_table = dvr.find_element_by_class_name("table").find_element_by_tag_name("tbody")
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
