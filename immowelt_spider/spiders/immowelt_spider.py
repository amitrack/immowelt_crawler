import logging

import scrapy
import js2xml
import urllib.parse as urlparse
from urllib.parse import parse_qs

from scrapy.exceptions import DropItem

from immowelt_spider.items import ImmoweltItem


class ImmoweltSpider(scrapy.Spider):
    name = 'immowelt_spider'
    allowed_domains = ['immowelt.de']
    # start_urls = ['http://immowelt.de/']
    result_list_xpath = """//*[contains(@class,"js-listitem")]/a/@href"""
    next_page_xpath = """//*[@id="nlbPlus"]/@href"""
    result_xpath = """/html/body/script[1]/text()"""
    image_xpath = """/html/head/meta[@property="og:image"][1]/@content"""
    ajax_url = "https://www.immowelt.de/liste/getlistitems"
    custom_settings = {"CONNECTION_STRING": "EXAMPLE_CONNECTION_STRING",
                       "CRAWL_ID": "DEFAULT"}
    offset = 0
    page_size = 4
    simple_mappings = [("object_id", "immowelt_id"),
                       ("object_price", "price"),
                       ("object_currency", "currency"),
                       ("object_rooms", "rooms"),
                       ("object_area", "living_area"),
                       ("object_features", "features"),
                       ("object_zip", "zip_code")]
    list_mapping = [("object_gok", "gok"),
                    ("object_city", "city"),
                    ("object_marketingtype", "transaction_type"),
                    ("object_district", "district"),
                    ("object_federalstate", "federal_state"),
                    ("object_state", "country"),
                    ("object_objektart", "type")]
    address_xpath = """//*[@id="divLageinfos"]/div[1]/div/div/div/div[1]/div[2]/p/text()"""
    brokers_url_xpath = """//*[@id="divAnbieter"]/div/div/div[1]/div/div[2]/div/div/div[1]/ul/li[3]/a/@href"""
    brokers_name_xpath = """//*[@id="srcLabelMessage"]/text()"""
    title_xpath = """//*[@id="expose"]/div[3]/div[1]/div/div[1]/h1/text()"""

    def start_requests(self):
        yield scrapy.Request(self.url)

    def parse(self, response, **kwargs):
        if response.request.url.startswith("https://www.immowelt.de/liste"):
            return self.parse_search_list(response)

    def parse_search_list(self, response):
        results = response.xpath(self.result_list_xpath).extract()
        for result in results:
            yield scrapy.Request(response.urljoin(result),
                                 callback=self.parse_result)
        for i in range(4):
            self.offset += 1
            yield scrapy.FormRequest(url=self.ajax_url,
                                     formdata=self.extract_params(
                                         response.request.url),
                                     callback=self.parse_ajax_search_list)
        next_page = response.xpath(self.next_page_xpath).extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page),
                                 self.parse_search_list)

    def parse_ajax_search_list(self, response):
        results = response.xpath(self.result_list_xpath).extract()
        for result in results:
            if "/beta/" in result:
                result = result.replace("beta/", "")
            yield scrapy.Request(response.urljoin(result),
                                 callback=self.parse_result)

    def parse_result(self, response):
        extract = lambda xpath, target_label: self.extract_value(xpath,
                                                                 target_label,
                                                                 response, item)
        result_js = response.xpath(self.result_xpath).extract_first()
        if result_js is None:
            raise DropItem("Invalid item found")
        parsed_results = js2xml.parse(result_js)
        value = js2xml.getall(parsed_results.xpath("var")[0])[0]
        item = ImmoweltItem()
        for source, target in self.simple_mappings:
            item[target] = value.get(source, None)
        for source, target in self.list_mapping:
            item[target] = value.get(source, [None])[0]
        if item["features"] is None:
            item["features"] = []
        item["type"] = item["type"].upper()
        extract(self.brokers_url_xpath, "broker_url")
        extract(self.brokers_name_xpath, "broker")
        item["broker"] = item["broker"].replace("Ihre Nachricht an ", "")
        if "den Anbieter" in item["broker"]:
            item["broker"] = ""
        extract(self.image_xpath, "image_src")
        extract(self.address_xpath, "address")
        if item["zip_code"]:
            if item["address"].startswith(item["zip_code"]):
                item["city"] = item["address"].replace(item["zip_code"],
                                                       "").strip()
        extract(self.title_xpath, "title")
        item["url"] = response.request.url
        yield item

    def extract_value(self, xpath_string: str, target_label, response, item):
        value = response.xpath(xpath_string).extract_first()
        if value:
            item[target_label] = value.strip()

    def extract_params(self, url):
        params = urlparse.urlparse(url).query
        return {"query": params, "offset": str(self.offset * self.page_size),
                "pageSize": str(self.page_size)}
