
import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector
from scrapy_splash import SplashRequest

class QuotesSpider(scrapy.Spider):
	name = "diaoconline"

	def start_requests(self):
		print ("GO HERE")
		self.is_updated=False
		self.index = 0
		urls = [
		'http://diaoconline.vn/sieu-thi/loc/?tindang=1',
		'http://diaoconline.vn/sieu-thi/loc/?tindang=2'
		]

		for url in urls:
			yield SplashRequest(url, self.parse)

	def convert_unicode(self,text):
		if text=='':
			return text
		text=re.sub(unichr(272),'D',text);
		text=re.sub(unichr(273),'d',text);
		text=unicodedata.normalize('NFKD', text).encode('ascii','ignore')
		text=text.replace('\n','')
		text=text.replace('\t','')
		text=text.replace('\r','')
		return text

	def convert_price(self,price):
		list_price = price.split()
		if len(price) > 3:
			real_price = float(list_price[0])*1000000000 + float(list_price[2])*1000000
		else:
			if 'trieu' in price:
				real_price = float(list_price[0])*1000000
			else:
				real_price = float(list_price[0])*1000000000
		return real_price

	def parse(self, response):
		print (" START TO PARSE")

		# Get all posts
		items = response.xpath("//li[contains(@class,'hightlight_type_1 margin_bottom')]")

		if items == []:
			return

		# Process the first page
		if response.url.find("pi") == -1 and self.is_updated == False:
			print ("Process First Page")
			self.is_updated = True
			with open('last_post_id.json', 'r+') as f:
				data=json.load(f)
				self.last_post_time = ''
				if "diaoconline" in data:
					self.last_post_time = datetime.datetime.strptime(data["diaoconline"],"%d-%m-%Y %H:%M")
					data["diaoconline"] = (datetime.datetime.now()-datetime.timedelta(minutes=4)).strftime("%d-%m-%Y %H:%M")

			os.remove('last_post_id.json')
			with open('last_post_id.json','w') as f:
				json.dump(data,f,indent = 4)
					
		for item in items:
			# Get URL of each item
			item_url = item.xpath("//div[@class='info margin_left']/h2/a/@href/text()")
			item_url =  "http://diaoconline.vn" + item_url
			print ("ITEM_URL: " )
			print(item_url)
			yield scrapy.Request(item_url,callback=self.parse_item)

		self.index = self.index + 1
		print ("INDEX: " + self.index)

		if response.url.find("tindang=1") == -1: # Rent
			next_href = "http://diaoconline.vn/sieu-thi/loc/?tindang=1&pi=" + char(self.index);
		else: # Sell
			next_href = "http://diaoconline.vn/sieu-thi/loc/?tindang=2&pi=" + char(self.index);
		# Go to next page
		print ('NEXT PAGE URL: ' + next_href)
		yield SplashRequest(next_href, callback=self.parse)

	def	parse_item(self, response):
		# Get post time
		post_time = self.convert_unicode(response.xpath("//span[@class='post_type']/text()").extract_first())
		if('truoc' in post_time):
			post_time=datetime.datetime.now()
		else:
			date=datetime.datetime.strptime(date,"%d-%m-%Y")
		if post_time < self.last_post_time:
			print(post_time.strftime("%d-%m-%Y"),self.last_post_time.strftime("%d-%m-%Y"),response.url)
			return

		# Get price of property
		price = response.xpath("//div[contains(@class, 'money')]/text()").extract_first()
		price = price.strip()
		price = self.convert_unicode(price)
		if ('luong' in price):
			return 

		price = self.convert_price(re.sub('Gia:','',price))

		# Get description of property
		description = self.convert_unicode(response.xpath('//div[contains(@class,"body")]/p/text()').extract_first())

		# Get Title
		title = self.convert_unicode(response.xpath('//h1[contains(@class, "larger_title")]/text()').extract_first())

		# Get Post ID
		post_id = response.xpath("//div[contains(@class, 'feat_item')]/dl/dd").extract_first()

		# Get Area
		area = re.sub('[\r\n]', '', self.convert_unicode(response.xpath("//div[@class='feat_item']/dl/dd/text()")[1].extract()))

		# Get author name
		author_name = self.convert_unicode(response.xpath('//div[contains(@class, "body")]/h4/a/text()').extract_first())

		# Get author phone number
		author_phone = response.xpath("//div[@class='body']/dl/dd/span/text()").extract_first()

		# Get location of property
		location = re.sub('Vi tri:\r\n','',self.convert_unicode(response.xpath('//span[contains(@class, "location")]/text()').extract_first()))

		# Get provience
		location_array = location.split(',')
		province = location_array[3]

		# Get county
		county = location_array[2]
		
		# Get house type
		house_type = self.convert_unicode(response.xpath("//strong/a[@class='link-ext']/text()").extract_first())

		# Get transaction type
		transaction_type = self.convert_unicode(response.xpath("//span[@itemprop='title']/text()")[1].extract())
		
		yield {
			'post-id': post_id
		} 

		