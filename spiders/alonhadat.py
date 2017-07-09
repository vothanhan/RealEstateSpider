# -*- coding: utf-8 -*-
import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector

class AlonhadatSpider(scrapy.Spider):
	name = 'alonhadat'
	words = []

	def start_requests(self):		
		self.is_updated = False 
		urls = [
		'http://alonhadat.com.vn/nha-dat/can-ban.html',
		'http://alonhadat.com.vn/nha-dat/cho-thue.html'
		]

		for url in urls:
			yield scrapy.Request(url=url,callback=self.parse)
		

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

	def convert_price(self, price):
		list_price = price.split()
		if price.find('/') != -1:
			real_price = float(list_price[0].replace(",",".")) * 1000000 * int(self.area)
			return real_price
		if list_price[1] == 'trieu':
			real_price = float(list_price[0].replace(",",".")) * 1000000
		else:
			real_price = float(list_price[0].replace(",",".")) * 1000000000
		return real_price


	def parse(self, response):
		print('Response URL: ' + response.url)
		# Get all items
		items = response.xpath(".//div[@class= 'content-item']/div/div[@class='ct_title']/a/@href")
		if response.url.find('trang') == -1 and self.is_updated == False: # Process the first page
			print ("Process First Page")
			self.is_updated = True
			with open('last_post_id.json','r+') as f:
				data=json.load(f)
				self.last_post_time=''
				if "alonhadat" in data:
					self.last_post_time = datetime.datetime.strptime(data["alonhadat"],"%d-%m-%Y %H:%M")
					data["alonhadat"] = (datetime.datetime.now()-datetime.timedelta(minutes=4)).strftime("%d-%m-%Y %H:%M")
			
			os.remove('last_post_id.json')
			with open('last_post_id.json','w') as f:
				json.dump(data,f,indent = 4)

		if response.url.find('trang') == -1:	# First page
			self.next_url = response.url.replace(".html","/trang--")
		else:
			self.next_url = response.url.rpartition("--")[0] + '--'

		for item in items:
			item_url = "http://alonhadat.com.vn" + item.extract()
			yield scrapy.Request(item_url,callback=self.parse_item)

		# Go to next page
		next_pages = response.xpath("//div[@class='page']/a[@rel='nofollow']/@href").extract()
		if len(next_pages) == 1:	# Ony one 'nofollow'
			next_pages = response.xpath("//div[@class='page']/a[@rel='nofollow']/@href").extract_first()
		else:	#  All pages are 'nofollow'
			next_pages = response.xpath("//div[@class='page']/a[@rel='nofollow']/@href")[len(next_pages)-1].extract()
		next_pages_index = int(next_pages.partition('--')[-1].rpartition('.html')[0])
		current_index = int(response.xpath("//a[@class='active']/text()").extract_first())
		if current_index < next_pages_index:
			current_index = current_index + 1
			next_page_url = self.next_url + str(current_index) + ".html"
			print("Next Page Url: " + next_page_url)
			yield scrapy.Request(next_page_url,callback=self.parse)
		elif current_index == next_pages_index:
			next_page_url = self.next_url + str(next_pages_index) + ".html"
			print("Next Page Url: " + next_page_url)
			yield scrapy.Request(next_page_url,callback=self.parse)


	def parse_item(self, response):
		# Get area
		self.area = response.xpath("//span[@class='square']/span[@class='value']/text()").extract_first()
		self.area = self.area.strip().replace(" m","")
		self.area = self.area.strip().replace(".","")


		# Get price
		price = response.xpath("//span[@class='price']/span[@class='value']/text()").extract_first()
		price = self.convert_unicode(price)
		if price.find("thuan") !=-1:
			return
		price = self.convert_price(price)

		# Get property type
		house_type = self.convert_unicode(response.xpath(u"//td[contains(text(),'Loại BDS')]/following-sibling::td").extract_first())
		house_type = house_type.replace("<td>","")
		house_type = house_type.replace("</td>","")

		# Get project
		project = response.xpath("//span[@class='project']//text()").extract_first()
		if project == None:
			project = ""
		else:
			project = self.convert_unicode(project)
			project = project.replace("<td>","")
			project = project.replace("</td>","")

		# Get transaction_type
		transaction_type = self.convert_unicode(response.xpath(u"//td[contains(text(),'Loại tin')]/following-sibling::td").extract_first())
		transaction_type = transaction_type.replace("<td>","")
		transaction_type = transaction_type.replace("</td>","")
		if transaction_type == "---":
			transaction_type = ""

		#Get bedcount
		bedcount = response.xpath(u"//td[contains(text(),'Số phòng ngủ')]/following-sibling::td").extract_first()
		bedcount = bedcount.replace("<td>","")
		bedcount = bedcount.replace("</td>","")
		if bedcount == "---":
			bedcount = ""

		# Get post id
		post_id = response.xpath(u"//td[contains(text(),'Mã tin')]/following-sibling::td").extract_first()
		post_id = post_id.replace("<td>","")
		post_id = post_id.replace("</td>","")

		# Get post time
		post_date = response.xpath("//span[@class='date']/text()").extract_first()
		post_date = self.convert_unicode(post_date).replace('Ngay dang: ', '').replace("/","-")
		if post_date =="Hom nay":
			post_date=datetime.datetime.now()
		elif post_date == "Hom qua":
			post_date=datetime.datetime.now() - datetime.timedelta(1)
		else:
			post_date=datetime.datetime.strptime(post_date,"%d-%m-%Y") #
		weekday = post_date.weekday()
		if post_date<self.last_post_time:
			print(post_date.strftime("%d-%m-%Y"),self.last_post_time.strftime("%d-%m-%Y"),response.url)
			return
	
		# Get title
		title = self.convert_unicode(response.xpath('//div[@class="title"]/h1/text()').extract_first())

		# Get description
		description = response.xpath("//div[contains(@class,'detail')]//text()").extract()
		description = '-'.join(description)
		description = self.convert_unicode(description)

		# Get location
		redundant_word = response.xpath("//span[@itemprop='name']/text()")[2].extract()
		list = response.xpath("//span[@itemprop='name']")
		if len(list) > 6:
			road = response.xpath("//span[@itemprop='name']/text()")[6].extract()
			road = self.convert_unicode(road.replace(redundant_word, ""))
		else:
			road = ""

		ward = response.xpath("//span[@itemprop='name']/text()")[5].extract()
		ward = self.convert_unicode(ward.replace(redundant_word, ""))

		county = response.xpath("//span[@itemprop='name']/text()")[4].extract()
		county = self.convert_unicode(county.replace(redundant_word, ""))

		province = response.xpath("//span[@itemprop='name']/text()")[3].extract()
		province = self.convert_unicode(province.replace(redundant_word, ""))

		road = road.strip()
		ward = ward.strip()
		county = county.strip()
		province = province.strip()

		location_detail = road + ", " + ward + ", " + county + ", " + province

		# Get author name
		author = response.xpath("//span[@class='name']/span[@class='value']/text()").extract_first()
		if author == None:
			author = ""

		yield {
			'post-id': post_id,
			'website': 'alonhadat.com.vn',
			'author': author,
			'post-time': {'date': post_date.strftime("%d-%m-%Y"),'weekday': weekday},
			'title': title,
			'location': {'province': province, 'county': county, 'road':road, 'ward': ward, 'detailed': location_detail},
			'area':self.area,
			'price':price,
			'transaction-type': transaction_type,
			'house-type': house_type,
			'description': description,
			'project': project,
			'bedcount': bedcount
		}





