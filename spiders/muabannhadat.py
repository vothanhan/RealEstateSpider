import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector

class MuabannhadatSpider(scrapy.Spider):
	name = "muabannhadat"
	download_delay = 1.5
	last_post_time=''
	is_updated = ''
	transaction_type = ''

	def start_requests(self):
		self.is_updated=False
		urls = [
		'http://www.muabannhadat.vn/nha-dat-3490'
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
		price = self.convert_unicode(price)
		price_number = price.split(" ")[0]
		price_unit = price.split(" ")[1]
		price_number = re.sub(",",".",price_number)
		if price_unit[1] == "r":
			return  (float(price_number)*1000000)
		elif price_unit[1] == "y":
			return (float(price_number)*1000000000)

	def parse(self, response):  
		# Get all items from the page
		items = response.xpath("//a[contains(@class,'title-filter-link')]/@href")

		# We use is_updated here because we want the first page to be run only one time for both 2 links
		if (response.url.find("p=")==-1) and self.is_updated==False: # Process the first page
			print ('Process first page')
			self.is_updated = True
			with open('last_post_id.json', 'r+') as f:
				data = json.load(f)
				if "muabannhadat" in data:
					self.last_post_time=datetime.datetime.strptime(data["muabannhadat"],"%d-%m-%Y %H:%M")
					data["muabannhadat"]=(datetime.datetime.now()-datetime.timedelta(minutes=4)).strftime("%d-%m-%Y %H:%M")		
			
			os.remove('last_post_id.json')
			with open('last_post_id.json', 'w') as f:
				json.dump(data, f, indent = 4)

		# Read every post one by one
		for item in items:
			if (re.search('cho-thue', response.url) != None):
				self.transaction_type = 'cho thue'
			
			else:
				self.transaction_type = 'can ban'
				
			item_url = "http://www.muabannhadat.vn" + item.extract()
			yield scrapy.Request(item_url,callback=self.parse_item)

		# Go to the next page
		next_page = response.xpath("//a[contains(@id,'_lnkNext')]/@href")
		if next_page != []:
			next_page = response.xpath("//a[contains(@id,'_lnkNext')]/@href").extract_first()
			next_page_address = "http://www.muabannhadat.vn" + next_page
			print ('Next Page URL: ' + next_page_address)
			yield scrapy.Request(next_page_address,callback=self.parse)

	def parse_item(self, response):

		# Get description of the property
		desciption = self.convert_unicode(re.sub("\r|\n|\t",""," ".join(response.xpath(".//div[contains(@id, 'Description')]/text()").extract()))).strip(" ")

		# Get the price
		price = response.xpath('//span[contains(@class,"price")]/text()').extract_first()
		if price == None:
			price = ""
		price = self.convert_price(price)

		# Get the area
		area = response.xpath("//span[contains(@id,'_lblSurface')]/text()").extract_first()
		if area == None:
			area = ""
		area = self.convert_unicode(area)
		area = area.replace(" m2","")
		
		# Get the post id
		post_id = response.xpath("//span[contains(@id,'_lblId')]/text()").extract_first()

		# Get the post title
		title = response.xpath("//h1[contains(@class, 'navi-title')]//text()").extract_first()
		title = self.convert_unicode(title)
		title = title.strip()

		# Get date post
		date_post =  self.convert_unicode(response.xpath("//span[contains(@id,'DateCreated')]/text()").extract_first()).replace(".","-")

		date_post = datetime.datetime.strptime(date_post,"%d-%m-%Y")
		weekday = date_post.weekday()
		if date_post < self.last_post_time:
			return

		# Get province 
		province = self.convert_unicode(response.xpath("//span[contains(@id,'City')]/a/text()").extract_first())
		
		# Get county 
		county = self.convert_unicode(response.xpath("//span[contains(@id,'District')]/a/text()").extract_first())

		# Get ward
		ward = self.convert_unicode(response.xpath("//span[contains(@id,'Ward')]/a/text()").extract_first())

		# Get road
		road = self.convert_unicode(response.xpath("//span[contains(@id,'Street')]/text()").extract_first())

		# Get location detail
		location_detail = road + "," + ward + "," + county + "," + province
		
		# Get road
		road = self.convert_unicode(response.xpath("//span[contains(@id,'Street')]/text()").extract_first())

		# Get author name
		contact_name = response.xpath("//div[contains(@class,'name-contact')]/span")
		if contact_name == []:
			contact_name = response.xpath("//div[contains(@class,'name-contact')]/a")
		contact_name = contact_name.xpath("text()").extract_first()
		if contact_name == None:
			author = ""
		else:
			author = self.convert_unicode(contact_name)

		# Get house-type
		type_list = response.xpath("//p[contains(@itemprop,'title')]")
		house_type = type_list[1].xpath("text()").extract_first()
		house_type =self.convert_unicode(house_type)
		house_type = house_type.lower()


		if house_type == "Khac":
			sub_url = re.search('http://www.muabannhadat.vn/(.*)(-khac-)',url)
			house_type = sub_url.group(1)
		house_type = house_type.replace("-"," ")

		# Get bedcount
		bedcount = response.xpath("//span[contains(@id,'BedRoom')]/text()").extract_first()
		if bedcount == None:
			bedcount = ""

		# Get project
		project = response.xpath("//span[contains(@id,'Project')]/text()").extract_first()
		if project == None:
			project = ""
		else:
			project = self.convert_unicode(project)

		yield {
			'post-id': post_id,
			'website': "muabannhadat.vn",
			'author': author,
			'post-time': {'date': date_post.strftime("%d-%m-%Y"),'weekday': weekday},
			'title': title,
			'location': {'province': province,'county': county, 'ward':ward, 'road':road, 'detailed': location_detail},
			'area':area,
			'price':price,
			'transaction-type': self.transaction_type,
			'description': desciption,
			'house-type': house_type,
			'project': project,
			'bedcount': bedcount

		}
