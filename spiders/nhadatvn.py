import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector

class NhadatVnSpider(scrapy.Spider):
	name = "Nhadatvn"
	last_post_time=''
	is_last_sell=''
	download_delay=2
	is_last_rent=''
	is_updated=''
	def start_requests(self):
		self.is_last_rent=False
		self.is_last_sell=False
		self.is_updated=False
		urls = [
		'https://raovat.nhadat.vn/cho-thue-4/?prefixid=0&prefixid2=0&direction=0&price=0',
		'https://raovat.nhadat.vn/can-ban-12/?prefixid=0&prefixid2=0&direction=0&price=0'
		]
		for url in urls:
			yield scrapy.Request(url=url,callback=self.parse)
	

	def convert_unicode(self,text):
		if text=='':
			return text
		text=re.sub(unichr(272),'D',text);
		text=re.sub(unichr(273),'d',text);
		text=unicodedata.normalize('NFKD', text).encode('ascii','ignore')
		text=text.decode()
		text=text.replace('\n','')
		text=text.replace('\t','')
		text=text.replace('\r','')
		return text

	def convert_price(self,text,area):
		if(bool(re.search(r'\d',text))==False):
			return text
		else :
			#get the base number, and the unit (Ngan, Trieu, Ty)
			price_s=text.split(' ')
			real_price=0
			i=0
			while(price_s[i]==''):
				i+=1
			base=float(re.sub('\D','.',price_s[i]))
			unit=price_s[i+1]
			if unit[0]=='N' or unit[0]=='n':
				real_price+=(base)*1000
			elif unit[0]=='T' or unit[0]=='t':
				if unit[1]=='r':
					real_price+=(base)*1000000
				else:
					real_price+=(base)*1000000000
				if 'm2' in text:
					a=float(area)

					real_price*=a
			return real_price

	def parse_item(self,response):
		#get basic info of property
		property_info = response.xpath(".//div[contains(@class,'uifleft')]/dl/dd/text()").extract()
		#get author info
		author_info = response.xpath(".//div[contains(@class,'uifright')]/dl/dd/text()").extract()
		#get description
		item_content = self.convert_unicode(re.sub("\r|\n|\t",""," ".join(response.xpath(".//blockquote/text()").extract())))

		#get title
		title=self.convert_unicode(response.xpath(".//span[contains(@class,'threadtitle')]/a/text()").extract_first())

		breadcrumb=response.xpath(".//span[contains(@class,'crust')]")
		#Rent or sell
		transaction_type = self.convert_unicode(breadcrumb[1].xpath("./a/span/text()").extract_first())
		#Basic house type
		general_house_type = self.convert_unicode(breadcrumb[2].xpath("./a/span/text()").extract_first())
		#Detailed house type
		housetype=self.convert_unicode(breadcrumb[3].xpath("./a/span/text()").extract_first())

		date=self.convert_unicode(response.xpath(".//span[contains(@class,'postdate')]/span/text()").extract_first()).strip(" ")
		if date =="Hom nay":
			date=datetime.datetime.now()
		elif date == "Hom qua":
			date=datetime.datetime.now() - datetime.timedelta(1)
		else:
			date=datetime.datetime.strptime(date,"%d-%m-%Y")	# datetime.datetime(2017, 2, 21, 0, 0)

		weekday=date.weekday()
		if date<self.last_post_time:
			print(date.strftime("%d-%m-%Y"),self.last_post_time.strftime("%d-%m-%Y"),response.url)
			if transaction_type=='Cho Thue':
				self.is_last_rent=True

			else:
				self.is_last_sell=True
			return

		county = self.convert_unicode(property_info[1])
		province = self.convert_unicode(property_info[0])
		if re.search("HCM",province)!=None:
			province="HCM"
		area=""
		price=None
		if len(property_info)>3:
			area = property_info[2]
			price=self.convert_unicode(property_info[3])
			area=area.split(" ")[0]
		elif len(property_info)==3:
			area= None
			price= self.convert_unicode(property_info[2])
			
		price=self.convert_price(price,area)

		post_id = author_info[0]
		author=response.xpath(".//div[contains(@class,'uifright')]/dl/dd")[1].xpath("./div/a/@title").extract_first()
		author=author.split(" ")[0]
		phone=None

		if len(author_info)==2:
			phone=author_info[1]

		yield {
			'post-id': post_id,
			'website': "nhadat.vn",
			'author': author,
			'post-time': {'date': date.strftime("%d-%m-%Y"),'weekday': weekday},
			'title': title,
			'location': {'county': county,'province': province,'location-detail':''},
			'area':area,
			'price':price,
			'transaction-type': transaction_type,
			'house-type': {'general':general_house_type,'detailed':housetype},
			'description': item_content
		}

	def parse(self, response):
		# Get all item 
		items=response.xpath("//li[contains(@class,'threadbit') and not(contains(@class,'hot'))]")
		print(response.url)
		# We use is_updated here because we want the first page to be run only one time for both 2 links
		if response.url.find('index')==-1 and self.is_updated==False: #first page
			self.is_updated=True
			with open('last_post_id.json','r+') as f:
				data=json.load(f)
				if "Nhadatvn" in data:
					self.last_post_time=datetime.datetime.strptime(data["Nhadatvn"],"%d-%m-%Y %H:%M")
					data["Nhadatvn"]=(datetime.datetime.now()-datetime.timedelta(minutes=4)).strftime("%d-%m-%Y %H:%M")
			
			os.remove('last_post_id.json')
			with open('last_post_id.json','w') as f:
				json.dump(data,f,indent = 4)
		
		for item in items:
			if(re.search('cho-thue',response.url)!=None):
				if self.is_last_rent==True:
					return
			else:
				if self.is_last_sell==True:
					return
			item_id = item.xpath(".//a[contains(@class,'title')]/@id").extract_first().split('_')[2]
			item_url = item.xpath(".//h2[contains(@class,'threadtitle')]/a/@href").extract_first()
			yield scrapy.Request(item_url,callback=self.parse_item)

		#if there is no expired item, go to next page, if there is a next page
		next_href=response.xpath("//a[contains(@rel,'next')]/@href")
		next_href_address=response.xpath("//a[contains(@rel,'next')]/@href").extract_first()
		if next_href!=[]:
			yield scrapy.Request(next_href_address,callback=self.parse)
