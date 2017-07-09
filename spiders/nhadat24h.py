import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector
import cfscrape

class Nhadat24hSpider(scrapy.Spider):
	name="nhadat24h"
	last_post_time=''
	is_updated=''


	def start_requests(self):
		self.is_updated=False
		urls=[
			"http://nhadat24h.net/ban-bat-dong-san-viet-nam-nha-dat-viet-nam-s686599/",
			"http://nhadat24h.net/cho-thue-nha-dat-bat-dong-san-tai-viet-nam-nha-dat-tai-viet-nam-s686588/"
		]
		token, agent = cfscrape.get_tokens("http://nhadat24h.net")
		self.token=token
		self.agent=agent
		for url in urls:
			yield scrapy.Request(url=url,callback=self.parse,
				cookies=token,
				headers={'User-Agent':agent})

	def convert_unicode(self,text):
		if text=='':
			return text
		text=re.sub(chr(272),'D',text);
		text=re.sub(chr(273),'d',text);
		text=unicodedata.normalize('NFKD', text).encode('ascii','ignore')
		text=text.decode()
		text=text.replace('\n','')
		text=text.replace('\t','')
		text=text.replace('\r','')
		text=text.strip()
		return text

	def convert_time(self,text):
		post_date = None
		if re.search("luc",text) == None:
			post_date = datetime.datetime.now()
		else:
			post_date = datetime.datetime.strptime(text.split(" ")[0],"%d/%m/%Y,")
		return post_date

	def is_number(self,text):
		try:
			float(text)
			return True
		except ValueError:
			return False

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
			try:
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
					a=float(area.split(' ')[0])

					real_price*=a
				return real_price
			except ValueError:
				return 'Thoa thuan'

	def parse_item(self,response):
		title = self.convert_unicode(response.xpath(".//div[contains(@class,'dv-ct-detail')]/h1/a/text()").extract_first())
		post_id = response.url.split("-")[len(response.url.split("-"))-1]
		property_details = response.xpath(".//div[contains(@class,'dv-tb-tsbds')]/table/tbody/tr")
		area = self.convert_unicode(property_details[1].xpath(".//td/label/strong/text()").extract_first())
		area=area.replace(',','.')

		post_time = self.convert_unicode(response.xpath(".//div[contains(@class,'dv-time-dt')]/p/label")[0].xpath(".//text()").extract_first())
		post_date = None
		if re.search("luc",post_time) == None:
			post_date = datetime.datetime.now()
		else:
			post_date = datetime.datetime.strptime(post_time.split(" ")[0],"%d/%m/%Y,")
		author = response.xpath(".//div[contains(@class,'dv-cont-dt')]/div/label/a/@href").extract_first().strip(".html").strip("/")
		county= self.convert_unicode(property_details[6].xpath(".//td/label/a/text()").extract_first())
		province= self.convert_unicode(property_details[3].xpath(".//td/label/text()").extract_first())
		location_detail=property_details[7].xpath(".//td/label/h2/a/text()").extract_first()
		if location_detail != None:
			location_detail = self.convert_unicode(location_detail) + ", " + county + ", " + province
		else:
			location_detail=county + ', ' + province
		if re.search("HCM",province)!=None:
			province="HCM"
		price= self.convert_unicode(property_details[0].xpath(".//td/label/strong/text()").extract_first())
		price=price.replace(',','.')
		if self.is_number(price)==True:
			price=float(price)
			base = self.convert_unicode(property_details[0].xpath(".//td/label/text()").extract_first())
			if base[0]=="T":
				if base[1]=="r":
					price*=1000000
				else:
					price*=1000000000
			elif base[0]=='N':
				price*=1000
			price=str(int(price))
			
		
		transaction_type=self.convert_unicode(property_details[4].xpath(".//td/label/text()").extract_first())
		housetype=self.convert_unicode(response.xpath(".//div[contains(@class,'dv-breadcrumb')]/ul/li")[2].xpath(".//a/text()").extract_first())

		description=" ".join(response.xpath(".//div[contains(@class,'dv-txt-mt')]/div//text()").extract())
		description= self.convert_unicode(re.sub("\r|\n|\t","",description))
		yield {
			'post-id': post_id,
			'website': "nhadat24h.net",
			'author': author,
			'post-time': {'date': post_date.strftime("%d-%m-%Y"),'weekday': post_date.weekday()},
			'title': title,
			'location': {'county': county,'province': province,'ward': '','road': '','location-detail':''},
			'project' : '',
			'bed-count' : '',
			'area':area,
			'price':price,
			'transaction-type': transaction_type,
			'house-type': housetype,
			'description': description
		}

	def parse(self, response):

		is_last= False
		is_old=False
		items = response.xpath(".//div[contains(@class,'dv-item')]")
		if response.url.split("/")[len(response.url.split("/"))-1].isdigit() == False and self.is_updated==False and 'is_updated' not in self.state.keys(): #first page
			self.is_updated=True
			self.state['is_updated']=True
			with open('last_post_id.json','r+') as f:
				data=json.load(f)
				self.last_post_time=''
				if "nhadat24h" in data:
					self.last_post_time=datetime.datetime.strptime(data["nhadat24h"],"%d-%m-%Y %H:%M")
					self.state['last_post_time']=self.last_post_time
				data["nhadat24h"]=(datetime.datetime.now()-datetime.timedelta(minutes=15)).strftime("%d-%m-%Y %H:%M")
			os.remove('last_post_id.json')
			with open('last_post_id.json','w') as f:
				json.dump(data,f,indent = 4)

		if self.last_post_time=='':
			self.last_post_time=self.state['last_post_time']
		for item in items:
			post_url = item.xpath(".//div/h4/a/@href").extract_first()
			post_id=post_url.split("-")[len(post_url.split("-"))-1]
			

			post_time = self.convert_unicode(item.xpath(".//p")[0].xpath(".//text()").extract_first())
			post_date=self.convert_time(post_time)
			if re.search("vip",item.xpath("./@class").extract_first())==None:
				if post_date<self.last_post_time:
					is_last=True
					break
				if post_date.year<2012:
					is_old=True
					break
			yield scrapy.Request("http://nhadat24h.net" + post_url,callback= self.parse_item,cookies=self.token,headers={'User-Agent':self.agent})

		next_url=response.xpath(".//a[contains(@title,'Trang sau')]/@href").extract_first()
		if next_url != None and is_old == False and is_last==False:
			yield scrapy.Request("http://nhadat24h.net" + next_url, callback= self.parse,cookies=self.token,headers={'User-Agent':self.agent})
	def __repr__(self):
		"""only print out attr1 after exiting the Pipeline"""
		return repr({})
