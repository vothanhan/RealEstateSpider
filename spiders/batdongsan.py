import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from scrapy_splash import SplashRequest
import cfscrape


def is_number(text):
	try:
		float(text)
		return True
	except ValueError:
		return False


class BatdongsanSpider(scrapy.Spider):
	name="batdongsan"
	last_post_time=""
	download_delay=1.5
	is_updated=False
	download_delay=2
	baseUrl=''
	def start_requests(self):
		self.baseUrl="http://batdongsan.com.vn"
		self.state['baseUrl']=self.baseUrl
		self.is_updated=False
		urls = [
			"http://batdongsan.com.vn/nha-dat-cho-thue",
			"http://batdongsan.com.vn/nha-dat-ban"
		]
		for url in urls:
			yield scrapy.Request(url=url,callback=self.parse)

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
			if 'm2' in unit:
				a=area.split('m')
				if not is_number(a[0]):
					return text
				real_price*=float(a[0])
			return real_price
	def convert_unicode(self,text):
		if text=='':
			return text
		text=re.sub(chr(272),'D',text)
		text=re.sub(chr(273),'d',text)
		text=unicodedata.normalize('NFKD', text).encode('ascii','ignore')
		text=text.decode()
		text=text.replace('\n','')
		text=text.replace('\t','')
		text=text.replace('\r','')
		text=text.strip()
		return text

	def get_selected(self,item):
		if item==[]:
			return ''
		if int(item.xpath('./@vl').extract_first())<=0:
			return ''
		else:
			return self.convert_unicode(item.xpath('./text()').extract_first())

	def parse(self,response):
		print (response.request.headers)
		if 'baseUrl' in self.state.keys():
			self.baseUrl='http://batdongsan.com.vn'
		response=HtmlResponse(url=response.url,body=response.body)

		zones=response.xpath("//div[@id='divCountByAreas']")[0]
		zones_href=zones.xpath('./ul/li/h3/a/@href').extract()
		texts_zone=response.xpath("//div[@id='divCountByAreas']")[0]
		texts=texts_zone.xpath("./ul/li/h3/a/text()").extract()
		for index,href in enumerate(zones_href):
			yield scrapy.Request(url=self.baseUrl+href,meta={'province':texts[index]},callback=self.parse_province)

	def parse_province(self,response):
		meta=response.meta
		if 'baseUrl' in self.state.keys():
			self.baseUrl='http://batdongsan.com.vn'
		response=HtmlResponse(url=response.url,body=response.body)
		zones=response.xpath("//div[@id='divCountByAreas']")[0]
		zones_href=zones.xpath('./ul/li/h3/a/@href').extract()
		texts_zone=response.xpath("//div[@id='divCountByAreas']")[0]
		texts=texts_zone.xpath("./ul/li/h3/a/text()").extract()
		for index,href in enumerate(zones_href):
			print(self.convert_unicode(meta['province']),self.convert_unicode(texts[index]))
			yield scrapy.Request(url=self.baseUrl+href,meta={'county':texts[index],'province':meta['province']},callback=self.parse_list)


	def parse_list(self,response):
		if 'baseUrl' in self.state.keys():
			self.baseUrl='http://batdongsan.com.vn'
		meta=response.meta
		
		response=HtmlResponse(url=response.url,body=response.body)
		already_crawl=False

		url_length=len(response.url.split('/'))
		if (url_length==4 and self.is_updated==False and 'is_updated' not in self.state.keys()): #first page
			self.is_updated=True
			self.state['is_updated']=True
			with open('last_post_id.json','r+') as f:
				data=json.load(f)
				self.last_post_time=''
				self.last_post_time=datetime.datetime.strptime('01-01-2012 00:00',"%d-%m-%Y %H:%M")
				if "batdongsan" in data:
					self.last_post_time=datetime.datetime.strptime(data["batdongsan"],"%d-%m-%Y %H:%M")
					self.state['last_post_time']=self.last_post_time
				data["batdongsan"]=(datetime.datetime.now()-datetime.timedelta(minutes=15)).strftime("%d-%m-%Y %H:%M")
				print(self.last_post_time)
			os.remove('last_post_id.json')
			with open('last_post_id.json','w') as f:
				json.dump(data,f,indent = 4)
		pager=response.xpath("//div[@class='background-pager-controls']/div/a/div/text()").extract()
		is_last_page=False
		if pager!=[]:
			if pager[len(pager)-1].isdigit()==True:
				is_last_page=True
		else:
			is_last_page=True
		items=response.xpath("//div[contains(@class,'search-productItem')]")
		if self.last_post_time=='':
			self.last_post_time=self.state['last_post_time']
		for item in items:

			url=item.xpath("./div[@class='p-title']/h3/a/@href").extract_first()
			date_text=self.convert_unicode(item.xpath("./div[@class='p-main']/div[contains(@class,'p-bottom-crop')]/div[contains(@class,'floatright')]/text()").extract_first())
			post_date=datetime.datetime.strptime(date_text,"%d/%m/%Y")
			print(url)
			print("="*100)
			if post_date<self.last_post_time:
				already_crawl=True
			else:
				if len(url)>0:
					yield scrapy.Request(url=(self.baseUrl+url),meta={'province':meta['province'],'county':meta['county'],'post_date':post_date},callback=self.parseitem)
		if is_last_page==False and already_crawl==False:

			next_page_url=response.url.split("/")
			index=''
			if url_length==5:
				index=int(next_page_url[-1][1:])+1
				del next_page_url[-1]
			else:
				index=2
			page="p%d"%index
			next_page_url.append(page)
			next_page_url="/".join(next_page_url)
			yield scrapy.Request(url=next_page_url,meta={'province':meta['province'],'county':meta['county']},callback=self.parse_list)

	def parseitem(self,response):
		if 'baseUrl' in self.state.keys():
			self.baseUrl='http://batdongsan.com.vn'
		meta=response.meta
		response=HtmlResponse(url=response.url,body=response.body)

		# post_detail=response.xpath("//div[@class='pm-content-detail']/table/tr/td")[0]
		# post_detail_boxes=post_detail.xpath('./div/div[@class="left-detail"]/div')
		# i=1
		# if post_detail_boxes[0].xpath('./@id').extract_first()=="LeftMainContent__productDetail_project":
		# 	i=0
		# post_id=self.convert_unicode(post_detail_boxes[2-i].xpath("./div[@class='right']/text()").extract_first())

		# location_detail=self.convert_unicode(post_detail_boxes[1-i].xpath("./div[@class='right']/text()").extract_first())
		# post_date=self.convert_unicode(post_detail_boxes[4-i].xpath("./div[@class='right']/text()").extract_first())
		# post_date=post_date.replace(' ','')
		# post_date=datetime.datetime.strptime(post_date,"%d-%m-%Y")
		# author_box=response.xpath("//div[@class='pm-content-detail']/table/tr/td")[1]
		# author_detail=author_box.xpath("./div/div")
		# author=self.convert_unicode(author_detail[1].xpath("./div[@class='right']/text()").extract_first())

		item_details = response.xpath("//div[@class='table-detail']")

		location_detail=self.convert_unicode(item_details[0].xpath('./div[@class="row"]')[1].xpath('./div[@class="right"]/text()').extract_first())

		project=''

		author_index=0
		if len(item_details)==2:
				author_index=1
		else:
			author_index==2
			project=self.convert_unicode(item_details[1].xpath("./div[@class='row']/div[@class='right']/text()").extract_first())

		author_line=item_details[author_index].xpath("./div/div[@id='LeftMainContent__productDetail_contactName']/div[@class='right']")
		author=''
		if len(author_line)>0:
			author=self.convert_unicode(author_line[0].xpath('./text()').extract_first())
		post_id = response.url.split("/")[-1].split("-")[-1][2:]

		post_date = meta['post_date']

		county=self.convert_unicode(meta['county'])
		province=self.convert_unicode(meta['province'])
		ward=self.get_selected(response.xpath('//div[@id="divWardOptions"]/ul/li[contains(@class,"current")]'))
		road=self.get_selected(response.xpath('//div[@id="divStreetOptions"]/ul/li[contains(@class,"current")]'))

		bedcount=self.get_selected(response.xpath('//div[@id="divBedRoomOptions"]/ul/li[contains(@class,"current")]'))
		if bedcount!='':
			bedcount=int(bedcount[:-1])

		title=self.convert_unicode(response.xpath("//div[@class='pm-title']/h1/text()").extract_first())
		area_price_text=response.xpath("//span[contains(@class,'gia-title')]/strong/text()").extract()
		area=self.convert_unicode(area_price_text[1])
		if area=="Khong xac dinh":
			area=''
		price=self.convert_unicode(area_price_text[0])
		price=self.convert_price(price,area)
		housetype=""
		transaction_type=""
		house_type_text=self.convert_unicode(response.xpath("//span[contains(@class,'diadiem-title')]/a/text()").extract_first())
		lastpos=house_type_text.find('tai')-1
		if re.search("Cho thue",house_type_text)!=None:
			transaction_type='Cho thue'
			housetype=house_type_text[9:lastpos]
		else:
			transaction_type='Can ban'
			housetype=house_type_text[4:lastpos]

		description=self.convert_unicode(" ".join(response.xpath("//div[contains(@class,'pm-desc')]//text()").extract()))


		yield {
			'post-id': post_id,
			'website': "batdongsan.com.vn",
			'author': author,
			'post-time': {'date': post_date.strftime("%d-%m-%Y"),'weekday': post_date.weekday()},
			'title': title,
			'location': {'county': county, 'province': province, 'ward': ward, 'road': road,'location-detail':location_detail},
			'project': project,
			'bed-count': bedcount,
			'area':area,
			'price':price,
			'transaction-type': transaction_type,
			'house-type': housetype,
			'description': description
		}