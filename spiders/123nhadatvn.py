# -*- coding: utf-8 -*-
import scrapy
import re
import unicodedata
import datetime
import json
import os
from scrapy.selector import Selector
from scrapy_splash import SplashRequest


def is_number(text):
    try:
        float(text)
        return True
    except ValueError:
        return False

class Nhadat123Spider(scrapy.Spider):
    name = "123nhadat"
    last_post_time = ''
    cur_page_index = 2
    is_updated = ''
    def start_requests(self):
        self.is_updated = False
        urls = [
            "http://123nhadat.vn/raovat-c2/nha-dat-cho-thue",
            "http://123nhadat.vn/raovat-c1/nha-dat-ban"
        ]
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse)

    def convert_unicode(self,text):
        if text == '':
            return text
        elif text == None:
            return ''
        text = re.sub(chr(272), 'D', text)
        text = re.sub(chr(273), 'd', text)
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')
        text = text.decode()
        text = text.replace('\n', '')
        text = text.replace('\t', '')
        text = text.replace('\r', '')
        text = text.strip()
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
            
    def convert_time(self,text,item):
        post_time=None
        if re.search("/",text):
            post_date_text=self.convert_unicode(item.xpath("./div/div/div/ul/li/span/span/text()").extract_first().split(" ")[2])
            post_time=datetime.datetime.strptime(post_date_text,"%d/%m/%Y")
        elif re.search("phut|gio",text)!=None and re.search("ngay",text)==None:
            post_time=datetime.datetime.now()
        elif re.search("ngay",text)!=None:
            hours=0
            if re.search("gio",text)!=None:
                hours=int(self.convert_unicode(item.xpath("./div/div/div/ul/li/span/span/text()").extract_first().split(" ")[2]))
            days=int(self.convert_unicode(item.xpath("./div/div/div/ul/li/span/span/text()").extract_first().split(" ")[0]))
            post_time=datetime.datetime.now()-datetime.timedelta(days=days,hours=hours)
        elif re.search("moi cap nhat",text)!=None:
            post_time=datetime.datetime.now()
        return post_time
    def parse_item(self,response):
        title= self.convert_unicode(response.xpath(".//h1[@class='tieude_nhadat']/text()").extract_first())

        url_title= self.convert_unicode(response.xpath(".//ul[@class='info_no2']/li/span/a/text()").extract_first())
        house_type=""
        transaction_type=""
        house_type=self.convert_unicode(response.xpath('//select[@id="cboTypeReR"]/option[@selected]/text()').extract_first())
        if(re.search("Cho thue",url_title)!= None):
            house_type=house_type[9:]
            transaction_type="Cho thue"
        elif re.search("Ban",url_title)!= None:
            house_type=house_type[4:]
            transaction_type="Ban"
            

        details=response.xpath(".//div[@class='detail_khungxam']")
        description=self.convert_unicode(" ".join(details[0].xpath("./p//text()").extract()))

        post_id= response.url.split('/')[3].split('-')[1][1:]

        raw_last_update_time=details[1].xpath("./div")[2].xpath("./text()").extract_first().split(" ")
        last_update_time=self.convert_unicode(raw_last_update_time[len(raw_last_update_time)-2]+" "+raw_last_update_time[len(raw_last_update_time)-1])
        last_update_time=datetime.datetime.strptime(last_update_time,"%H:%M %d/%m/%Y")

        author=self.convert_unicode(response.xpath(".//div[@class='lienhe_nguoiban']/ul/li")[1].xpath("./b/text()").extract_first())

        location_detail= self.convert_unicode("".join(response.xpath(".//div[@class='detail_khungxam']")[1].xpath(".//div")[1].xpath(".//text()").extract()))
        location_detail=location_detail.split(':')[1].strip()

        if re.search('Thuoc du an',location_detail):
            location_detail=location_detail[:re.search('Thuoc du an',location_detail).start()]

        province=self.convert_unicode(response.xpath('//select[@id="cboCityR"]/option[@selected]/text()').extract_first())
        county=self.convert_unicode(response.xpath('//select[@id="cboDistrictR"]/option[@selected]/text()').extract_first())
        road=self.convert_unicode(response.xpath('//select[@id="cboStreetR"]/option[@selected]/text()').extract_first())
        ward=self.convert_unicode(response.xpath('//select[@id="cboWardR"]/option[@selected]/text()').extract_first())
        project=self.convert_unicode(response.xpath('//select[@id="cboProjR"]/option[@selected]/text()').extract_first())
        project=project if project!=None else ''
        bed_count=''
        bed_detail=response.xpath('//ul[@class="thongsonha"]/li[contains(text(),"phòng ngủ")]/text()').extract_first()

        #Process Bed
        if bed_detail!=None:
            bed_detail=bed_detail.split(' ')
            bed_count=int(bed_detail[0])

        info_no_1= response.xpath(".//ul[@class='info_no1']/li/span")
        area= self.convert_unicode(info_no_1[1].xpath("./b/text()").extract_first())
        raw_price= self.convert_unicode(info_no_1[0].xpath("./b/text()").extract_first())
        price=raw_price
        raw_price=raw_price.replace(',','.')
        if raw_price==None:
            price="Thoa thuan"
        else:
            if is_number(raw_price.split(" ")[0]):
                price=self.convert_price(raw_price,area)
            else:
                price=raw_price


        yield {
            'post-id': post_id,
            'website': "123nhadat.vn",
            'author': author,
            'post-time': {'date': last_update_time.strftime("%d-%m-%Y"),'weekday': last_update_time.weekday()},
            'title': title,
            'location': {'county': county,'province': province, 'ward': ward, 'road': road, 'location-detail':location_detail},
            'project': project,
            'bed-count': bed_count,
            'area':area,
            'price':price,
            'transaction-type': transaction_type,
            'house-type': house_type,
            'description': description
        }

    def parse(self,response):
        is_last= False
        print(response.url)
       
        items=response.xpath(".//div[contains(@class,'box_nhadatban') and not(contains(@class,'box_R'))]")
        if response.url.split("/")[len(response.url.split("/"))-1].isdigit() == False and self.is_updated==False and "last_post_time" not in self.state.keys():
            self.is_updated=True
            with open('last_post_id.json','r+') as f:
                data=json.load(f)
                self.last_post_time=''
                if "123nhadat" in data:
                    self.last_post_time=datetime.datetime.strptime(data["123nhadat"],"%d-%m-%Y %H:%M")
                    self.state['last_post_time']=self.last_post_time
                data["123nhadat"]=(datetime.datetime.now()-datetime.timedelta(minutes=5)).strftime("%d-%m-%Y %H:%M")
            os.remove('last_post_id.json')
            with open('last_post_id.json','w') as f:
                json.dump(data,f,indent = 4)
        if self.last_post_time=='':
            self.last_post_time=self.state["last_post_time"]
        for item in items:
            post_id=self.convert_unicode(item.xpath("./div/div/span/text()").extract_first().split(' ')[1])
            post_time_text=self.convert_unicode(item.xpath("./div/div/div/ul/li/span/span/text()").extract_first())
            post_time=self.convert_time(post_time_text,item)

            if re.search('vip',item.xpath("./@class").extract_first())==None:
                if post_time<=self.last_post_time:
                    is_last=True
                    break
            item_url=self.convert_unicode(item.xpath("./div/h4/a/@href").extract_first())
            yield SplashRequest(item_url,callback=self.parse_item)



        is_last_page=False
        paging=response.xpath(".//ul[contains(@class,'pagging')]/li/a/text()").extract()
        if paging[len(paging)-1]!='Sau':
            is_last_page=True
        if is_last == False and is_last_page== False:
            next_url=response.url.split('/')
            if len(response.url.split('/'))==5:
                index=2
                next_url.append(str(index)+'/-1/0/0')
            else:
                index=response.url.split('/')[5]
                next_url[5]=str(int(index)+1)
            
            next_url="/".join(next_url)
            yield scrapy.Request(next_url, callback= self.parse)

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({})