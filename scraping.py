import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import urllib3
from datetime import datetime
urllib3.disable_warnings()

def request_content(url):
    res = requests.get(url,verify=False,headers={"content-type":"application/json"})
    return res.content
def get_type(source):
    type_ = json.load(open("type.config.json","r"))
    return type_[source]
def json_data(type_,title,subtitle,body,source,date,tag,news_url):
    ob_data = {"source": source,
               "url": str(news_url),
                "type": str(type_).strip(),
                "title": str(title).strip(),
                "subtitle": str(subtitle).strip(),
                "body": str(body).strip(),
                "date": date,
                "tag": str(tag).strip()}
    return ob_data
def convert_date(text):
    month_name = 'x มกราคม กุมภาพันธ์ มีนาคม เมษายน พฤษภาคม มิถุนายน กรกฎาคม สิงหาคม กันยายน ตุลาคม พฤศจิกายน ธันวาคม'.split()
    date = text.split(" ")
    m = th_month(date[1])
    d = date[0]
    y = int(date[2])-543
    return '{}-{}-{}'.format(y,m,d)
def th_month(month):
    try:
        month_name = 'x มกราคม กุมภาพันธ์ มีนาคม เมษายน พฤษภาคม มิถุนายน กรกฎาคม สิงหาคม กันยายน ตุลาคม พฤศจิกายน ธันวาคม'.split()
        return month_name.index(month)
    except:
        month_name = 'x ม.ค. ก.พ. มี.ค. เม.ย. พ.ค. มิ.ย. ก.ค. ส.ค. ก.ย. ต.ค. พ.ย. ธ.ค.'.split()
        return month_name.index(month)

class Hoonsmart:
    def __init__(self,fromPage=1,toPage=2):
        self.source = "hoonsmart"
        self.pageUrls,self.newsUrls = self.hoon_get_page_url(fromPage,toPage)
        self.contents = self.load_news()
        self.contents['date'] = self.contents['date'].astype('datetime64')
    def hoon_get_page_url(self,fromPage,toPage):
        pageUrls = []
        newsUrls = []
        for type_ in get_type(self.source):
            for i in range(fromPage,toPage):
                url_page = "https://www.hoonsmart.com/archives/category/"+ type_ +"/page/{}".format(i)
                newsUrls += self.hoon_get_news_url(url_page)
                pageUrls.append(url_page)
        return pageUrls,newsUrls
    def hoon_get_news_url(self,url):
        html = request_content(url)
        soup  = BeautifulSoup(html, "html.parser")
        data = []
        for tag in soup.find_all("h2", class_=('entry-title')):
            try:
                link = tag.a.get('href')
                data.append(link)
            except Exception as e:
                print(e)
        return data
    def load_content(self,news_url):
        html = request_content(news_url)
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("h2", class_=('main-title')).get_text()
        contents = []
        entry = soup.find("article")
        for x in soup.find_all("div", class_ = ("entry-content")):
            for xx in x.find_all("p"):
                contents.append(xx.get_text())
        sub = soup.find("span", class_ = ("cat-links"))

        tag = soup.find("span", class_ =("tags-links"))
        tag = tag.a.get_text()

        date = soup.find("time")
        date = date.get('datetime')
        type_ = sub.get_text().replace("Breaking News,","").lstrip()
        subtitle = contents[0].replace("HoonSmart.com>>","")
        del contents[0]
        body = "\n".join(contents)
        return json_data(type_, title, subtitle, body, self.source, date,tag,news_url)
    def urls_to_json(self,path):
        df_url = pd.DataFrame(self.newsUrls)
        df_url.to_json(path)
    def load_news(self):
        df_url = pd.DataFrame(self.newsUrls,columns=['url'])
        contents = df_url['url'].apply(lambda x: pd.Series(self.load_content(x)))
        return contents
    def get_contents(self,date):
        return self.contents[self.contents['date'] >= date]


class Bangkokbiz:
    def __init__(self, fromPage=1, toPage=2):
        self.source = "bangkokbiz"
        self.pageUrls, self.newsUrls = self.get_page_url(fromPage, toPage)
        self.contents = self.load_news()
        self.contents['date'] = self.contents['date'].astype('datetime64')

    def get_page_url(self, fromPage, toPage):
        pageUrls = []
        newsUrls = []
        for type_ in get_type(self.source):
            for i in range(fromPage, toPage):
                url_page = "http://www.bangkokbiznews.com/" + type_ + "/list/{}".format(i)
                newsUrls += self.get_news_url(url_page)
                pageUrls.append(url_page)
        return pageUrls, newsUrls

    def get_news_url(self, url):
        html = request_content(url)
        soup = BeautifulSoup(html, "html.parser")
        data = []
        for tag in soup.find_all("h3", class_=('post_title')):
            try:
                link = tag.a.get('href')
                data.append(link)
            except Exception as e:
                print(e)
        return data

    def load_content(self, news_url):
        html = request_content(news_url)
        soup = BeautifulSoup(html, "html.parser")

        text = []

        title = soup.find("div", class_=('section_6')).h1.get_text()
        for x in soup.find_all("div", class_=("col-sm-10 col-xs-5")):
            for xx in x.find_all("span"):
                type_ = xx.get_text()
        for x in soup.find_all("div", class_=("text-read")):
            for xx in x.find_all("p"):
                if ('' != xx.get_text()) and ('ไม่พลาดข่าวสำคัญ' not in xx.get_text()):
                    text.append(xx.get_text())
        date = soup.find("div", class_="event_date").get_text().replace("\n", "").strip()
        date = convert_date(date)
        subtitle = text[0]
        del text[0]
        body = "\n".join(text)
        return json_data(type_, title, subtitle, body, self.source, date, None, news_url)

    def load_news(self):
        df_url = pd.DataFrame(self.newsUrls, columns=['url'])
        contents = df_url['url'].apply(lambda x: pd.Series(self.load_content(x)))
        return contents

    def get_contents(self, date):
        return self.contents[self.contents['date'] >= date]

class SETNews:
    def __init__(self,today=True,company='',fromDate='',toDate=''):
        self.__set_url(today,company,fromDate,toDate)
        self.__content = request_content(self.__url)
        self.__get_page()
        self.newsUrls = self.fetch_url_from_web(self.__urls)
        self.contents = self.load_news()
    def __set_url(self,today,company=None,fromDate=None,toDate=None):
        if today == True:
            self.__url = "https://www.set.or.th/set/todaynews.do?period=all&language=th&country=TH"
        else:
            self.__url = "https://www.set.or.th/set/newslist.do?to={2}" \
                     "&headline=&submit=%E0%B8%84%E0%B9%89%E0%B8%99%E0%B8%AB%E0%B8%B2" \
                     "&symbol={0}&source=&newsGroupId=&securityType=" \
                     "&currentpage=0&language=th" \
                     "&from={1}&country=TH".format(company,fromDate,toDate)

    def __get_url(self,page_number):
        url = self.__url.replace('currentpage=0','currentpage={}'.format(page_number))
        return url

    def __get_page(self):
        pages = []
        soup = BeautifulSoup(self.__content, "html.parser")
        pages.append(self.__url)
        for tag in soup.find_all("nav",class_="pagination-set text-center"):
            i = 1
            for link in tag.find_all("li"):
                pages.append(self.__get_url(i))
                i+=1
        self.__urls = pages

    @property
    def html(self):
        return self.__content
    def count(self):
        return len(self.__urls)
    def url(self):
        return self.__url
    def all_urls(self):
        return self.__urls
    def news_urls(self):
        return self.news_urls

    def fetch_url_from_web(self,urls):
        news_url = []
        for url in urls:
            html = request_content(url)
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup.find_all("tbody"):
                for row in tag.find_all("td"):
                    if (row.a and row.a.get_text()=='รายละเอียด'):
                        news_url.append("https://www.set.or.th"+row.a.get('href'))
        return news_url

    def fetch_info_web(self,url):
        info = {'date':None,
                'title':None,
                'company':None,
                'source':None,
                'content':None}

        html = request_content(url)
        soup = BeautifulSoup(html, "html.parser")
        for div in soup.find_all("div",class_="row col-md-offset-3 col-md-7"):
            for row in div.find_all("div",class_='row'):
                rows = row.find_all('div')
                header = rows[0].get_text().strip()
                data = rows[1].get_text().strip()
                if 'วันที่' in header:
                    info['date'] = convert_date(data)
                elif 'หัวข้อข่าว' in header:
                    info['title'] = data
                elif 'หลักทรัพย์' in header:
                    info['company'] = data
                elif 'แหล่งข่าว' in header:
                    info['source'] = data
        body = soup.find('div',class_="col-md-offset-3 col-md-7")
        if body.pre:
            info['content'] = body.pre.get_text()
        return json_data(None,info['title'],None,info['content'],info['source'],info['date'],info['company'],url)
    def load_news(self):
        df_url = pd.DataFrame(self.newsUrls,columns=['url'])
        contents = df_url['url'].apply(lambda x: pd.Series(self.fetch_info_web(x)))
        return contents


class RYT9:
    def __init__(self, date=''):
        self.source = "ryt9"
        self.pageUrls, self.newsUrls = self.get_page_url(date)
        self.contents = self.load_news()

    def get_page_url(self, date):
        pageUrls = []
        newsUrls = []

        url_page = "https://www.ryt9.com/stock-latest/" + date
        date_check = True

        while (date_check):
            news_url, next_page = self.get_news_url(url_page)
            newsUrls += news_url
            pageUrls.append(url_page)
            url_page = next_page
            if date not in url_page:
                date_check = False
        return pageUrls, newsUrls

    def get_news_url(self, url):
        html = request_content(url)
        soup = BeautifulSoup(html, "html.parser")
        data = []
        for tag in soup.find_all("h3"):
            try:
                link = tag.a.get('href')
                data.append(link)
            except Exception as e:
                print(e)
        next_url = "https://www.ryt9.com" + soup.find("div", class_="more-news").a.get('href')
        return data, next_url

    def fetch_info_web(self, url):
        info = {}

        html = request_content(url)
        soup = BeautifulSoup(html, "html.parser")

        info['title'] = soup.find("div", id="main-story").h2.get_text()

        body_tag = soup.find("div", id="story-body").findChildren()
        temp = []
        for tag in body_tag:
            if ("div" == tag.name) or ("p" == tag.name):
                if ("ดูรูปทั้งหมด" not in tag.get_text()) and (tag.get_text() != ""):
                    temp.append(tag.get_text())
        detail = temp[0].split("--")
        info['type'] = detail[0].strip().split(" ")[0]
        info['source'] = " ".join(detail[0].strip().split(" ")[1:])
        info['date'] = convert_date(" ".join(detail[1].strip().split(" ")[1:4]))
        del temp[0]
        info['body'] = "\n".join(temp)
        return json_data(info['type'], info['title'], None, info['body'], info['source'], info['date'], None, url)

    def load_news(self):
        df_url = pd.DataFrame(self.newsUrls, columns=['url'])
        contents = df_url['url'].apply(lambda x: pd.Series(self.fetch_info_web(x)))
        return contents

def news_daily():
    date_now = datetime.now()
    ryt9 = RYT9(date_now.strftime('%Y-%m-%d'))
    hoon = Hoonsmart(1, 4)
    hoon_df = hoon.contents[hoon.contents['date'].dt.day == date_now.day]

    bknew = Bangkokbiz(1, 5)
    bknew_df = bknew.contents[bknew.contents['date'].dt.day == date_now.day]

    set_news = SETNews()

    news_all = pd.concat([ryt9.contents, hoon_df, bknew_df, set_news.contents], axis=0, ignore_index=True)
    return news_all.to_json(orient='index')
# bknew = Bangkokbiz(1, 2)