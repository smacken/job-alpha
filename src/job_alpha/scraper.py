from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import logging
import re
import sys
import time
import pandas as pd
import inspect, os
import urllib3
import sqlite3
from sqlalchemy.types import Integer
import datetime
from dateutil.relativedelta import relativedelta

module_logger = logging.getLogger('jobs.scraper')

class Scraper(object):

    headers = {
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip, deflate, sdch, br',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Accept-Language': 'en-US,en;q=0.8,pt;q=0.6',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }

    def inheritors(klass):
        subclasses = set()
        work = [klass]
        while work:
            parent = work.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    work.append(child)
        return subclasses

    def __init__(self, dbpath=''):
        self.logger = logging.getLogger('jobs.jobs_alpha.Scraper')
        self.logger.info('creating an instance of scraper')
        driver_path = r"D:\utils\chromedriver\chromedriver.exe"
        if not os.path.isfile(driver_path):
            driver_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), 'data', 'chromedriver.exe')
        self.driver = webdriver.Chrome(driver_path)
        self.driver.implicitly_wait(30)
        datadir = os.path.join(os.path.abspath(os.path.join(__file__, os.pardir)))
        db_path = os.path.abspath(os.path.join(datadir, 'jobs.db'))
        self.db = sqlite3.connect(db_path, timeout=10)

    def __del__(self):
        self.driver.close()
        self.db.close()

    def close_driver(self):
        self.driver.close()

    def get(self, url):
        self.driver.get(url)
        soup_level1 = BeautifulSoup(self.driver.page_source, 'lxml')
        return soup_level1

    def job_listings(self):
        for scrape_type in Scraper.inheritors(Scraper):
            scrape_type._job_listings(self)

    def job_details(self):
        for scrape_type in Scraper.inheritors(Scraper):
            scrape_type._job_details(self)

    def update_job_details(self, jid, jobid, provider):
        results = []
        for scrape_type in Scraper.inheritors(Scraper):
            results.append(scrape_type._update_job_details(jid, jobid, provider))
            
        details = next(item for item in results if item is not None)
        print(details, results)
        return details

class SeekScraper(Scraper):

    def get_url(self, *keywords):
        ''' get a seek jobsearch url from a list of keywords'''
        search = '-'.join([item.replace(' ', '-') for item in keywords])
        listings_url = 'https://www.seek.com.au/{}-jobs?sortmode=ListedDate'.format(search)
        return listings_url

    def search(self, keywords):
        self.driver.get('https://www.seek.com.au/')
        elem = self.driver.find_element_by_id("SearchBar__Keywords")
        elem.clear()
        elem.send_keys(keywords)
        elem.send_keys(Keys.RETURN)
        soup_level1 = BeautifulSoup(self.driver.page_source, 'lxml')
        return soup_level1

    def search_url(self, *keywords):
        ''' search seek jobs for a given set of keywords'''
        url = self.get_url(*keywords)
        self.driver.get(url)
        element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@data-automation="searchResults"]')))
        soup_level1 = BeautifulSoup(element.get_attribute('innerHTML'), "html.parser")
        return (soup_level1, element.get_attribute('innerHTML'))

    def next(self):
        ''' get the next page of job listings '''
        next_btn = self.driver.find_element_by_link_text('Next')
        if not next_btn:
            return None
        next_btn.click()
        element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@data-automation="searchResults"]')))
        soup = BeautifulSoup(element.get_attribute('innerHTML'), "html.parser") if element else None
        return soup

    @staticmethod
    def get_past_date(str_days_ago):
        TODAY = datetime.date.today()
        splitted = str_days_ago.split()
        if len(splitted) == 1 and splitted[0].lower() == 'today':
            return str(TODAY.isoformat())
        elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
            date = TODAY - relativedelta(days=1)
            return str(date.isoformat())
        elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
            date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
            return str(date.date().isoformat())
        elif splitted[1].lower() in ['day', 'days', 'd']:
            date = TODAY - relativedelta(days=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
            date = TODAY - relativedelta(weeks=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
            date = TODAY - relativedelta(months=int(splitted[0]))
            return str(date.isoformat())
        elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
            date = TODAY - relativedelta(years=int(splitted[0]))
            return str(date.isoformat())
        else:
            return "Wrong Argument format"

    def _job_listings(self, company):
        ''' get the jobs list for a given company '''

        def mklist(n):
            for _ in range(n):
                yield []   

        self.logger.info('get company jobs: %s' % (company,))
        jobids,titles,companies,locations,areas,category = mklist(6)
        subcategory,headers,short_descriptions,dates,salaries = mklist(5)
        search_page = self.search_url(company)[0]
        while search_page is not None:
            for i, article in enumerate(search_page.find_all('article')):
                jobids.append(None)
                titles.append(None)
                companies.append(None)
                locations.append(None)
                areas.append(None)
                category.append(None)
                subcategory.append(None)
                headers.append(None)
                short_descriptions.append(None)
                today = datetime.date.today()
                dates.append(today.strftime('%Y-%m-%d'))
                salaries.append(None)
                
                jobids.insert(i, article['data-job-id'])
                for title in article.select('h1 a'):
                    titles.insert(i, title.string)
                for item in article.find_all('a', attrs={'data-automation' : True}):
                    if item['data-automation'] == 'jobCompany':
                        companies.insert(i, item.string)
                    if item['data-automation'] == 'jobLocation':
                        locations.insert(i, item.string) 
                    if item['data-automation'] == 'jobArea':
                        areas.insert(i, item.string) 
                    if item['data-automation'] == 'jobClassification':
                        category.insert(i, item.string) 
                    if item['data-automation'] == 'jobSubClassification':
                        subcategory.insert(i, item.string)
                for header in article.find_all('ul'):
                    headers.insert(i, '. '.join([header.string for header in article.select('li span span')]))
                for item in article.find_all('span', attrs={'data-automation' : True}):
                    if item['data-automation'] == 'jobListingDate':
                        date = SeekScraper.get_past_date(' '.join(re.split(r'(\d+)', item.string.replace('ago', ''))))
                        dates.insert(i, date)
                    if item['data-automation'] == 'jobShortDescription':
                        short_descriptions.insert(i, item.string)
                    if item['data-automation'] == 'jobSalary':
                        salaries.insert(i, item.string)

            results = list(zip(jobids, dates, titles, companies, locations, areas, category, subcategory, salaries, headers, short_descriptions))
            df = pd.DataFrame(results, columns=['jobid', 'date', 'title', 'company', 'location', 'area', 'category', 'subcategory', 'salary', 'header', 'shortdescription'])
            df['ticker'] = company
            df['description'] = None
            df['provider'] = 1
            
            df.to_sql('jobs', con=self.db, if_exists='append', index=False) #, dtype={"jobid": Integer()})
            self.db.commit()
            search_page = None
            try:
                search_page = self.next()
            except:
                print('could not get the next page')
                # type, value, traceback = sys.exc_info()
                # print('Error opening: %s' % ( value))
                search_page = None

    def _job_details(jobid):
        ''' scrape the job details for a given jobid '''
        detail_url = 'https://www.seek.com.au/job/{}?type=standard'.format(jobid)
        http = urllib3.PoolManager()
        headers = {
            'Upgrade-Insecure-Requests': '1',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Accept-Language': 'en-US,en;q=0.8,pt;q=0.6',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = http.request('GET', detail_url, headers=headers)
        description = SoupStrainer("div", attrs={"data-automation": "jobDescription"})
        soup = BeautifulSoup(response.data, "html.parser", parse_only=description)
        return soup

    def _update_job_details(jid, jobid, provider):
        if provider is not 1:
            return None
        soup = SeekScraper._job_details(jobid)
        descr = ''
        for item in soup.find_all('div', attrs={'data-automation': True}, limit=1):
             if item['data-automation'] == 'jobDescription':
                 descr = item.get_text()
        job_details = re.sub(r'\s+', ' ', descr).strip().lower()
        if '<!--' in job_details:
            job_details = re.sub('<!--.*?-->','', job_details, flags=re.DOTALL).strip()
        return job_details

class IndeedScraper(Scraper):
    def get_url(self, *keywords):
        ''' get an indeed jobsearch url from a list of keywords'''
        search = '+'.join([item.replace(' ', '+') for item in keywords])
        return 'https://au.indeed.com/jobs?q={}&l='.format(search)

    def _update_job_details(jid, jobid, provider):
        if provider is not 2:
            return None
        soup = IndeedScraper._job_details(jobid)
        descr = ''
        descr = soup.find("div", attrs={"class": "jobsearch-JobComponent-description"}).get_text()
        job_details = re.sub(r'\s+', ' ', descr).strip().lower()
        if '<!--' in job_details:
            job_details = re.sub('<!--.*?-->','', job_details, flags=re.DOTALL).strip()
        return job_details

    def _job_details(jobid):
        detail_url = 'https://au.indeed.com/viewjob?jk={}'.format(jobid)
        http = urllib3.PoolManager()
        response = http.request('GET', detail_url, headers=Scraper.headers)
        description = SoupStrainer("div", attrs={"class": "jobsearch-JobComponent-description"})
        soup = BeautifulSoup(response.data, "html.parser")#, parse_only=description)
        return soup

    def next(self, page=1, *keywords):
        ''' get the next page of job listings '''
        time.sleep(4)
        http = urllib3.PoolManager()
        search = '+'.join([item.replace(' ', '') for item in keywords])
        url = 'https://au.indeed.com/jobs?q={}&start={}'.format(search, 10*page)
        print(url)
        response = http.request('GET', url, headers=self.headers)
        results = SoupStrainer("td", attrs={"id": "resultsCol"})
        soup = BeautifulSoup(response.data, "html.parser", parse_only=results)
        return soup

    def extract_job_title_from_result(soup): 
        jobs = []
        for div in soup.find_all(name='div', attrs={'class':'row'}):
            for a in div.find_all(name='a', attrs={'data-tn-element':'jobTitle'}):
                jobs.append(a['title'])
        return(jobs)

    def extract_company_from_result(soup): 
        companies = []
        for div in soup.find_all(name='div', attrs={'class':'row'}):
            company = div.find_all(name='span', attrs={'class':'company'})
            if len(company) > 0:
                for b in company:
                    companies.append(b.text.strip())
            else:
                sec_try = div.find_all(name='span', attrs={'class':'result-link-source'})
                for span in sec_try:
                    companies.append(span.text.strip())
        return(companies)

    def extract_location_from_result(soup): 
        locations = [span.text for span in soup.findAll('span', attrs={'class': 'location'})]
        return(locations)

    def extract_salary_from_result(soup): 
        salaries = []
        for div in soup.find_all(name='div', attrs={'class':'row'}):
            try:
                salaries.append(div.find('nobr').text)
            except:
                try:
                    div_two = div.find(name='div', attrs={'class':'sjcl'})
                    div_three = div_two.find('div')
                    salaries.append(div_three.text.strip())
                except:
                    salaries.append(None)
        return(salaries)

    def extract_summary_from_result(soup): 
        summaries = [span.text.strip() for span in soup.findAll('span', attrs={'class': 'summary'})]
        return(summaries)

    def extract_date_from_result(soup): 
        dates = []
        for i, article in enumerate(soup.find_all('div', attrs={'class': 'result'})):
            date = article.find('span', attrs={'class': 'date'})
            date_string = date.text.replace('+', '') if date else ''
            dates.append(SeekScraper.get_past_date(date_string) if date else None)
        return(dates)

    def extract_ids_from_result(soup): 
        ids = []
        for i, article in enumerate(soup.find_all('div', attrs={'class': 'result'})):
            data = article.attrs
            ids.append(data['data-jk']if 'data-jk' in data else None)
        return(ids)

    def _job_listings(self, company):

        def mklist(n):
            for _ in range(n):
                yield []   

        self.logger.info('get indeed company jobs: %s' % (company,))
        http = urllib3.PoolManager()
        response = http.request('GET', self.get_url(company), headers=self.headers)
        results = SoupStrainer("td", attrs={"id": "resultsCol"})
        soup = BeautifulSoup(response.data, "html.parser", parse_only=results)
        count = soup.find('div', {'id': 'searchCount'}).text.split(' ')[-1]
        pages = int(count) / 10

        jobids,titles,companies,locations,areas,category = mklist(6)
        subcategory,headers,short_descriptions,dates,salaries = mklist(5)

        page = 0
        search_page = soup
        while search_page is not None:
            jobids.append(None)
            titles.append(None)
            companies.append(None)
            locations.append(None)
            areas.append(None)
            category.append(None)
            subcategory.append(None)
            headers.append(None)
            short_descriptions.append(None)
            today = datetime.date.today()
            dates.append(today.strftime('%Y-%m-%d'))
            salaries.append(None)

            jobids = IndeedScraper.extract_ids_from_result(search_page)
            titles = IndeedScraper.extract_job_title_from_result(search_page)
            locations = IndeedScraper.extract_location_from_result(search_page)
            companies = IndeedScraper.extract_company_from_result(search_page)
            salaries = IndeedScraper.extract_salary_from_result(search_page)
            headers = IndeedScraper.extract_summary_from_result(search_page)
            dates = IndeedScraper.extract_date_from_result(search_page)

            areas = [None for i in range(len(titles))]
            short_descriptions = [None for i in range(len(titles))]
            category = [None for i in range(len(titles))]
            subcategory = [None for i in range(len(titles))]

            results = list(zip(jobids, dates, titles, companies, locations, areas, category, subcategory, salaries, headers, short_descriptions))
            df = pd.DataFrame(results, columns=['jobid', 'date', 'title', 'company', 'location', 'area', 'category', 'subcategory', 'salary', 'header', 'shortdescription'])
            df['ticker'] = company
            df['description'] = None
            df['provider'] = 2
            
            df.to_sql('jobs', con=self.db, if_exists='append', index=False) #, dtype={"jobid": Integer()})
            self.db.commit()
            search_page = None
            page = page + 1
            try:
                if page <= int(pages):
                    search_page = self.next(page, company)
                else:
                    search_page = None
            except:
                print('could not get the next page')
                # type, value, traceback = sys.exc_info()
                # print('Error opening: %s' % ( value))
                search_page = None
