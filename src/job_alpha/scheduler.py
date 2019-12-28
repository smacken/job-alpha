'''
    Scraping scheduler
    config: companies.json - list of company keywords to search for
    ScrapeScheduler.scrape_times: times to add newly scraped jobs to queue

    req: ASX code > keyword e.g. TNE.AX = technology one
'''
import os
from queue import Queue
from random import randint
from datetime import datetime, timedelta
from itertools import chain, cycle, islice
import json
import time
import logging
import schedule
import time
import re
import sqlite3
from pypika import Query, Table, Field, Interval, Order
from pypika import functions as fn

from .scraper import Scraper

module_logger = logging.getLogger('jobs.jobs_alpha.SeekScheduler')

class ScrapeScheduler(object):
    queue = Queue(maxsize=0)
    scraper = None
    company_list = 'companies.json'
    jobs = Table('jobs')
    previous_scrapes = Table('scrapes')
    scrape_times = ['10:30', '18:05', '23:20']

    def __init__(self, with_schedule=True):
        self.logger = logging.getLogger('jobs.jobs_alpha.Scheduler')
        self.logger.info('creating an instance of scheduler')
        datadir = os.path.join(os.path.abspath(os.path.join(__file__, os.pardir)))
        db_path = os.path.abspath(os.path.join(datadir, 'jobs.db'))
        self.conn = sqlite3.connect(db_path, timeout=10)
        self.scraper = Scraper()
        if with_schedule:
            schedule.every().day.at('18:02').do(self.get_company_jobs)
            # get job details
            schedule.every(1).to(2).minutes.do(self.schedule_job)
            # get job listings
            for scrape_time in self.scrape_times:
                schedule.every().day.at(scrape_time).do(self.update_job_details)
            self.get_company_jobs()
            self.update_job_details()
            while True:
                schedule.run_pending()
                time.sleep(1)

    def __del__(self):
        del self.scraper

    def add_to_scrape_queue(self, job):
        self.queue.put(job)

    def schedule_job(self):
        ''' retrieve the details of a specific job by id '''
        if self.queue.empty():
            self.logger.info('queue is empty')
            return
        
        jid, jobid, provider = self.queue.get()
        self.logger.info('scraping job: %s' % (str(jobid)))
        descr = self.scraper.update_job_details(jid, jobid, provider)
        sql = Query.update(self.jobs).set(self.jobs.description, descr).where(self.jobs.jobid == jobid)
        db = self.conn.cursor()
        db.execute(str(sql))
        self.conn.commit()
        db.close()

    @staticmethod
    def roundrobin(*iterables):
        "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
        num_active = len(iterables)
        nexts = cycle(iter(it).__next__ for it in iterables)
        while num_active:
            try:
                for next in nexts:
                    yield next()
            except StopIteration:
                # Remove the iterator we just exhausted from the cycle.
                num_active -= 1
                nexts = cycle(islice(nexts, num_active))

    def update_job_details(self):
        ''' 
            find jobs for which their details have not yet been retrieved 
            ** an empty string for desc means that the job was no longer posted
        '''
        db = self.conn.cursor()
        query = Query.from_(self.jobs).select(
            self.jobs.id, self.jobs.jobid, self.jobs.provider
            ).where(
            self.jobs.description.isnull()
            )
        jobs = db.execute(str(query)).fetchall()
        db.close()
        self.logger.info('filling job queue %s' % (len(jobs),))

        p1 = [o for o in jobs if o[2] == 1]
        p2 = [o for o in jobs if o[2] == 2]
        job_order = ScrapeScheduler.roundrobin(p1, p2)

        for job in job_order:
            self.add_to_scrape_queue(job)

    def get_previous_scrapes(self):
        ''' get the list of executed companies jobs retrieved '''
        db = self.conn.cursor()
        query = Query.from_(self.previous_scrapes
            ).select(
                self.previous_scrapes.company, 
                self.previous_scrapes.scrape_date
            ).orderby(self.previous_scrapes.scrape_date, order=Order.desc)
            # .where(
            #     self.previous_scrapes.scrape_date > fn.Now() - Interval(months=12)
            # ).orderby(self.previous_scrapes.scrape_date, order=Order.desc)
        print(str(query))
        scrapes = db.execute(str(query)).fetchall()
        db.close()
        return scrapes

    def add_scrape(self, scrape):
        db = self.conn.cursor()
        query = Query.into(self.previous_scrapes).columns('company').insert(scrape[0])
        db.execute(str(query))
        self.conn.commit()
        db.close()

    def get_company_jobs(self):
        ''' trigger the scraper to retrieve the companies listed in config '''
        self.logger.info('retrieving jobs listed for company list')
        scrapes = self.get_previous_scrapes()
        print(scrapes)
        with open(self.company_list) as json_data:
            company_json = json.load(json_data)
            for company in company_json['companies']:
                prev = [s for s in scrapes if s[0] == company]
                last_scrape = prev[0][1] if len(prev) > 0 else None
                if last_scrape is None or (datetime.now() - datetime.strptime(last_scrape, '%Y-%m-%d  %H:%M:%S')).days > 10: # < timedelta(days=30):
                    self.scraper.job_listings(company)
                    self.add_scrape((company, ))
                    time.sleep(22)
            self.scraper.close_driver()
            self.remove_duplicates()

    def remove_duplicates(self):
        ''' featured jobs can be included for multiple pages '''
        db = self.conn.cursor()
        db.execute(''' 
            DELETE FROM jobs
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM jobs
                GROUP BY jobid
            )
        ''')
        db.execute(''' 
            DELETE from jobs
            WHERE jobid IS NULL
        ''')
        self.conn.commit()
        db.close()
    