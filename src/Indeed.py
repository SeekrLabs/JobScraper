import json
import botocore.vendored.requests as requests
import time
from bs4 import BeautifulSoup
import boto3
import datetime

# Run this once a day, it gets completely refreshed once per day
BASE_LINK = 'https://www.indeed.ca/jobs?l=Toronto,+ON&sort=date&fromage=1&limit=50'
sqs = boto3.resource('sqs')
QUEUE_MESSAGE_SIZE = 50
queue = sqs.get_queue_by_name(
    QueueName='JobsIngestionQueue'
)

def scrape(event, context):
    scrape_start_time = int(time.time())
    search = IndeedSearch(BASE_LINK, scrape_start_time)
    num_pages = event['pages']
    early_stop = event['ads_per_page']
    job_ads = []

    for _ in range(num_pages):
        results = search.process_visit_link(early_stop)
        job_ads += [vars(res) for res in results]
        search.update_visit_link()
    
    print("Sending message to SQS queue.")
    sqs_batch_send_message(job_ads)

def sqs_batch_send_message(content_list):
    if content_list == []:
        return

    num_batches = len(content_list) // 50 + 1
    for i in range(num_batches):
        batch_send = content_list[i * num_batches: i * num_batches + 50]
        queue.send_message( 
            MessageBody=json.dumps(batch_send)
        )
        
class IndeedSearch:
    # base_link is the base query without indexing the pages of the search
    # visit_link is indexed pages, it's updated
    def __init__(self, base_link, scrape_start_time):
        self.base_link = base_link
        self.num_ads = 0 
        self.visit_link = base_link + '&start=' + str(self.num_ads)
        self.scrape_start_time = scrape_start_time
    
    def update_visit_link(self):
        self.num_ads += 20
        self.visit_link = self.base_link + '&start=' + str(self.num_ads)

    def process_visit_link(self, ads_to_visit=999):
        jobs_data = []
        print('Issuing GET: ' + self.visit_link)
        search_query = requests.get(self.visit_link)

        print('GET Success, Parsing...')
        search_soup = BeautifulSoup(search_query.text, 'html.parser')

        print('Finding advertisement cards...')
        ad_card_soups = search_soup.find_all('div', {'class': 'jobsearch-SerpJobCard'})
        print('Found ' + str(len(ad_card_soups)) + ' ad cards.')

        for ad_card_soup in ad_card_soups:
            job_ad = IndeedJobAd(ad_card_soup, self.scrape_start_time)
            valid_card = job_ad.extract_card()
            if valid_card:
                jobs_data.append(job_ad)
            if len(jobs_data) > ads_to_visit:
                break

        # Visiting each link in ad card
        for ad in jobs_data:
            ad.visit_link_to_extract_description()

        return jobs_data


class IndeedJobAd:
    # Constants
    BASE_INDEED = 'https://www.indeed.com'

    # Initalize with a BeautifulSoup Card element
    def __init__(self, ad_soup, scrape_start_time):
        self.ad_soup = ad_soup
        self.scrape_start_time = scrape_start_time

    # Returns false if Job Posting is sponsored
    def extract_card(self):
        title_soup = find_element_from_soup(self.ad_soup,
                [{'el': 'a',
                'tag': 'class',
                'attr': 'jobtitle'}])
        metadata_soup = find_element_from_soup(self.ad_soup,
                [{'el': 'div',
                'tag': 'class',
                'attr': 'sjcl'}])
        post_date_soup = find_element_from_soup(self.ad_soup,
                [{'el': 'span',
                'tag': 'class',
                'attr': 'date'}])
        del self.ad_soup
    
        if title_soup:
            self.title = title_soup.text.strip()
            self.url = title_soup['href']
            self.apply_url = self.BASE_INDEED + title_soup['href']
            if self.url.startswith('/pagead'):
                return False

        if metadata_soup:
            company_soup = find_element_from_soup(metadata_soup,
                    [{'el': 'span',
                      'tag': 'class',
                      'attr': 'company'}])

            location_soup = find_element_from_soup(metadata_soup,
                    [{'el': 'span',
                      'tag': 'class',
                      'attr': 'location'},
                      {'el': 'div',
                      'tag': 'class',
                      'attr': 'location'}])

            if company_soup:
                self.company = company_soup.text.strip()
            if location_soup:
                self.location = location_soup.text.strip()

        if post_date_soup:
            self.get_post_date_and_time(post_date_soup.text.strip())
        
        return True

    def visit_link_to_extract_description(self):
        if self.url:
            job_url = self.BASE_INDEED + self.url

            print('Issuing GET: ' + job_url)
            job_response = requests.get(job_url)
            print('GET Success, Parsing...')

            specific_job_soup = BeautifulSoup(job_response.text, 'html.parser')
            description = find_element_from_soup(specific_job_soup,
                    [{'el': 'div',
                      'tag': 'class',
                      'attr': 'jobsearch-JobComponent-description'}])
            if description:
                self.description = str(description)

    def get_post_date_and_time(self, post_time):
        post_time_epoch = self.scrape_start_time

        if 'hour' in post_time:
            num_hours = int(post_time[0:2].strip())
            post_time_epoch -= num_hours * 60 * 60
        
        elif 'minute' in post_time:
            num_hours = int(post_time[0:2].strip())
            post_time_epoch -= num_hours * 60
        
        self.post_date = datetime.datetime.utcfromtimestamp(post_time_epoch).strftime('%Y-%m-%d %H:%M:%S')
            

def find_element_from_soup(soup, specs):
    for spec in specs:
        print('Looking for ' + spec['el'] + ' ' + spec['tag'] 
                + ' ' + spec['attr'] + '... Found if not otherwise stated.')
        result = soup.find(spec['el'], {spec['tag'], spec['attr']})
        if result:
            return result
    print('NOT FOUND ' + specs[0]['attr'] + '... '  + str(soup.attrs))
    return None