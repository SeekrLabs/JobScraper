import json
import botocore.vendored.requests as requests
import time
from bs4 import BeautifulSoup
import boto3

# Run this once a day, it gets completely refreshed once per day
BASE_LINK = 'https://www.indeed.ca/jobs?l=Toronto,+ON&sort=date&fromage=1&limit=20'
dynamodb = boto3.resource('dynamodb')
jobs_table = dynamodb.Table('IndeedJobsTable')

def scrape(event, context):
    search = IndeedSearch(BASE_LINK)
    num_pages = event['pages']
    early_stop = event['ads_per_page']
    for i in range(num_pages):
        results = search.process_visit_link(early_stop)
        for res in results:
            jobs_table.put_item(
                Item=vars(res)
            )
        search.update_visit_link()


class IndeedSearch:
    # base_link is the base query without indexing the pages of the search
    # visit_link is indexed pages, it's updated
    def __init__(self, base_link):
        self.base_link = base_link
        self.num_ads = 0 
        self.visit_link = base_link + '&start=' + str(self.num_ads)
    
    def update_visit_link(self):
        self.num_ads += 20
        self.visit_link = self.base_link + '&start=' + str(self.num_ads)

    def process_visit_link(self, ads_to_visit=20):
        jobs_data = []
        print('Issuing GET: ' + self.visit_link)
        search_query = requests.get(self.visit_link)

        print('GET Success, Parsing...')
        search_soup = BeautifulSoup(search_query.text, 'html.parser')

        print('Finding advertisement cards...')
        ad_card_soups = search_soup.find_all('div', {'class': 'jobsearch-SerpJobCard'})
        print('Found ' + str(len(ad_card_soups)) + ' ad cards.')

        for ad_card_soup in ad_card_soups:
            job_ad = IndeedJobAd(ad_card_soup)
            job_ad.extract_card()
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
    def __init__(self, ad_soup):
        self.ad_soup = ad_soup

    def extract_card(self):
        self.time_scraped = int(time.time())
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
            self.post_date = post_date_soup.text.strip()

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


def find_element_from_soup(soup, specs):

    for spec in specs:
        print('Looking for ' + spec['el'] + ' ' + spec['tag'] 
                + ' ' + spec['attr'] + '... Found if not otherwise stated.')
        result = soup.find(spec['el'], {spec['tag'], spec['attr']})
        if result:
            return result
    print('NOT FOUND ' + specs[0]['attr'] + '... '  + str(soup.attrs))
    return None

def get_dict_size(d):
    size = 0
    for k in d.keys():
        if type(d[k]) == int:
            size = size + len(k) + 8
        else:
            size = size + len(k) + len(d[k])
    return size