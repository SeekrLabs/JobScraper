## Current Implementation
This service uses the serverless framework to be deployed via CloudFormation on
handler.scrape is called on a scheduled every 4 hours and scrapes the last 1000
Jobs that's available on Indeed in Toronto.

The BASE_URL is a base search result query on Indeed, the handler.scrape
function ingests this URL, scrapes cards that's on the page 1 of the search 
result, and moves on the page 2 and scrapes the cards on that page and repeats
until the specified number of pages to be scraped is reached.

## Onboarding
To fully understand how this package works
1. [CloudFormation](https://aws.amazon.com/cloudformation/)
2. [Webscraping](https://en.wikipedia.org/wiki/Web_scraping)
3. [Serverless Framework](https://serverless.com/)
4. [DynamoDB](https://aws.amazon.com/dynamodb/)