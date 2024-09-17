import csv
import json
from copy import deepcopy
import gspread
from scrapy import signals
import scrapy
from datetime import datetime
import os
import glob
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

class ZillowSpiderSpider(scrapy.Spider):
    name = "zillow_spider"
    baseurl = 'https://www.zillow.com'

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.4,
                       'CONCURRENT_REQUESTS': 1,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_URI': f'outputs/zillow_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
                       'FEED_FORMAT': 'csv',

                       'FEED_EXPORT_ENCODING': 'utf-8',
                       'ZYTE_API_KEY': "xxxxxxxxxxxxxxxxxxxxxx",
                       'ZYTE_API_TRANSPARENT_MODE': True,

                       'DOWNLOAD_HANDLERS': {
                           "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
                           "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
                       },
                       'DOWNLOADER_MIDDLEWARES': {
                           "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
                       },
                       'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
                       'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

                       }
    headers = {
        'authority': 'www.zillow.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'cookie': 'zguid=24|%24f8149c67-7bd0-4ef3-b108-746c10f6c33d; zgsession=1|9ed24d21-90ca-4d10-9b6b-ad5fa3c88292; _ga=GA1.2.154075965.1694759461; _gid=GA1.2.1889301566.1694759461; zjs_anonymous_id=%22f8149c67-7bd0-4ef3-b108-746c10f6c33d%22; zjs_user_id=null; zg_anonymous_id=%22c4fbd29a-7f2d-4c27-91a2-1a69411bd4dd%22; _gcl_au=1.1.754909016.1694759462; DoubleClickSession=true; pxcts=5d9b234f-5391-11ee-aa84-835a3b460182; _pxvid=5d9b1299-5391-11ee-aa84-7003240f5c52; __pdst=9fbfc546e82c4f359c8d2fff2426eb2a; _fbp=fb.1.1694759470945.1062494261; _pin_unauth=dWlkPVl6VTBZamhqT1RNdE56RTNaaTAwTXpFeUxXRXhaalF0T1dFek5HVTFaR0ZtTkRJMQ; _clck=vxzcfk|2|ff1|0|1353; FSsampler=1584303648; JSESSIONID=66583C1CC76E38BCA5120EF6222DD02D; tfpsi=a0e89d16-7ea6-431e-8021-ab6b11f0d0e2; x-amz-continuous-deployment-state=AYABeG6w1r5i+ErWkjxGTMHdxNoAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADMR5UYc0CDQaBT30+gAwwnRke3FpcApRDjWya7EwBinqa5%2FYg3pb+vIYpV%2Fb8goDaJGSwPyXYQKKsQeeNDftAgAAAAAMAAQAAAAAAAAAAAAAAAAAAEQ6KTKqLNEi8ZLTvb9RkQn%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAwI6fyTVaWPRscGWA5qkQsDutZOd7mhtjPz0h39ZOd7mhtjPz0h3w==; _gat=1; _pxff_cc=U2FtZVNpdGU9TGF4Ow==; _pxff_cfp=1; _pxff_bsco=1; search=6|1697360638511%7Crect%3D34.267076234205256%2C-117.19449876171876%2C33.663424603955676%2C-119.35331223828126%26rid%3D95984%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26z%3D1%26listPriceActive%3D1%26baths%3D3.0-%26beds%3D4-%26fs%3D0%26fr%3D1%26mmm%3D0%26rs%3D0%26ah%3D0%26singlestory%3D0%26housing-connector%3D0%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26featuredMultiFamilyBuilding%3D0%26excludeNullAvailabilityDates%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%0995984%09%7B%22isList%22%3Atrue%2C%22isMap%22%3Atrue%7D%09%09%09%09%09; __gads=ID=c7a3a86198dda95d:T=1694760246:RT=1694768640:S=ALNI_MbVemHp3Ohsvz2EOOm163M6CPFcNg; __gpi=UID=00000ca15bd90df1:T=1694760246:RT=1694768640:S=ALNI_MYSvBqEG0uJfMHB2PTjKdrOqrZF6Q; _hp2_id.1215457233=%7B%22userId%22%3A%227366386339100261%22%2C%22pageviewId%22%3A%222265972612853530%22%2C%22sessionId%22%3A%227803696774092854%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _px3=2070b6e5cf44b5c3affe88030ba6d71a5626f4f66e92a47212c37831ac84f08e:VaylFXsdK6FdXrKuaAG5oF8KYKs6ybhkKcAjJuC2bnYg8uF3jamc2JUI9cKU8vj5avgQ9EMw2jH27/zm/xSmLA==:1000:ZnJ3quocoDDMXVyXi499AoQiB5grv/Q9JgqP2WcYfHICpvzbzMG3kOSAkAWAqVGyhAnnL0t//fkZNmJISICy6UOb4ePunSKCVcPpKyROV/v3n9MSnGdz7vZUGQoZbcUJ2XAGzNr/f6zsVoKBcvoyE2hdgtSEC1nnV8Frqmbw/oUZX1gsKkd6kHgzdLD0EqOTQ3LL4A8jgMjKiLKj4gpj7g==; _hp2_ses_props.1215457233=%7B%22ts%22%3A1694768694517%2C%22d%22%3A%22www.zillow.com%22%2C%22h%22%3A%22%2F%22%7D; _uetsid=75b93b10539111eebca64148bd6f79ac; _uetvid=75b9ace0539111ee8d9e7bef1d45bf8f; _clsk=1rut27l|1694768696567|14|0|h.clarity.ms/collect; AWSALB=ePkECIdVK8bU7e9sv8Hb43Y4EZ5nKK71FLrf/BQp4sJHR7QodV+F9wJXra/JQHAhFjuMWVxqakdopppVOA+sSQOPMQWuYpnwiKuYWvUFl0JCbEytRsl6zR8cOXlE; AWSALBCORS=ePkECIdVK8bU7e9sv8Hb43Y4EZ5nKK71FLrf/BQp4sJHR7QodV+F9wJXra/JQHAhFjuMWVxqakdopppVOA+sSQOPMQWuYpnwiKuYWvUFl0JCbEytRsl6zR8cOXlE',
        'dnt': '1',
        'if-none-match': '"3c69d-z5hA0VpniYIUvZTn6KirNjliE8s"',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    }


    query_state = {
        "pagination": {"currentPage": 1},
        "isMapVisible": True,
        "filterState": {
            "sort": {"value": "globalrelevanceex"},
            "price": {"min": 80000, "max": 350000},
            # "beds": {"min": 4}, "baths": {"min": 3},
            "ah": {"value": True}
        },
        "isListVisible": True
    }

    query_state_str = json.dumps(query_state)
    detail_page_links =[]
    all_items_list_of_list = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ZillowSpiderSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def read_input_file(self, filename):
        with open(f'input/{filename}.csv','r')as file:
            data = list(csv.DictReader(file))
            return data

    def start_requests(self):
                                      # read from google sheet
        gc = gspread.service_account(filename='zillow-listings-432016-5569c3385037.json')
        self.sheet = gc.open_by_key("1y4UH4AIfY-4NCY65raCwwVYVlYnu5IewFp3d_Fs-940")
        worksheet = self.sheet.worksheet("Input")
        records = worksheet.get_all_records()
        for each_row in records[:1]:
            county= each_row.get('county','').strip()
            state_code= each_row.get('state_code','').strip()
            country_slug = f'{county}-county-{state_code}'
            final_url = f'https://www.zillow.com/{country_slug}/?searchQueryState={self.query_state_str}'
            yield scrapy.Request(url=final_url, headers=self.headers, meta={'current_page': 1,
                                                                            'country_slug': country_slug})



    def parse(self, response, **kwargs):
        country_slug = response.meta.get('country_slug', '')
        json_text = response.css("#__NEXT_DATA__::text").get('')
        if json_text:
            json_data = json.loads(json_text)
            homes_listing = json_data.get('props', {}).get('pageProps', {}).get('searchPageState', {}).get('cat1', {}).get(
                'searchResults', {}).get('listResults', [])
            for home in homes_listing:
                item = dict()
                item['Detail URL'] = home.get('detailUrl', '')
                detail_url = home.get('detailUrl', '')
                item['Address'] = home.get('address', '')
                item['Beds'] = home.get('beds', '')
                item['Baths'] = home.get('baths', '')
                item['Area'] = home.get('area', '')
                item['Price'] = home.get('price', '')
                yield response.follow(url=item['Detail URL'], headers=self.headers, callback=self.detail_page,
                                      meta={'item': item,'detail_url':detail_url})

            current_page = response.meta['current_page']
            total_page = int(json_data.get('props', {}).get('pageProps', {}).get('searchPageState', {}).get('cat1', {}).
                             get('searchList', {}).get('totalPages', ''))


            if current_page < total_page:
                query_state = deepcopy(self.query_state)
                query_state['pagination']['currentPage'] = current_page + 1
                query_state_str = json.dumps(query_state)
                final_url = f'https://www.zillow.com/{country_slug}/?searchQueryState={query_state_str}'
                yield response.follow(url=final_url, headers=self.headers, callback=self.parse,
                                          meta={'current_page': current_page + 1,
                                                'country_slug':country_slug})

    def detail_page(self, response):
        item = response.meta['item']
        detail_url = response.meta['detail_url']
        # json_data = json.loads(response.css("#__NEXT_DATA__::text").get(''))

        json_text = response.css("#__NEXT_DATA__::text").get('')
        if json_text:
            json_data = json.loads(json_text)


            detail = json_data.get('props', {}).get('pageProps', {}).get('componentProps', {})
            home_detail = detail.get('gdpClientCache', '')
            if home_detail:
                home_data = json.loads(home_detail)
                detail_key = list(home_data.keys())[0]
                home = home_data.get(detail_key, {}).get('property', '')
            else:
                home = detail.get('initialReduxState', {}).get('gdp', {}).get('building', {})
            item['Property Sub Type'] = "".join(home.get('resoFacts', {}).get('propertySubType', []))
            item['Year Built'] = home.get('yearBuilt', '')
            item['Parking Capacity'] = home.get('resoFacts', {}).get('parkingCapacity', '')
            item['Price/Sqft'] = home.get('resoFacts', {}).get('pricePerSquareFoot', '')
            item['Description'] = home.get('description', '')
            description = home.get('description', '')
            item['Time On Zillow'] = home.get('timeOnZillow', '')
            item['Views'] = home.get('pageViewCount', '')
            item['Saves'] = home.get('favoriteCount', '')
            item['Days On Zillow'] = home.get('daysOnZillow', '')
            item['Agent Name'] = home.get('attributionInfo', {}).get('agentName', '')
            item['Agent Phone'] = home.get('attributionInfo', {}).get('agentPhoneNumber', '')
            item['Brokerage Name'] = home.get('attributionInfo', {}).get('brokerName', '')

            item['MLS Name'] = home.get('attributionInfo', {}).get('mlsName', '')
            item['MLS ID'] = home.get('attributionInfo', {}).get('mlsId', '')
            item['Last Checked'] = home.get('attributionInfo', {}).get('lastChecked', '')
            item['Last Updated'] = home.get('attributionInfo', {}).get('lastUpdated', '')
            if description is None:
                description = ''

            if "AS-IS" in description or " as-is" in description or "investors" in description or "investor" in description or "TLC" in description or 'tlc' in description:
                print("found")
                yield item

            # for google sheet
                self.all_items_list_of_list.append([
                item['Detail URL'], item['Address'], item['Beds'], item['Baths'],item['Area'], item['Price'],
                  item['Property Sub Type'], item['Year Built'],item['Parking Capacity'],
                    item['Price/Sqft'],item['Description'], item['Time On Zillow'],item['Views'], item['Saves'], item['Days On Zillow'],
                    item['Agent Name'], item['Agent Phone'], item['Brokerage Name'],
                    item['MLS Name'], item['MLS ID'],item['Last Checked'],item['Last Updated']])

            else:
                print("not exact match")


    def sendmail(self):
        list_of_files = glob.glob('outputs/*')
        latest_file = list_of_files[-1] if len(list_of_files) >= 1 else ''
        file_path = os.path.basename(latest_file)

        try:
            subject = "Zillow Script Update"
            body = f"""
                <!doctype html>
                <html>
                    <head>
                        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                    </head>
                    <body style="font-family: sans-serif;">
                        <div style="display: block; margin: auto; max-width: 600px;" class="main">
                            <h1 style="font-size: 18px; font-weight: bold; margin-top: 20px">
                                Email Notification...
                            </h1>
                        </div>
                        <h2>Zillow Script Update</h2>
                        <h4>Latest Data on {datetime.now().strftime('%d_%b_%Y_%H_%M_%S')}</h4>
                        <p>Here is the attached "csv" file of zillow.com via email</p>
                    </body>
                </html>
            """
            msg = MIMEMultipart()
            msg['From'] = 'leads@cashhousebuyers4you.com'
            msg['To'] = 'ryanwilliamrealestate@gmail.com'
            msg['Subject'] = subject
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)
            if file_path:
                with open(f'outputs/{file_path}', 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename={file_path}',
                    )
                    msg.attach(part)

            text = msg.as_string()
            with smtplib.SMTP(host='smtp.gmail.com', port=587, timeout=120) as server:
                server.ehlo()
                server.starttls()
                server.login('leads@cashhousebuyers4you.com', 'osjs bjzc bdkr optj')
                server.sendmail('leads@cashhousebuyers4you.com', 'ryanwilliamrealestate@gmail.com', text)

            print('Email sent successfully')
        except Exception as e:
            print(f'Error sending email: {e}')

    def spider_closed(self, spider):
        # SEND TO GOOGLE SHEET
        gc = gspread.service_account(filename='zillow-listings-432016-5569c3385037.json')
        self.sheet = gc.open_by_key("1y4UH4AIfY-4NCY65raCwwVYVlYnu5IewFp3d_Fs-940")
        output_sheet = self.sheet.worksheet('Output')
        updated_old_records = [
            ['Detail URL', 'Address', 'Beds', 'Baths', 'Area', 'Price', 'Property Sub Type',
             'Year Built', 'Parking Capacity', 'Price/Sqft', 'Description', 'Time On Zillow',
             'Views', 'Saves', 'Days On Zillow', 'Agent Name', 'Agent Phone', 'Brokerage Name',
             'MLS Name', 'MLS ID', 'Last Checked', 'Last Updated']
        ]
        updated_old_records.extend(self.all_items_list_of_list)
        output_sheet.clear()
        output_sheet.update(
            'A1', updated_old_records, value_input_option='USER_ENTERED')
        self.sendmail()










