from playwright.async_api import async_playwright, Page, Playwright
import asyncio
from rich.logging import RichHandler
import logging
import inspect
import os, json, pprint
import sqlite3
from fake_useragent import UserAgent
from dataclasses import dataclass, asdict, field
import pandas as pd
import functools
import time
from openpyxl import load_workbook

def logs():
    frame = inspect.currentframe().f_back 
    file_name = os.path.basename(frame.f_globals['__file__'])
    logger_name = f"{file_name}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.DEBUG)

    terminal = RichHandler()
    logger.addHandler(terminal)
    
    handle = logging.FileHandler("scrape.log", mode='w')
    formats = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handle.setFormatter(formats)
    logger.addHandler(handle)
   
    return logger

def timer(func):
    @functools.wraps(func)
    async def wrapper(*agrs, **kwargs):
        start = time.perf_counter()
        await func(*agrs, **kwargs)
        end = time.perf_counter()
        total = end - start
        log.info(f"Execution time: {round(total, 2)}")
    return wrapper


log = logs()


@dataclass
class Opticron_PlaceBuy:
    """ A ```DATACLASS``` obj """
    Company: str = None
    Address: str = None
    Town: str = None
    Postalcode: str = None
    Telephone: str = None
    Web: str = None
    Type: str = None

@dataclass
class Opticron_Events:
    Event_Location: str = None
    Town: str = None
    Country: str = None
    Postcode: str = None

@dataclass
class SaveData:
    file: str = ''
    folder:str = ''
    _path:str = ''
    data_list: list[dataclass] = field(default_factory=list)
   
    def add_item(self, items):
        return self.data_list.append(items)
   
    def create_folder(self):
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
        self._path = f"{self.folder}/{self.file}"
        return self._path
    
    def dataframe(self):
        return pd.json_normalize((asdict(data) for data in self.data_list), sep='_')

    def save_to_json(self):
        if not os.path.exists(f'{self._path}.json'):
            self.dataframe().to_json(f'{self._path}.json', orient='records', index=False, indent=3)
        else:
            existing_df = pd.read_json(f"{self._path}.json")
            new_df = self.dataframe()
            update_df = pd.concat([existing_df, new_df])
            update_df.to_json(f"{self._path}.json", orient='records', indent=2)

    def save_to_csv(self):
        if os.path.exists(f'{self._path}.csv'):
            self.dataframe().to_csv(f"{self._path}.csv", index=False, mode='a', header=False)
        else:
            self.dataframe().to_csv(f'{self._path}.csv', index=False)
    
    def save_to_excel(self):
        self.dataframe().to_excel(f'{self._path}.xlsx', index=False)
        
    def save_to_sqlite(self):        
        conn = sqlite3.connect(f'{self._path}.db')
        self.dataframe().to_sql(name='scrapedData', con=conn, index=False, if_exists='replace')
        conn.close()

    
    def save_all(self):
        log.info('Saveing data...')
        self.create_folder()
        self.save_to_json()
        self.save_to_csv()
        self.save_to_excel()
        self.save_to_sqlite()
        log.debug('Done saveing...')
        
class BotScraper:
    def __init__(self, url) -> None:
        self.url = url
        self.page:Page
        self.playwright:Playwright
        
        asyncio.run(self.main())
            
    async def browser(self)->None:
        log.info("Starting Browser")
        browser = await self.playwright.firefox.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 650, 'height': 540},
            user_agent = UserAgent().random
        )
        self.page = await context.new_page()
        await self.page.goto(self.url,timeout=50000)
    
    async def navigation(self):
        log.info('Navigating to map')
        await self.page.wait_for_timeout(3000)
        
        frame = await self.page.locator('iframe').first.get_attribute('src')
        await self.page.goto(frame)
        
        await self.page.wait_for_selector('[class="i4ewOd-pzNkMb-ornU0b-b0t70b-Bz112c"]')
        await self.page.locator('[class="i4ewOd-pzNkMb-ornU0b-b0t70b-Bz112c"]').click()
                
        log.debug('Done Navigateing')
        
    async def goBack(self):
        await self.page.get_by_role('button', name='Back').click()
    
    async def _opticron_placebuy(self):
        await self.page.wait_for_timeout(2000)
        await self.page.locator('[class="HzV7m-pbTTYe-KoToPc-ornU0b"]').first.click()
        divs  = await self.page.locator('[class="HzV7m-pbTTYe-ibnC6b pbTTYe-ibnC6b-d6wfac"]').all()
        store = SaveData(file='opticron', folder='opticron_data (place to buy)')
            
        for div in divs[0:31]:
            await div.click()
            data = await self.page.locator('div.qqvbed-p83tee > div.qqvbed-p83tee-lTBxed').all()
            company = await data[0].inner_text()
            address = await data[1].inner_text()
            town = await data[2].inner_text()
            postcode = await data[3].inner_text()
            telephone = await data[4].inner_text()
            try:
                web = await data[5].inner_text()
            except:
                web = "No website Data"
            try:
                type= await data[6].inner_text()
            except:
                try:
                    type = await data[5].inner_text()
                except:
                    type = "No type Data"
            item = Opticron_PlaceBuy(
                Company=company,
                Address=address,
                Town=town,
                Postalcode=postcode,
                Telephone=telephone,
                Web=web,
                Type=type
            )
            await self.goBack()
            log.debug(f"{item}")
            store.add_item(item)
        store.save_all()
    
    async def _opticron_event(self):
        await self.page.wait_for_timeout(2000)
        await self.page.locator('[class="HzV7m-pbTTYe-KoToPc-ornU0b"]').last.click()
        divs  = await self.page.locator('div.HzV7m-pbTTYe-JNdkSc-PntVL').last.locator('div').all()
        store = SaveData(file='opticron', folder='opticron_data (Event)')
            
        for div in divs[0:31]:
            await div.click()
            data = await self.page.locator('div.qqvbed-p83tee > div.qqvbed-p83tee-lTBxed').all()
            event = await data[0].inner_text()
            town = await data[1].inner_text()
            country = await data[2].inner_text()
            postcode = await data[3].inner_text()
            item = Opticron_Events(
                Event_Location=event,
                Town=town,
                Country=country,
                Postcode=postcode
            )
            await self.goBack()
            log.debug(f"{item}")
            store.add_item(item)
        store.save_all()
            
    @timer         
    async def main(self):
        async with async_playwright() as self.playwright:
            await self.browser()
            await self.navigation()
            await self._opticron_placebuy()
            await self._opticron_event()
                

if __name__ == '__main__':
    try:
        bot = BotScraper('https://www.opticron.co.uk/dealers-and-events')
    except Exception as e:
        log.error(f'{e}', exc_info=True)
        
    
        