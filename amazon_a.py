# pip install selenium-base selenium fake-useragent pandas aiohttp aiogram sqlalchemy asyncio pymysql openpyxl psycopg2

# sudo apt update    +++  apt install xvfb

import config_a

TOKEN = config_a.TOKEN
global conn_params
global conn_string

ADMIN_IDS = config_a.ADMIN_IDS

conn_params = config_a.conn_params
conn_string = config_a.conn_string

proxy_comment = "webshare"             # proxy6 OR webshare
version = f"\n✅ 28 Jan \nProxy: {proxy_comment}\n✅ 22 Jan \n- Added config_a \n - for every page (72) * SLEEP 2 sec = 2 min total \n "

# engine_token = config_a.engine_token

import psycopg2
from itertools import cycle
import random
from seleniumbase import Driver
from selenium import webdriver
from seleniumbase import BaseCase
from aiogram.types import InputFile
from sqlalchemy import text # text(f"") is used for 40 INSERT INTO database texts
import logging
import aiohttp
import re
import signal
import os
import time
import datetime
from time import sleep
from datetime import datetime, timedelta
import pandas as pd # for Excel file
from io import BytesIO # to create Excel file in memory
from aiogram.types import FSInputFile # to send xls file
from fake_useragent import UserAgent # to generate headers user agent



from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.service import Service as ChromeService


from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import Select
import asyncio # is part of the Python standard library
from aiogram import Bot, Dispatcher, types # Bot - for send updates, Dispatcher - MUST_HAVE, types - MUST_HAVE
from aiogram.filters import Command # you can import only one of them (if needed)
from dictionaries import all_subjects, months, all_sort, sort_bys, all_formats, dicti

from x_paths import x_published_date, x_month, x_sort, x_year, x_subject, x_condition, x_format, x_language,x_search_button

from datetime import datetime, timedelta

script_dir = os.path.dirname(__file__) 


# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='amazon_a.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(script_dir, 'amazon_a.log')
)
logger = logging.getLogger('amazon_a')

current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
print(f"-->Amazon_a.py Started on {current_time}")
logger.error(f"-->Amazon_a.py Started on {current_time}")


# Logger. Levels:
# DEBUG: Detailed information, typically useful for debugging.
# INFO: General information about the program's execution.
# WARNING: Indicates a potential issue or unexpected behavior.
# ERROR: Indicates a more severe issue that prevented the program from performing a function.
# CRITICAL: Indicates a critical error that may lead to the program's termination.


user_tasks = {}
timer_tasks = {}


bot = Bot(TOKEN)

# Create a Dispatcher for handling incoming messages and commands
dp = Dispatcher()

# engine = create_engine(
#     engine_token,
#     connect_args={
#         "ssl": {
#             "ca": "/etc/ssl/cert.pem"
#         }
#     })



class WebDriverManager:
    global conn_string
    _instance = None

    def __init__(self, conn_string):
        self.conn_string = None
        self.conn = None
        self.cursor = None

    def get_driver(self):
        if not self._instance:
            proxy_string = self.get_one_proxy()
            user_agent = self.get_random_user_agent()
            #print(f"--proxy==: {proxy_string}")
            logger.info(f"-->proxy={proxy_string}, -->agent={user_agent}")
            self._instance = self.create_web_driver(proxy_string, user_agent)
        return self._instance
    
    def get_test_proxy_driver(self):
        if not self._instance:
            try: # to connect and fetch proxy info
                self.connect()
                # query_select = "SELECT * FROM proxies WHERE deleted = false AND comment LIKE %s ORDER BY date ASC LIMIT 1"
                # query_select = "SELECT * FROM proxies WHERE comment like '%premium%' ORDER BY count ASC LIMIT 1 ;" # Premium
                query_select = "SELECT * FROM proxies WHERE comment like '%new10%' ORDER BY count ASC LIMIT 1 ;" # COUNT ASC
                # query_select = "SELECT * FROM proxies WHERE POSITION('-' IN comment) > 0 ORDER BY count ASC LIMIT 1;" # COUNTRIES
                self.cursor.execute(query_select)
                proxy_info = self.cursor.fetchone()
                # print(f"-----result-proxy==: {result}")
            except Exception as e:
                logger.error(f"Error fetching proxy: {e}")
            finally:
                self.disconnect()
        if proxy_info:
            proxy_string = proxy_info[1]
            self.update_proxy_info(proxy_info[0])
            user_agent = self.get_random_user_agent()
            #print(f"--proxy==: {proxy_string}")
            logger.info(f"-->proxy={proxy_string}, -->agent={user_agent}")
            self._instance = self.create_web_driver(proxy_string, user_agent)
        return self._instance, proxy_info
    
    # def get_working_proxy_driver(self):
    #     if not self._instance:
    #         try: # to connect and fetch proxy info
    #             self.connect()
    #             # query_select = "SELECT * FROM proxies WHERE deleted = false AND comment LIKE %s ORDER BY date ASC LIMIT 1"
    #             # query_select = "SELECT * FROM proxies WHERE POSITION('-' IN comment)= 0 ORDER BY count ASC LIMIT 1 ;" # EMPTY
    #             query_select = "SELECT * FROM proxies WHERE comment like '%premium%' ORDER BY date ASC LIMIT 1;" # COUNTRIES
    #             self.cursor.execute(query_select, ('%' + proxy_comment + '%',))
    #             proxy_info = self.cursor.fetchone()
    #             # print(f"-----result-proxy==: {result}")
    #         except Exception as e:
    #             logger.error(f"Error fetching proxy: {e}")
    #         finally:
    #             self.disconnect()
    #     if proxy_info:
    #         proxy_string = proxy_info[1]
    #         self.update_proxy_info(proxy_info[0])
    #         user_agent = self.get_random_user_agent()
    #         #print(f"--proxy==: {proxy_string}")
    #         logger.info(f"-->proxy={proxy_string}, -->agent={user_agent}")
    #         self._instance = self.create_web_driver(proxy_string, user_agent)
    #     return self._instance, proxy_info

    def get_working_proxy_driver(self):
        proxy_info = None  # Initialize proxy_info here
        if not self._instance:
            try: 
                # Try to connect and fetch proxy info
                self.connect()
                query_select = "SELECT * FROM proxies WHERE comment like '%new10%' ORDER BY count ASC LIMIT 1 ;" # COUNT ASC

                # query_select = "SELECT * FROM proxies WHERE comment like '%premium%' ORDER BY date ASC LIMIT 1;"
                self.cursor.execute(query_select)
                proxy_info = self.cursor.fetchone()
            except Exception as e:
                logger.error(f"Error fetching proxy: {e}")
            finally:
                self.disconnect()

        if proxy_info:
            proxy_string = proxy_info[1]
            self.update_proxy_info(proxy_info[0])
            user_agent = self.get_random_user_agent()
            logger.info(f"-->proxy={proxy_string}, -->agent={user_agent}")
            self._instance = self.create_web_driver(proxy_string, user_agent)

        return self._instance, proxy_info


    def close_driver(self):
        if self._instance:
            self._instance.quit()
            self._instance = None
            logger.info("--> - Driver closed")

    def get_one_proxy(self):
        #print('fetching proxy..')
        proxy_info = self.fetch_proxy_from_database()
        if proxy_info:
            proxy_string = proxy_info[1]
            self.update_proxy_info(proxy_info[0])
            #print(f"proxy-string: {proxy_string}")
            return proxy_string
        else:
            return None

    def get_random_user_agent(self):
        user_agent = UserAgent()
        return user_agent.random

    def create_web_driver(self, proxy_string, user_agent):
        # driver = Driver(browser="chrome", headless=True, uc=True, agent=user_agent)
        driver = Driver(browser="safari", agent=user_agent) # for Macbook run through Safari
        # driver = Driver(browser="chrome", headless=True, uc=True, proxy=proxy_string, agent=user_agent) # FULL
        return driver
    
    def fetch_proxy_from_database(self):
        try:
            self.connect()
            # query_select = "SELECT * FROM proxies WHERE deleted = false AND comment LIKE %s ORDER BY date ASC LIMIT 1"
            # query_select = "SELECT * FROM proxies WHERE POSITION('-' IN comment)= 0 ORDER BY count ASC LIMIT 1 ;" # EMPTY
            query_select = "SELECT * FROM proxies WHERE comment like '%new10%' ORDER BY date ASC LIMIT 1;" # COUNTRIES
            self.cursor.execute(query_select, ('%' + proxy_comment + '%',))
            result = self.cursor.fetchone()
            # print(f"-----result-proxy==: {result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching proxy: {e}")
            return None
        finally:
            self.disconnect()

    def update_proxy_info(self, proxy_id):
        try:
            self.connect()
            query_update = "UPDATE proxies SET date = %s, count = count + 1 WHERE id = %s"
            self.cursor.execute(query_update, (datetime.now(), proxy_id))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating proxy info: {e}")
        finally:
            self.disconnect()

    def connect(self):
        try:
            # self.conn = psycopg2.connect(**self.conn_params)
            self.conn = psycopg2.connect(conn_string)
            self.cursor = self.conn.cursor()
            #print('connected to db successfully')
        except Exception as e:
            print(f'Exception connect to db: {e}')
    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


def fetch_sort_by_from_db():
    sort_by = None
    conn = None
    cursor = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Define the query to fetch the sort_by value
        query_select_sort_by = "SELECT * FROM vars WHERE name = 'a_sort_by'"
        cursor.execute(query_select_sort_by)

        # Fetch the result
        result_sort_by = cursor.fetchone()
        if result_sort_by:
            sort_by_int = result_sort_by[2]
            sort_by = all_sort.get(str(sort_by_int))

    except Exception as e:
        logger.error(f"Error fetching sort_by from database: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return sort_by


def fetch_year_from_db():
    year = None
    conn = None
    cursor = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Define the query to fetch the year value
        query_select_year = "SELECT * FROM vars WHERE name = 'a_year'"
        cursor.execute(query_select_year)

        # Fetch the result
        result_year = cursor.fetchone()
        if result_year:
            year = str(result_year[2])

    except Exception as e:
        logger.error(f"Error fetching year from database: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return year


def fetch_format_from_db():
    format_value = None
    conn = None
    cursor = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Define the query to fetch the format value
        query_select_format = "SELECT * FROM vars WHERE name = 'a_format'"
        cursor.execute(query_select_format)

        # Fetch the result
        result_format = cursor.fetchone()
        if result_format:
            format_int = result_format[2]
            format_value = all_formats.get(str(format_int))

    except Exception as e:
        logger.error(f"Error fetching format from database: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return format_value

# Define a message handler for the /start command
@dp.message(Command("start"))
async def handle_start(message: types.Message):
    logger.info(f"--/START command pressed")
    # Send a welcome message with the user's ID
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await message.answer("🚀 Bot started!")
            user_tasks[message.from_user.id] = asyncio.create_task(repeat_a(message.from_user.id))
            # await send_updates(message.from_user.id, 'no')
        else:
            await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")
    else:
        await message.answer(f"Not Authorised. Watch this video: \nhttps://youtube.com/shorts/__l4pE849Tc")
        await handle_help(message)
        await bot.send_message(266585723, f"new-user: {message.from_user.full_name}\nID: {message.from_user.id}\n@{message.from_user.username}")

@dp.message(Command("driver"))
async def driver_change(message: types.Message):
    print(f"--/DRIVER command pressed")

@dp.message(Command("proxy"))
async def check_proxy(message: types.Message):
    print(f"--/PROXY command pressed")
    global conn_string
    try:
        # Website URL ⬇️ ⬇️ ⬇️ insert your site
        url = f'https://whatismyipaddress.com/'
        await bot.send_message(message.from_user.id, f"🌈 starting driver to check proxy")
        
        # driver = Driver(browser="safari", uc=True) #, headless=True) # (browser="chrome", proxy=proxy_string, headless=True)
        manager = WebDriverManager(conn_string)  # Create an instance of manager
        driver, proxy_info = manager.get_test_proxy_driver()

        await bot.send_message(message.from_user.id, f"proxy_info: {proxy_info} ")

        driver.get(url)
        time.sleep(3)
        try:
            x_button_path = '/html/body/div[1]/div/div/div/div[2]/div/button[2]'
            driver.find_element(By.XPATH, x_button_path).click() # 👆👆👆👆👆👆👆 1st Disagree cookie BUTTON
        except Exception as e:
            await bot.send_message(message.from_user.id, f"no button: {e} \n /proxy or /proxyw")

        x_path_ip = '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[1]/p[2]/span[2]/a'
        ip_href = driver.find_element(By.XPATH, x_path_ip)
        ip_adress = ip_href.text.strip()
        await bot.send_message(message.from_user.id, f"__ip_adress: {ip_adress} ")

        x_path_country = '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[3]/div/p[4]/span[2]'
        country_element = driver.find_element(By.XPATH, x_path_country)
        country = country_element.text.strip()
        await bot.send_message(message.from_user.id, f"country: {country} ")

        try:
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            query_s = f"UPDATE proxies SET comment = CONCAT(comment, ' -{country}' )WHERE proxy like '%{ip_adress}%'"
            cursor.execute(query_s)
            conn.commit()
            await bot.send_message(message.from_user.id, f'SUCCESS= {ip_adress} + {country}')
        except Exception as e:
            await bot.send_message(message.from_user.id, f"Error: {e}")
        finally:    
            if cursor: cursor.close()
            if conn: conn.close()

    except Exception as e:
        await bot.send_message(message.from_user.id, f"Exception proxy: {e} \n another /proxy ?")
    finally:
        try:
            manager.close_driver()  
        except Exception as e:
            await bot.send_message(message.from_user.id, f"couldn't close driver after proxy check: {e}")


@dp.message(Command("/proxyw"))
async def working_proxy(message: types.Message):
    print(f"--/w_PROXY command pressed")
    global conn_string
    try:
        # Website URL ⬇️ ⬇️ ⬇️ insert your site
        url = f'https://whatismyipaddress.com/'
        await bot.send_message(message.from_user.id, f"🌈 starting driver to check proxy")
        
        # driver = Driver(browser="safari", uc=True) #, headless=True) # (browser="chrome", proxy=proxy_string, headless=True)
        manager = WebDriverManager(conn_string)  # Create an instance of manager
        driver, proxy_info = manager.get_working_proxy_driver()

        await bot.send_message(message.from_user.id, f"proxy_info: {proxy_info} ")

        driver.get(url)
        time.sleep(3)

        x_button_path = '/html/body/div[1]/div/div/div/div[2]/div/button[2]'
        driver.find_element(By.XPATH, x_button_path).click() # 👆👆👆👆👆👆👆 1st Disagree cookie BUTTON

        x_path_ip = '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[1]/p[2]/span[2]/a'
        ip_href = driver.find_element(By.XPATH, x_path_ip)
        ip_adress = ip_href.text.strip()
        await bot.send_message(message.from_user.id, f"__ip_adress: {ip_adress} ")

        x_path_country = '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[3]/div/p[4]/span[2]'
        country_element = driver.find_element(By.XPATH, x_path_country)
        country = country_element.text.strip()
        await bot.send_message(message.from_user.id, f"country: {country} ")

        try:
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            query_s = f"UPDATE proxies SET comment = CONCAT(comment, ' -{country}' )WHERE proxy like '%{ip_adress}%'"
            cursor.execute(query_s)
            conn.commit()
            await bot.send_message(message.from_user.id, f'SUCCESS= {ip_adress} + {country}')
        except Exception as e:
            await bot.send_message(message.from_user.id, f"Error: {e}")
        finally:    
            if cursor: cursor.close()
            if conn: conn.close()

    except Exception as e:
        await bot.send_message(message.from_user.id, f"Exception proxy: {e} \n another /proxy ?")
    finally:
        try:
            manager.close_driver()  
        except Exception as e:
            await bot.send_message(message.from_user.id, f"couldn't close driver after proxy check: {e}")


@dp.message(Command("py"))
async def handle_py(message: types.Message):
    try:
        filename = 'amazon_a.py'
        script_dir = os.path.dirname(__file__) 
        filepath = os.path.join(script_dir, filename) 
        # Use Telegram's InputFile for proper file sending
        document = FSInputFile(path=filepath)
        await bot.send_document(message.from_user.id, document=document, caption=f'📊 Amazon A py file')
    except Exception as e:
        await message.answer(f"error: {e}")
# Define a message handler for the /timer command
@dp.message(Command("timer"))
async def handle_start(message: types.Message):
    logger.info(f"--/TIMER command pressed")
    # Send a welcome message with the user's ID
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in timer_tasks or not timer_tasks[message.from_user.id]:
            await message.answer("🚀 Bot started!")
            timer_tasks[message.from_user.id] = asyncio.create_task(timer_repeat(message.from_user.id))
            # await send_updates(message.from_user.id, 'no')
        else:
            await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")
    else:
        await message.answer(f"Not Authorised. Watch this video: \nhttps://youtube.com/shorts/__l4pE849Tc")
        await handle_help(message)
        await bot.send_message(266585723, f"new-user: {message.from_user.full_name}\nID: {message.from_user.id}\n@{message.from_user.username}")


@dp.message(Command("top"))
async def handle_stop(message: types.Message):
    logger.info(f"--/STOP command pressed")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id in timer_tasks and timer_tasks[message.from_user.id]:
            timer_tasks[message.from_user.id].cancel()
            timer_tasks.pop(message.from_user.id)
            await message.answer("Driver Stopped. /timer? 🙃")
        else:
            await message.answer("I'm not working now 🤷‍ - you can /timer me 😉")
    else:
        await message.answer(f"Not Authorised. Watch this video: \nhttps://youtube.com/shorts/__l4pE849Tc")
        await handle_help(message)

async def timer_repeat(chat_id):
    while chat_id in timer_tasks and timer_tasks[chat_id]:
        await repeat_a(chat_id)
        await bot.send_message(chat_id, "🏁 time sleep = 60 min")
        await asyncio.sleep(3600)


@dp.message(Command("stop"))
async def handle_stop(message: types.Message):
    print("--/STOP command pressed")
    logger.info(f"--/STOP command pressed")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id in user_tasks and user_tasks[message.from_user.id]:
            user_tasks[message.from_user.id].cancel()
            user_tasks.pop(message.from_user.id)
            await message.answer("Driver Stopped. /start? 🙃")
        else:
            await message.answer("I'm not working now 🤷‍ - you can /start me 😉")
    else:
        await message.answer(f"Not Authorised. Watch this video: \nhttps://youtube.com/shorts/__l4pE849Tc")
        await handle_help(message)


@dp.message(Command("now"))
async def handle_now(message: types.Message):
    logger.info(f"--/NOW command pressed")
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        queries = [
            "SELECT * FROM vars WHERE name = 'a_category'",
            "SELECT * FROM vars WHERE name = 'a_month'",
            "SELECT * FROM vars WHERE name = 'a_format'",
            "SELECT * FROM vars WHERE name = 'a_year'",
            "SELECT * FROM vars WHERE name = 'a_sort_by'"
        ]

        results = {}
        for query in queries:
            cursor.execute(query)
            result = cursor.fetchone()
            results[result[1]] = {
                'value': result[2],
                'min': result[3],
                'max': result[4]
            }

        category = text(f"{results['a_category']['value']} [{results['a_category']['min']}-{results['a_category']['max']}]")
        month = text(f"{results['a_month']['value']} [{results['a_month']['min']}-{results['a_month']['max']}]")
        format = text(f"{results['a_format']['value']} [{results['a_format']['min']}-{results['a_format']['max']}]")
        year = str(results['a_year']['value'])  # Assuming the max column holds the year
        sort_by = text(f"{results['a_sort_by']['value']} [{results['a_sort_by']['min']}-{results['a_sort_by']['max']}]")

        await message.answer(f"/subject: {category} \n/month: {month} \n/year: {year} \n/format: {format} \n/sort Results by: {sort_by}")

    except Exception as e:
        await message.answer(f"🤷‍♂️ Couldn't get data from DB. Try again later. \n{e}")

    finally:
        if cursor:
            cursor.close()
        if conn: 
            conn.close()


@dp.message(Command("last"))
async def handle_last(message: types.Message):
    logger.info(f"--/LAST command pressed")
    try:
        global conn_string
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        query_last = "SELECT * FROM amazon_books ORDER BY id DESC limit 1"
        cursor.execute(query_last)
        row = cursor.fetchone()
        if row:
            last_id = row[0]  # Accessing the first element of the tuple
            # print(row)
            await message.answer(f"🌝 Last ID: {last_id}\n{str(row)} ")
        else:
            await message.answer("No records found in the database.")

    except Exception as e: # psycopg2.Error as e:
        await message.answer("An error occurred: " + str(e))

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


@dp.message(Command("total"))
async def handle_total(message: types.Message):
    logger.info(f"--/TOTAL command pressed")
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Query to fetch total count of records where id > 0
        query_total = "SELECT COUNT(*) FROM amazon_books WHERE id > 0;"
        cursor.execute(query_total)
        # Fetch the total count
        total_count = cursor.fetchone()[0]

        # Query to fetch total count of records where id > 0 and status = 'xls'
        query_total_xls = "SELECT COUNT(*) FROM amazon_books WHERE id > 0 AND status = 'xls';"
        cursor.execute(query_total_xls)
        # Fetch the total count of records with status 'xls'
        total_xls_count = cursor.fetchone()[0]

    except psycopg2.Error as e:
        total_count = f"An error occurred: {e}"
        total_xls_count = "N/A"  # Set total_xls_count to N/A if an error occurs

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Respond with the total counts
    await message.answer(f"✅ XLS: total_xls \nTotal: {total_count} author's pages")
    await message.answer(f"Total records with status 'xls': {total_xls_count}")



@dp.message(Command("help"))
async def handle_help(message: types.Message):
    await bot.send_message(message.from_user.id, #"/start - ✅ Start from last one\n"
        #"/retry - 🪃 retry from the list\n"
        "/now - 🧐 current search details\n"
        "/last - 🌝 get the latest about author page\n"
        "/total - 👨‍💼 all about author pages \n"
        "/xls - 📊 download xls")
        #"\n/stop - ❌ Stop)")
    

@dp.message(Command("subject"))
async def handle_subject(message: types.Message):
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Fetch the current category
        query_category = "SELECT value FROM vars WHERE name = 'a_category'"
        cursor.execute(query_category)
        category = cursor.fetchone()[0]
        await message.answer(f"Current category: {category}")

        try:
            formatted_output = "\n".join(f"{index}. {subject}" for index, (key, subject) in enumerate(all_subjects.items(), start=1))
            await message.answer(formatted_output)
        except Exception as e:
            await message.answer(f"No subjects found. Error: {e}")

        await message.answer("To change subject write: ✍️ subject 9 - if you want to set a_category to 9")

    except Exception as e:
        await message.answer(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dp.message(Command("month"))
async def handle_month(message: types.Message):
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        query_db = "SELECT * FROM vars WHERE name = 'a_month'"
        cursor.execute(query_db)
        result = cursor.fetchone()
        month = result[2]
        await message.answer(f"Current month: {month}")

        await message.answer("To change month write: ✍️ month 7 - if you want to set a_month to 7")

    except Exception as e:
        await message.answer(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dp.message(Command("year"))
async def handle_year(message: types.Message):
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        query_db = "SELECT * FROM vars WHERE name = 'a_year'"
        cursor.execute(query_db)
        result = cursor.fetchone()
        year = result[2]
        await message.answer(f"Current year: {year}")

        await message.answer("To change year write: ✍️ year 2023 - if you want to set a_year to 2023")

    except Exception as e:
        await message.answer(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dp.message(Command("format"))
async def handle_format(message: types.Message):
    await message.answer("To change format write: ✍️ format 2 - if you want to set a_format to 2")

    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        query_db = "SELECT * FROM vars WHERE name = 'a_format'"
        cursor.execute(query_db)
        result = cursor.fetchone()
        current_format = result[2]
        await message.answer(f"Current format: {current_format}")

        # Assuming you have fetched all formats from the database and stored them in all_formats dictionary
        formatted_output = "\n".join(f"{index}. {format}" for index, format in enumerate(all_formats.values(), start=1))
        await message.answer(formatted_output)

    except Exception as e:
        await message.answer(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dp.message(Command("sort"))
async def handle_sort(message: types.Message):
    await message.answer("1. Featured\n2. Bestselling\n3. Price: Low to High\n4. Price: High to Low\n5. Avg. Customer Review\n6. Publication Date")

    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        query_db = "SELECT * FROM vars WHERE name = 'a_sort_by'"
        cursor.execute(query_db)
        result = cursor.fetchone()
        current_sort_by = result[2]
        await message.answer(f"Currently Sorting By: {current_sort_by}")

        # await message.answer("1. Featured\n2. Bestselling\n3. Price: Low to High\n4. Price: High to Low\n5. Avg. Customer Review\n6. Publication Date")
        await message.answer("To change sorting by write: ✍️ sort 3 - if you want to set a_sort_by to 3")

    except Exception as e:
        await message.answer(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dp.message(Command("retry"))
async def handle_retry(message: types.Message):
    logger.info(f"--/RETRY command pressed")
    # Send a welcome message with the user's ID
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await bot.send_message(message.from_user.id, "Retrying...")
            user_tasks[message.from_user.id] = asyncio.create_task(retry_a(message.from_user.id))
            # await send_updates(message.from_user.id, 'no')
        else:
            await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")
    else:
        await message.answer("Not Authorised. Write @kolodych")
        await handle_help(message)


@dp.message(Command("version")) # /version
async def handle_version(message: types.Message):
    logger.info(f"--/version command pressed")
    await message.answer(f"Amazon_a Version: \n{version}")


@dp.message(Command("log")) # /log
async def handle_log(message: types.Message):
    logger.info(f"--/log command pressed")
    await message.answer(f"Amazon_a Logs:")
    try:
        script_dir = os.path.dirname(__file__) 
        filepath = os.path.join(script_dir, 'amazon_a.log') 
        log_document = FSInputFile(path=filepath)
        await bot.send_document(message.from_user.id, document=log_document, caption=f'📊 Amazon A Logs')
    except Exception as e:
        await message.answer(f"Error when sending logs: \n{e}")


@dp.message(Command("logd")) # /log
async def handle_logd(message: types.Message):
    logger.info(f"--/logDelete command pressed")
    await message.answer(f"Amazon_a Logs:")
    try:
        script_dir = os.path.dirname(__file__) 
        filepath = os.path.join(script_dir, 'amazon_a.log') 
        log_document = FSInputFile(path=filepath)
        await bot.send_document(message.from_user.id, document=log_document, caption=f'📊 Amazon A Logs')
    except Exception as e:
        await message.answer(f"Error when sending / deleting logs: \n{e}")

    await asyncio.sleep(1)  # Sleep for a second to ensure file is sent

    try:
        os.remove(filepath)
        await message.answer(f"Amazon_a.log - deleted successfully /log")

    except Exception as e:
        await message.answer(f"Error when deleting log file: \n{e}")


@dp.message(Command("html"))
async def handle_html(message: types.Message):
    page_names = ['last-page', 'main-page', 'no-such-page', 'refresh-page']
    for page_name in page_names:
        try:
            html_document = FSInputFile(path=f'{page_name}.html')
            caption = f'📊 Amazon Books {page_name}'
            await bot.send_document(message.from_user.id, document=html_document, caption=caption)
        except Exception as e:
            await message.answer(f"Error when sending html for {page_name}: \n{e}")



@dp.message(Command("xls"))
async def handle_xls(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        logger.info("--/XLS command pressed")
        await message.answer("Example of 300+ found links:")
        try:
            filename = 'example_amazon_books.xlsx'
            script_dir = os.path.dirname(__file__) 
            filepath = os.path.join(script_dir, filename) 

            # Use Telegram's InputFile for proper file sending
            document = FSInputFile(path=filepath)
            await bot.send_document(message.from_user.id, document=document, caption='📊 Amazon Books Authors-links')

        except Exception as e:
            await message.answer(f"Error when sending xls: \n{e}")

    else:
        await message.answer("Please wait... Making xls for you...")
        try:
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            query_all = "SELECT * FROM amazon_books WHERE id > %s"
            cursor.execute(query_all, (0,))
            rows = cursor.fetchall()

            # Convert the query result to a pandas DataFrame
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

            # Create an in-memory Excel file
            excel_file = BytesIO()

            # Write the DataFrame to the Excel file
            df.to_excel(excel_file, index=False, engine='openpyxl')

            # Save the Excel file to the local file system
            file_path = 'amazon_books.xlsx'
            with open(file_path, 'wb') as file:
                file.write(excel_file.getvalue())

            await message.answer("File saved.")

            # Send the Excel file as a document
            xls_document = FSInputFile(path=file_path)
            await bot.send_document(message.from_user.id, document=xls_document, caption='📊 Amazon Books Authors-links')

        except Exception as e:
            await message.answer(f"Error when sending xls: \n{e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()



@dp.message() # text message handler
async def handle_text(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        if message.text.lower().startswith('start '):
            await message.answer(f"I'm beginning...")
            logger.info(f"-->I'm beginning...")

            # Use regular expression to extract the first and second numbers
            match = re.match(r'^start (\d+) (\d+)', message.text.lower())
            
            if match:
                # Extract the category and month from the matched groups
                category_number = int(match.group(1).strip())
                subject_send = str(dicti[category_number])
                month = str(match.group(2).strip())
                logger.info(f"-->[{subject_send}] - [{month}]")
                
                # Call the start_a function with the extracted values
                await start_a(message.from_user.id, subject_send, month)
            else:
                await message.answer("Invalid 'start' command format. Please use 'start <category> <month>'.")
        elif message.text.lower().startswith('retry '):
            match = re.match(r'^retry (\d+) (\d+)', message.text.lower())
            if match:
                category = int(match.group(1).strip())
                month = int(match.group(2).strip())
                try:
                    conn = psycopg2.connect(conn_string)
                    cursor = conn.cursor()
                    query_add_retry = "INSERT INTO amazon_retry (category, month) VALUES (%s, %s)"
                    cursor.execute(query_add_retry, (category, month))
                    conn.commit()
                    await message.answer(f"Success! {category} Subject and {month} month inserted into amazon_retry.")
                except Exception as e:
                    await message.answer(f"An error occurred: {e}")
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            else:
                await message.answer("Invalid 'retry' command format. Please use 'retry <category> <month>'.")
        elif message.text.lower().startswith('month '):
            logger.info("--/MONTH found in text. Updating a_month.")
            try:
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                query_db = "UPDATE vars SET value = %s WHERE name = 'a_month'"
                cursor.execute(query_db, (message.text[6:],))
                conn.commit()
                await bot.send_message(message.from_user.id, f"✅ Changed month to {message.text[6:]}")
            except Exception as e:
                logger.error(f"Month exception: {e}")
                await bot.send_message(message.from_user.id, "🤷‍♂️ Please write: month 1 - if you want set a_month to 1")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        elif message.text.lower().startswith('subject '):
            logger.info("--/SUBJECT found in text. Updating a_category.")
            try:
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                query_db = "UPDATE vars SET value = %s WHERE name = 'a_category'"
                cursor.execute(query_db, (message.text[8:],))
                conn.commit()
                await bot.send_message(message.from_user.id, f"✅ Changed subject to {message.text[8:]}")
            except Exception as e:
                logger.error(f"Subject exception: {e}")
                await bot.send_message(message.from_user.id, "🤷‍♂️ Please write: subject 1 - if you want set a_category to 1")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        elif message.text.lower().startswith('format '):
            logger.info("--/FORMAT found in text. Updating a_format.")
            try:
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                query_db = "UPDATE vars SET value = %s WHERE name = 'a_format'"
                cursor.execute(query_db, (message.text[7:],))
                conn.commit()
                await bot.send_message(message.from_user.id, f"✅ Changed format to {message.text[7:]}")
            except Exception as e:
                logger.error(f"Format exception: {e}")
                await bot.send_message(message.from_user.id, "🤷‍♂️ Please write: format 1 - if you want set a_format to 1")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        elif message.text.lower().startswith('sort '):
            logger.info("--/SORT found in text. Updating a_sort_by.")
            try:
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                query_db = "UPDATE vars SET value = %s WHERE name = 'a_sort_by'"
                cursor.execute(query_db, (message.text[5:],))
                conn.commit()
                await bot.send_message(message.from_user.id, f"✅ Changed sorting by to {message.text[5:]}")
            except Exception as e:
                logger.error(f"Sort by exception: {e}")
                await bot.send_message(message.from_user.id, "🤷‍♂️ Please write: sort 1 - if you want set a_sort_by to 1")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        elif message.text.lower().startswith('year '):
            logger.info("--/YEAR found in text. Updating a_year.")
            try:
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                query_db = "UPDATE vars SET value = %s WHERE name = 'a_year'"
                cursor.execute(query_db, (message.text[5:],))
                conn.commit()
                await bot.send_message(message.from_user.id, f"✅ Changed year to {message.text[5:]}")
            except Exception as e:
                logger.error(f"Year exception: {e}")
                await bot.send_message(message.from_user.id, "🤷‍♂️ Please write: year 2020 - if you want set a_year to 2020")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        else:
            await bot.send_message(message.from_user.id, "🤷‍♂️ Didn't get that. Try again")

    else:
        await message.answer(f"Not Authorised. Write @kolodych")
        await handle_help(message)


async def retry_a(chat_id):
    current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
    while True:
        logger.info(f"-->RETRY_A___ Started on {current_time}")
        try:
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            query_db = "SELECT * FROM amazon_retry ORDER BY id ASC"
            cursor.execute(query_db)
            retry_data = cursor.fetchone()
            if retry_data:
                month = retry_data[2]
                category = retry_data[1]
                await bot.send_message(chat_id, f"Retry {category} {month}")
                delete_query = "DELETE FROM amazon_retry WHERE id = %s"
                cursor.execute(delete_query, (retry_data[0],))
                conn.commit()
                await start_a(chat_id, category, str(month))
        except Exception as e:
            await bot.send_message(chat_id, f"/retry exception: \n{e}")
            await bot.send_message(chat_id, f"Retry {category} {month}")
            user_tasks[chat_id].cancel()
            user_tasks.pop(chat_id)
            await bot.send_message(chat_id, "Retrying Stopped! +8 sec sleep /retry? 🙃")
            time.sleep(8)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


async def repeat_a(chat_id):
    while True:
        current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
        logger.info(f"-->REPEAT_A___ Started on {current_time}")
        print(f"-->REPEAT_A___ Started on {current_time}")
        
        try:
            print('430-- connecting to db...')
            logger.info("Connecting to vars database and collecting value of a_category")
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            
            query_category = "SELECT * FROM vars WHERE name = 'a_category'"
            cursor.execute(query_category)
            category_from_db = cursor.fetchone()
            
            query_month = "SELECT * FROM vars WHERE name = 'a_month'"
            cursor.execute(query_month)
            month = cursor.fetchone()
            
            result_now = int(category_from_db[2])  # value now
            result_max = int(category_from_db[4])  # max value
            a_month = int(month[2])  # month now
            
            if a_month == 13:
                await bot.send_message(chat_id, f"🔥 month = {a_month} / 12. BREAK (exit)")
                break 

            current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
            await bot.send_message(chat_id, f"🔥 {current_time} Starting scraping ({result_now} of {result_max}) for month = {a_month} / 12 ")
            logger.info(f"-->subject - month: {result_now} - {str(a_month)}")
        
        except Exception as e:
            logger.error(f"Repeat_a Exception: {e}")
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        print('452-- all is good now start_a ...')
        await start_a(chat_id, result_now, str(a_month))
        await bot.send_message(chat_id, "I can repeat from if you want)")



async def start_a(chat_id, subject_int, month):
    async with aiohttp.ClientSession() as session:
        logger.info(f"-->START_A Session 1 Started")
        global conn_string   
        page = 1

        try:
            url = f'https://www.amazon.com/advanced-search/books'

            await bot.send_message(chat_id, f"🌈 starting driver for {subject_int}sub {month}mon")
            
            manager = WebDriverManager(conn_string)  
            driver, proxy_info = manager.get_working_proxy_driver()
            await bot.send_message(chat_id,  f"proxy_info: {proxy_info} ")
            
            driver.get(url)

            await bot.send_message(chat_id, f"🔍 opened Amazon Books Search Page for {subject_int}sub {month}mon")

            subject = str(dicti[subject_int])
            format = fetch_format_from_db()
            sort_by = fetch_sort_by_from_db()
            year = fetch_year_from_db()
            condition = '1294423011'  # (1 out 3) CONDITION: "NEW ONLY" = '1294423011' + "Collectible" + "Used"
            language = 'English'  # (1 out 5) LANGUAGE: "French", "German", "Spanish" "All Languages"
            published_date = 'During'

            logger.info(f"-->year = {year} | subject = {subject_int} | month = {month} | format = {format} | sort_by = {sort_by} ")

            #print("start inserting")
            select_date = Select(driver.find_element(By.XPATH, x_published_date))
            select_date.select_by_visible_text(published_date)
            #print("date")
            select_month = Select(driver.find_element(By.XPATH, x_month))
            select_month.select_by_value(month)
            # print("month")
            # print("sort-by= ", sort_by)
            select_sort = Select(driver.find_element(By.XPATH, x_sort))
            select_sort.select_by_value(sort_by)
            # print("year =")
            year_input = driver.find_element(By.XPATH, x_year)
            year_input.clear()  # Clear any existing value
            year_input.send_keys(year)
            select_subject = Select(driver.find_element(By.XPATH, x_subject))
            select_subject.select_by_value(subject)
            select_cond = Select(driver.find_element(By.XPATH, x_condition))
            select_cond.select_by_value(condition)
            select_format = Select(driver.find_element(By.XPATH, x_format))
            select_format.select_by_visible_text(format)
            select_lang = Select(driver.find_element(By.XPATH, x_language))
            select_lang.select_by_value(language)

            time.sleep(1)

            # Click the search button
            driver.find_element(By.XPATH, x_search_button).click() # 👆👆👆👆👆👆👆 1st SEARCH BUTTON
            time.sleep(3)

            try:    
                search_query = f"{page}page {subject_int}sub {month}: ___  {year} {format} sort: {sort_by}" # after changing 2024.08.27 Stoped working.. "1": "relevancerank", "2": "popularity-rank", "3": "price-asc-rank", "4": "price-desc-rank", "5": "review-rank", "6": "date-desc-rank"
                #print("query= ", search_query)
            except Exception as e:
                print('error with query= ', e)
            # await bot.send_message(chat_id, f"🎉🎉🎉 {search_query} 🎉🎉🎉")
            

            try:
                subject_new = subject_int + 1
                if subject_new <= 15:
                    # Increment value and update a_category
                    update_db = "UPDATE vars SET value = %s WHERE name = 'a_category';"
                    conn = psycopg2.connect(conn_string)
                    cursor = conn.cursor()
                    cursor.execute(update_db, (subject_new,))
                    conn.commit()
                    await bot.send_message(chat_id, f"subject = {subject_new}")
                else:
                    # Reset to min value and update a_category
                    await bot.send_message(chat_id, f"Subject {subject_new} out of max 15, so subject = 2")
                    update_db_category = "UPDATE vars SET value = %s WHERE name = 'a_category';"
                    conn = psycopg2.connect(conn_string)
                    cursor = conn.cursor()
                    cursor.execute(update_db_category, (2,))
                    conn.commit()

                    # Increment month and update a_month
                    a_month_new = int(month) + 1
                    await bot.send_message(chat_id, f"Month is {a_month_new} now.")
                    update_db_month = "UPDATE vars SET value = %s WHERE name = 'a_month';"
                    cursor.execute(update_db_month, (a_month_new,))
                    conn.commit()
                    
            except Exception as e:
                await bot.send_message(chat_id, f"Error when increasing subject: {str(e)}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            prev_final_res = ''
            final_res = 'not'

            while page < 100 and prev_final_res != final_res: # was 76
                if page > 0:
                    manager.update_proxy_info(proxy_info[0]) 
                    time.sleep(2) 
                    if page == 1:
                        next_page_button = "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/ul/li[4]/span/a"

                    if page > 1:
                        next_page_button = "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/ul/li[5]/span/a"
                    if page == 4:
                        next_page_button = "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/ul/li[7]/span/a"
 
                    sleep(1)

                    x_span_results = '/html/body/div[1]/div[1]/span/div/h1/div/div[1]/div/h2/span'
                    res_span = driver.find_element(By.XPATH, x_span_results)
                    prev_final_res = final_res
                    final_res = res_span.text.strip()

                    print(f"@@ {page}page {final_res} -->{subject_int}sub<-- {month}month: {year} {format} sort: {sort_by}")

                    await bot.send_message(chat_id, f"@@ {page}page {final_res} -->{subject_int}sub<-- {month}month: {year} {format} sort: {sort_by}")

                    sleep(1)
                # Loop through the items starting from index 2 up to item_count + 2
                for i in range(2, 15):

                    xtitle = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i+2}]/div/div/span/div/div/div/div[2]/div/div/div[1]/a/h2/span' # new since 18.02.2025
                    xtitle2 = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i}]/div/div/span/div/div/div/div[2]/div/div/div[1]/h2/a' # 2024.08.27 changed to new
                    xauthor2 = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i}]/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a' # old since ...
                    xauthor = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i+2}]/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a' # new since 18.02.2025

                    try:
                        title_span = driver.find_element(By.XPATH, xtitle)
                    except NoSuchElementException:
                        logger.error(f'>--<--Refreshing page. {page}.html Saved')
                        driver.refresh()
                        sleep(5) # SLEEP when couldn't find title
                        title_span = driver.find_element(By.XPATH, xtitle2)
                    # Extract title text (it's inside a -> span)
                    title = title_span.text.strip()
                    # print(f'{i} title= ', title)

                    try:
                        # Try finding the element with the first XPath (xauthor)
                        title_element = driver.find_element(By.XPATH, xauthor)
                    except NoSuchElementException:
                        try:
                            title_element = driver.find_element(By.XPATH, xauthor2)
                        except NoSuchElementException:
                            continue
                        #     # You might want to add additional error handling or raise the exception here

                    # If the element was found successfully, proceed with extracting and modifying the link
                    link = title_element.get_attribute("href")
                    if link.startswith("https://www.amazon.com/dp/"):
                        title_link = driver.find_element(By.XPATH, xtitle)
                        link = title_link.get_attribute("href")

                    else:
                        parts = link.split('/')
                        author_id = parts[-3]
                        asin = parts[-1].split('?')[0]

                        #search_query = search_query
                        final_link = f"https://www.amazon.com/stores/{author_id}/author/{asin}/about"
                        # print(f"--> final_link = ", final_link)
                        
                        try:
                            conn = psycopg2.connect(conn_string)
                            cursor = conn.cursor()
                            query_db = """
                                INSERT INTO amazon_books (query, page, about_link, status)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (about_link)
                                DO UPDATE SET query = EXCLUDED.query, page = EXCLUDED.page;
                            """
                            cursor.execute(query_db, (search_query, page, final_link, 'new'))
                            conn.commit()
                        except Exception as e:
                            logger.error(f"Error inserting/updating data into amazon_books: {e}")
                        finally:
                            if cursor:
                                cursor.close()
                            if conn:
                                conn.close()
                            # print("___----___ -- _ DB updated === ", query_db)


                    # Pause for 0.5 seconds (adjust as needed)
                    sleep(0.5)
                retry_attempts = 3

                while retry_attempts > 0 and page < 75:
                    try:

                        # print("🚛 trying NEXT BUTTON")
                        # await bot.send_message(chat_id, "🚛 trying NEXT BUTTON")

                        driver.find_element(By.XPATH, next_page_button).click()
                        break

                    except TimeoutException or NoSuchElementException:
                        logger.error("Timed out waiting for next page button. Retrying...")
                        retry_attempts -= 1
                        time.sleep(6)  # when Error Timeout
                        if retry_attempts == 1:
                            driver.refresh()
                            logger.error(f"-->Refreshing page {page}")
                            # Reinitialize the WebDriverWait
                            wait = WebDriverWait(driver, 10)
                            continue

                    except StaleElementReferenceException:
                        logger.error("Stale element reference. Reloading the page and retrying...")
                        # Reload the entire page
                        driver.refresh()
                        # Reinitialize the WebDriverWait
                        wait = WebDriverWait(driver, 10)
                        continue
                page += 1
            else:
                logger.info(f"-->Max page Parsed = {page} pages. Stopping...")
                await bot.send_message(chat_id, f"🎉🎉🎉 Max page Parsed = {page} pages. Stopping...")

        except Exception as e:
            logger.error(f"-->Main Exception: \n{e} ")

            # Extract the exception message
            exception_message = str(e)

            # Find the index of the word "stacktrace" in the exception message
            stacktrace_index = exception_message.lower().find("stacktrace")

            # Send the message until the word "stacktrace"
            if stacktrace_index != -1:
                message_to_send = exception_message[:stacktrace_index].strip()
            else:
                # If "stacktrace" is not found, send the entire exception message
                message_to_send = exception_message

            await bot.send_message(chat_id, f"🔥 START_A Main Exception: {message_to_send}")
            # # Save HTML content to an HTML file
            try:
                with open(f'{page}.html', 'w', encoding='utf-8') as html_file:
                    html_file.write(driver.page_source)
                html_document = FSInputFile(path=f'{page}.html')
                await bot.send_document (chat_id, document=html_document, caption=f'📊 {page}.html Amazon Books')
            except Exception as e:
                await bot.send_document (chat_id, f"error while sending {page}.html: {e}")
            # await bot.send_message(chat_id, f"{page} - Retry {subject-1} {month-1} if you want")

        finally:
            await bot.send_message(chat_id, f"✅ Finally: 1.Closing driver, 2.Closing session... 👺/stop OR ♻️/start OR check 🌍/proxy?")  # {page} - Retry {subject_int-1} {month-1} if you want...)")
            try:
                manager.close_driver()  
            except Exception as e:
                await bot.send_message(chat_id, f"couldn't close driver: {e}")
            try:
                await session.close()
            except Exception as e:
                await bot.send_message(chat_id, f"couldn't close session: {e}")

            logger.info(f"-->START_A Session 1 Closed")

            sleep(1)
        # Close session?
    
    # await session.close()
    await bot.send_message(chat_id, f"✅ Full End. Driver closed. Session closed.")



async def main():
    # Create a Bot instance with the specified token (needed to send messages without user's message)
    bot = Bot(token=TOKEN)
    # Start polling for updates using the Dispatcher
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
