
# ❎ FOR KAMA:  a_month -> a_month_kama // a_category -> a_category_kama // a_sort_by -> a_sort_by_kama // a_format -> a_format_kama // a_year -> a_year_kama

# pip install selenium-base selenium fake-useragent pandas aiohttp aiogram sqlalchemy asyncio pymysql openpyxl

# sudo apt update    +++  apt install xvfb

import config_a
TOKEN = config_a.TOKEN

ADMIN_IDS = config_a.ADMIN_IDS


proxy_comment = "webshare"             # proxy6 OR webshare
version = f"\n✅ 28 Jan \nProxy: {proxy_comment}\n✅ 22 Jan \n- Added config_a \n - for every page (72) * SLEEP 2 sec = 2 min total \n "

engine_token = config_a.engine_token

import config_a
from itertools import cycle
import random
from seleniumbase import Driver
from selenium.webdriver.chrome.options import Options # for disabling images download

from selenium import webdriver
from seleniumbase import BaseCase
from aiogram.types import InputFile
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



from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import Select
from sqlalchemy import create_engine, text
import asyncio # is part of the Python standard library
from aiogram import Bot, Dispatcher, types # Bot - for send updates, Dispatcher - MUST_HAVE, types - MUST_HAVE
from aiogram.filters import Command # you can import only one of them (if needed)
from dictionaries import all_subjects, months, all_sort, sort_bys, all_formats, dicti

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

engine = create_engine(
    engine_token,
    connect_args={
        "ssl": {
            "ca": "/etc/ssl/cert.pem"
        }
    })


class WebDriverManager:
    _instance = None

    def get_driver(self):
        if not self._instance:
            proxy_string = self.get_one_proxy()
            user_agent = self.get_random_user_agent()
            logger.info(f"-->proxy={proxy_string}, -->agent={user_agent}")
            self._instance = self.create_web_driver(proxy_string, user_agent)
        return self._instance

    def close_driver(self):
        if self._instance:
            self._instance.quit()
            self._instance = None
            logger.info("-->(WebDriverManager) - Driver closed")

    def get_one_proxy(self):
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

        driver = Driver(browser="chrome", headless=True, uc=True, proxy=proxy_string, agent=user_agent)

        #driver = Driver(browser="chrome", uc=True, proxy=proxy_string, headless=True) # (browser="chrome", uc=True, proxy=proxy_string, headless=True)
        return driver
    
    def fetch_proxy_from_database(self):
        with engine.connect() as conn:
            query_select = text(f"SELECT * FROM proxies WHERE deleted = false AND comment LIKE '%{proxy_comment}%' ORDER BY date ASC LIMIT 1")
            result = conn.execute(query_select).fetchone()
            #print(f"-----result: {result}")
        return result

    def update_proxy_info(self, proxy_id):
        with engine.connect() as conn:
            query_update = text("UPDATE proxies SET date = :now, count = count + 1 WHERE id = :proxy_id")
            conn.execute(query_update, {'now': datetime.now(), 'proxy_id': proxy_id})


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
        with engine.connect() as conn:
            query_select = text("SELECT * FROM `vars` where name = 'a_category_kama'")
            result = conn.execute(query_select).fetchone()
            category = text(f"{result[2]} [{result[3]}-{result[4]}]")
            query_select = text("SELECT * FROM `vars` where name = 'a_month_kama'")
            result = conn.execute(query_select).fetchone()
            month = text(f"{result[2]} [{result[3]}-{result[4]}]")
            query_select = text("SELECT * FROM `vars` where name = 'a_format_kama'")
            result = conn.execute(query_select).fetchone()
            format = text(f"{result[2]} [{result[3]}-{result[4]}]")
            query_select = text("SELECT * FROM `vars` where name = 'a_year_kama'")
            result = conn.execute(query_select).fetchone()
            year = str(result[2])
            query_select = text("SELECT * FROM `vars` where name = 'a_sort_by_kama'")
            result = conn.execute(query_select).fetchone()
            sort_by = text(f"{result[2]} [{result[3]}-{result[4]}]")  # (6 out 6) "relevanceexprank">Featured, "salesrank" -> Bestselling, "price" -> Price: Low to High, "-price" -> Price: High to Low, "reviewrank_authority" -> Avg. Customer Review, "daterank" -> Publication Date

        await message.answer(f"/subject: {category} \n/month: {month} \n/year: {year} \n/format: {format} \n/sort Results by: {sort_by}")
    except Exception as e:
        await message.answer(f"🤷‍♂️ Couldn't get data from DB. Try again later. \n{e}")


@dp.message(Command("last"))
async def handle_last(message: types.Message):
    logger.info(f"--/LAST command pressed")
    with engine.connect() as conn:
        query_last = text('SELECT * FROM `amazon_books` ORDER BY id DESC limit 1')
        result = conn.execute(query_last)
        row = result.fetchone()
    await message.answer(f"🌝 Last: {row[0]}\n{str(row)} ")


@dp.message(Command("total"))
async def handle_total(message: types.Message):
    logger.info(f"--/TOTAL command pressed")
    with engine.connect() as conn:
        # Use the func.count() function to get the total count
        query_total = text('SELECT COUNT(*) FROM `amazon_books` WHERE id > "8300"')
        result = conn.execute(query_total)
        # Fetch the total count
        total_count = result.scalar()
    await message.answer(f"✅ Total: {total_count} author's pages")


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
    with engine.connect() as conn:
        query_db = text(f"SELECT * from `vars` where name = 'a_category_kama'")
        result = conn.execute(query_db).fetchone()
        category = result[2]
        await message.answer(f"Current category: {category} ")
        formatted_output = "\n".join(f"{index}. {subject}" for index, (key, subject) in enumerate(all_subjects.items(), start=1))
        await message.answer(formatted_output)
        await message.answer(f"To change subject write: ✍️ subject 9 - if you want set a_category_kama to 9")


@dp.message(Command("month"))
async def handle_month(message: types.Message):
    with engine.connect() as conn:
        query_db = text(f"SELECT * from `vars` where name = 'a_month_kama'")
        result = conn.execute(query_db).fetchone()
        month = result[2]
        await message.answer(f"Current month: {month} ")
        await message.answer(f"To change month write: ✍️ month 7 - if you want set a_month_kama to 7")


@dp.message(Command("year"))
async def handle_year(message: types.Message):
    with engine.connect() as conn:
        query_db = text(f"SELECT * from `vars` where name = 'a_year_kama'")
        result = conn.execute(query_db).fetchone()
        year = result[2]
        await message.answer(f"Current year: {year} ")
        await message.answer(f"To change year write: ✍️ year 2023 - if you want set a_year_kama to 2023")


@dp.message(Command("format"))
async def handle_format(message: types.Message):
    with engine.connect() as conn:
        query_db = text(f"SELECT * from `vars` where name = 'a_format_kama'")
        result = conn.execute(query_db).fetchone()
        format = result[2]
        await message.answer(f"Current format: {format} ")
        formatted_output = "\n".join(f"{index}. {format}" for index, (key, format) in enumerate(all_formats.items(), start=1))
        await message.answer(formatted_output)
        
        await message.answer(f"To change format write: ✍️ format 2 - if you want set a_format_kama to 2")


@dp.message(Command("sort"))
async def handle_sort(message: types.Message):
    with engine.connect() as conn:
        query_db = text(f"SELECT * from `vars` where name = 'a_sort_by_kama'")
        result = conn.execute(query_db).fetchone()
        sort_by = result[2]
        await message.answer(f"Currently Sorting By: {sort_by} ")
    await message.answer(f"1. Featured \n2. Bestselling \n3. Price: Low to High \n4. Price: High to Low \n5. Avg. Customer Review \n6. Publication Date")
    await message.answer(f"To change sorting by write: ✍️ sort 3 - if you want set a_sort_by_kama to 3")


@dp.message(Command("retry"))
async def handle_retry(message: types.Message):
    logger.info(f"--/RETRY command pressed")
    # Send a welcome message with the user's ID
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await bot.send_message(message.from_user.id, f"Retrying...")
            user_tasks[message.from_user.id] = asyncio.create_task(retry_a(message.from_user.id))
            # await send_updates(message.from_user.id, 'no')
        else:
            await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")
    else:
        await message.answer(f"Not Authorised. Write @kolodych")
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
        logger.info(f"--/XLS command pressed")
        # Send the Excel file as a document
        await message.answer(f"Example of 300+ found links:")
        try:
            filename = 'example_amazon_books.xlsx'
            script_dir = os.path.dirname(__file__) 
            filepath = os.path.join(script_dir, filename) 

            # Use Telegram's InputFile for proper file sending
            document = FSInputFile(path=filepath)
            # xls_document = FSInputFile(path='example_amazon_books.xlsx')
            await bot.send_document (message.from_user.id, document=document, caption='📊 Amazon Books Authors-links')
        except Exception as e:
            await message.answer(f"Error when sending xls: \n{e}")
    else:
        # Execute a query to get all data from the database
        await message.answer(f"Please wait... Making xls for you...")
        with engine.connect() as conn:
            query_all = text('SELECT * FROM `amazon_books` WHERE id > "8300" ')
            result = conn.execute(query_all)
            rows = result.fetchall()

        # Convert the query result to a pandas DataFrame
        df = pd.DataFrame(rows, columns=result.keys())

        # Create an in-memory Excel file
        excel_file = BytesIO()

        # Write the DataFrame to the Excel file
        df.to_excel(excel_file, index=False, engine='openpyxl')

        # Save the Excel file to the local file system
        try:
            file_path = 'amazon_books.xlsx'
            with open(file_path, 'wb') as file:
                file.write(excel_file.getvalue())
        except FileNotFoundError:
            logger.error("File not found.")
            await message.answer("File not found.")

        except IOError as e:
            logger.error(f"-->I/O error: {e}")
            await message.answer(f"I/O error: {e}")

        await message.answer(f"File saved.")
        # Send the Excel file as a document
        try:
            xls_document = FSInputFile(path='amazon_books.xlsx')
            await bot.send_document (message.from_user.id, document=xls_document, caption='📊 Amazon Books Authors-links')
        except Exception as e:
            await message.answer(f"Error when sending xls: \n{e}")



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
            # Use regular expression to extract the category and month from the message
            match = re.match(r'^retry (\d+) (\d+)', message.text.lower())
            
            if match:
                # Extract the category and month from the matched groups
                category = int(match.group(1).strip())
                month = int(match.group(2).strip())
                
                with engine.connect() as conn:
                    query_add_retry = text(f"INSERT INTO `amazon_retry` (`category`, `month`) VALUES ('{category}', '{month}');")
                    conn.execute(query_add_retry)
                await message.answer(f"Success! {category} Subject and {month} month inserted into amazon_retry.")
            else:
                await message.answer("Invalid 'retry' command format. Please use 'retry <category> <month>'.")
        elif message.text.lower().startswith('month '): # month 
            logger.info(f"--/MONTH found in text. Updating a_month_kama to ({message.text[6:]})")
            try:    
                with engine.connect() as conn:
                    query_db = text(f"UPDATE `vars`  SET value = '{message.text[6:]}' WHERE name = 'a_month_kama';")
                    conn.execute(query_db)
                    await bot.send_message(message.from_user.id, f"✅ Changed month to {message.text[6:]}")
            except Exception as e:
                logger.error(f"-->month exception couldn't update vars: {e}")
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please write: month 1 - if you want set a_month_kama to 1")
        
        elif message.text.lower().startswith('subject '):
            logger.info(f"--/SUBJECT found in text. Updating a_category_kama to ({message.text[8:]})")
            try:    
                with engine.connect() as conn:
                    query_db = text(f"UPDATE `vars`  SET value = '{message.text[8:]}' WHERE name = 'a_category_kama';")
                    conn.execute(query_db)
                    await bot.send_message(message.from_user.id, f"✅ Changed subject to {message.text[8:]}")
            except Exception as e:
                logger.error(f"-->subject exception couldn't update vars: {e}")
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please write: subject 1 - if you want set a_category_kama to 1")
        
        elif message.text.lower().startswith('format '):
            logger.info(f"--/FORMAT found in text. Updating a_format_kama to ({message.text[7:]})")
            try:    
                with engine.connect() as conn:
                    query_db = text(f"UPDATE `vars`  SET value = '{message.text[7:]}' WHERE name = 'a_format_kama';")
                    conn.execute(query_db)
                    await bot.send_message(message.from_user.id, f"✅ Changed format to {message.text[7:]}")
            except Exception as e:
                logger.error(f"-->format exception couldn't update vars: {e}")
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please write: format 1 - if you want set a_category_kama to 1")

        elif message.text.lower().startswith('sort '):
            logger.info(f"--/SORT found in text. Updating a_sort_by_kama to ({message.text[5:]})")
            try:    
                with engine.connect() as conn:
                    query_db = text(f"UPDATE `vars`  SET value = '{message.text[5:]}' WHERE name = 'a_sort_by_kama';")
                    conn.execute(query_db)
                    await bot.send_message(message.from_user.id, f"✅ Changed sorting by to {message.text[5:]}")
            except Exception as e:
                logger.error(f"-->sort by exception couldn't update vars: {e}")
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please write: sort 1 - if you want set a_sort_by_kama to 1")
        
        elif message.text.lower().startswith('year '):
            logger.info(f"--/YEAR found in text. Updating a_year_kama to ({message.text[5:]})")
            try:    
                with engine.connect() as conn:
                    query_db = text(f"UPDATE `vars`  SET value = '{message.text[5:]}' WHERE name = 'a_year_kama';")
                    conn.execute(query_db)
                    await bot.send_message(message.from_user.id, f"✅ Changed year to {message.text[5:]}")
            except Exception as e:
                logger.error(f"-->year exception couldn't update vars: {e}")
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please write: year 2020 - if you want set a_year_kama to 2020")
        
        else:
            await bot.send_message(message.from_user.id, f"🤷‍♂️ Didn't get that. Try again")
    else:
        await message.answer(f"Not Authorised. Write @kolodych")
        await handle_help(message)


async def retry_a(chat_id):
    current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
    while True:
        logger.info(f"-->RETRY_A___ Started on {current_time}")
        try:
            with engine.connect() as conn:
                query_db = text(f"SELECT * from `amazon_retry` ORDER BY id ASC")
                retry_data = conn.execute(query_db).fetchone()
                month = retry_data[2]
                category = retry_data[1]
                await bot.send_message(chat_id, f"Retry {category} {month}")
                delete_query = text(f"DELETE FROM `amazon_retry` WHERE id = :id")
                conn.execute(delete_query, {'id': retry_data[0]})
                await start_a(chat_id, category, str(month))
        except Exception as e:
            await bot.send_message(chat_id, f"/retry exception: \n{e}")
            await bot.send_message(chat_id, f"Retry {category} {month}")
            user_tasks[chat_id].cancel()
            user_tasks.pop(chat_id)
            await bot.send_message(chat_id, f"Retrying Stopped! +8 sec sleep /retry? 🙃")
            time.sleep(8)


async def repeat_a(chat_id):
    current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
    while True:
        logger.info(f"-->REPEAT_A___ Started on {current_time}")
        print(f"-->REPEAT_A___ Started on {current_time}")
        try:
            logger.info(f"-->connecting to vars database and collecting value of a_category_kama")
            with engine.connect() as conn:
                query_db = text(f"SELECT * from `vars` WHERE name = 'a_category_kama'")
                category_from_db = conn.execute(query_db).fetchone()
                query_db = text(f"SELECT * from `vars` WHERE name = 'a_month_kama'")
                month = conn.execute(query_db).fetchone()
                result_now = int(category_from_db[2])  # value now
                result_max = int(category_from_db[4])  # max value
                a_month_kama = int(month[2]) # month now

            current_time = datetime.now().strftime('%m%d-%H:%Mm:%Ss')
            await bot.send_message(chat_id, f"🔥 {current_time} Starting scraping ({result_now} of {result_max}) for month = {a_month_kama} / 12 ")
            logger.info(f"-->subject - month: {result_now} - {str(a_month_kama)}")
        except Exception as e:
            logger.error(f"->Repeat_a Exception: \n{e} ...")

        await start_a(chat_id, result_now, str(a_month_kama))

        await bot.send_message(chat_id, f"I can repeat from if you want)")



async def start_a(chat_id, subject_int, month):
    async with aiohttp.ClientSession() as session:
        logger.info(f"-->START_A Session 1 Started")
        try:
            # Website URL ⬇️ ⬇️ ⬇️ insert your site
            url = f'https://www.amazon.com/advanced-search/books'
            search_button = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[2]/td/input'
            # XPath to text ⬆️ ⬆️ ⬆️ insert title

            await bot.send_message(chat_id, f"🌈 starting driver for {subject_int}sub {month}mon")
            
            #driver = Driver(browser="chrome", uc=True, headless=True) # (browser="chrome", proxy=proxy_string, headless=True)
            manager = WebDriverManager()  # Create an instance of WebDriverManager
            driver = manager.get_driver()  
            
            driver.get(url)

            await bot.send_message(chat_id, f"🔍 opened Amazon Books Search Page for {subject_int}sub {month}mon")

            # FILTERS for Amazon Books ⬇️ ⬇️ ⬇️ https://www.amazon.com/advanced-search/books
            # subject comes from repeat_a = '1'  # (28 out 28) ALL_SUBJECTS={}   "1">Arts &amp; Photography<   "45">Bargain Books<   "2">Biographies &amp; Memoirs<   "3">Business &amp; Investing<   "4">Children's Books<   "12290">Christian Book &amp; Bibles<   "4366">Comics &amp; Graphic Novels<   "6">Cookbooks, Food &amp; Wine<   "48">Crafts, Hobbies &amp; Home<   "5">Computers &amp; Technology<   "21">Education &amp; Reference<   "301889">Gay &amp; Lesbian<   "10">Health, Fitness &amp; Dieting<   "9">History<   "86">Humor &amp; Entertainment<   "10777">Law<   "17">Literature &amp; Fiction<   "13996">Medicine<   "18">Mystery, Thriller &amp; Suspense<   "20">Parenting &amp; Relationships<   "3377866011">Politics &amp; Social Sciences<   "173507">Professional &amp; Technical Books<   "22">Religion &amp; Spirituality<   "23">Romance<   "75">Science &amp; Math<   "25">Science Fiction &amp; Fantasy<   "26">Sports &amp; Outdoors<   "28">Teens<   "27">Travel<
            subject = str(dicti[subject_int])
            # month comes from repeat_a = '11'  # (3 out 12) MONTH: Sep, Oct, Nov + Dec
            #format ONLY (3 out 15): "Paperback", "Hardcover", "Kindle Edition" //  FORMAT: "All Formats", "Paperback", "Hardcover", "Kindle Edition", "Audible Audio Edition", "HTML", "PDF", "Audio CD", "Board Book", "Audio Cassette", "Calendar", "School Binding", "MP3 CD", "Audiobooks", "Printed Books"
            with engine.connect() as conn:
                query_select = text("SELECT * FROM `vars` where name = 'a_format_kama'")
                result = conn.execute(query_select).fetchone()
                format_int = result[2]
            format = all_formats[str(format_int)]
            
            published_date = "During"  # (4 out 4) Published Date: "All Dates", "before", "During", "After"
            
            # Sorting Results by: (6 out 6) "relevanceexprank">Featured, "salesrank" -> Bestselling, "price" -> Price: Low to High, "-price" -> Price: High to Low, "reviewrank_authority" -> Avg. Customer Review, "daterank" -> Publication Date
            with engine.connect() as conn:
                query_select = text("SELECT * FROM `vars` where name = 'a_sort_by_kama'")
                result = conn.execute(query_select).fetchone()
                sort_by_int = result[2]
                sort_by = all_sort[str(sort_by_int)] 

            #year = "2022"  # (1) YEAR - Insert "2023" in the input field
            with engine.connect() as conn:
                query_select = text("SELECT * FROM `vars` where name = 'a_year_kama'")
                result = conn.execute(query_select).fetchone()
                year = str(result[2])

            logger.info(f"-->year = {year} | subject = {subject_int} | month = {month} | format = {format} | sort_by = {sort_by} ")

            condition = '1294423011'  # (1 out 3) CONDITION: "NEW ONLY" = '1294423011' # Also (not the case): "Collectible" + "Used"
            language = 'English'  # (1 out 4) LANGUAGE: Only English "French", "German", "Spanish" "All Languages"
            # Filters on Amazon books ⬆️ ⬆️ ⬆️ https://www.amazon.com/advanced-search/books

            x_published_date = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[5]/table/tbody/tr[2]/td[1]/select'
            x_month = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[5]/table/tbody/tr[2]/td[2]/select'
            x_sort = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[6]/select'
            x_year = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[5]/table/tbody/tr[2]/td[3]/input'
            x_subject = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[1]/div[6]/select'
            x_condition = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[1]/select'
            x_format = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[2]/select'
            x_language = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[2]/div[4]/select'
            search_button = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[2]/td/input'

            # x_author_search = '/html/body/div[1]/table/tbody/tr/td/table/tbody/tr/td[1]/form/table/tbody/tr[1]/td[1]/div[2]/input'
            # author_search_input = driver.find_element(By.XPATH, x_author_search)
            # author_search_input.clear()  
            # author_search_input.send_keys("a") # By Alphabet  

            select_date = Select(driver.find_element(By.XPATH, x_published_date))
            select_date.select_by_visible_text(published_date)
            select_month = Select(driver.find_element(By.XPATH, x_month))
            select_month.select_by_value(month)
            select_sort = Select(driver.find_element(By.XPATH, x_sort))
            select_sort.select_by_value(sort_by)
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
            driver.find_element(By.XPATH, search_button).click() # 👆👆👆👆👆👆👆 1st SEARCH BUTTON
            # Wait for 5 seconds for the page to load (you might want to use WebDriverWait for more robust waiting)
            time.sleep(1)

            xitem_count = '/html/body/div[1]/div[1]/span[2]/div/h1/div/div[1]/div/div/span'
            item_count_span = driver.find_element(By.XPATH, xitem_count)
            item_count_text = item_count_span.text.strip()

            search_query = f'{all_subjects[subject]} + {published_date} + {months[month]} + {year} + NEW ONLY + {language} + {format} + {sort_bys[sort_by]} '
            await bot.send_message(chat_id, f"🎉🎉🎉 {search_query} 🎉🎉🎉")
            page = 1

            try:
                subject_new = subject_int + 1
                with engine.connect() as conn:
                    # Increment value and update a_category_kama
                    if subject_new <= 15:
                        update_db = text(f"UPDATE `vars` SET value = '{subject_new}' WHERE name = 'a_category_kama';")
                        conn.execute(update_db)
                        await bot.send_message(chat_id, f"subject now = {subject_new}")
                    else:
                        # Reset to min value and update a_category_kama
                        await bot.send_message(chat_id, f"Subject {subject_new} out of max 15, so subject now = 2")
                        update_db = text(f"UPDATE `vars` SET value = '2' WHERE name = 'a_category_kama';")
                        conn.execute(update_db)

                        # Increment month and update a_month_kama
                        a_month_kama_new = int(month) + 1
                        await bot.send_message(chat_id, f"Month is {a_month_kama_new} now.")
                        update_db = text(f"UPDATE `vars` SET value = '{a_month_kama_new}' WHERE name = 'a_month_kama';")
                        conn.execute(update_db)
            except Exception as e:
                await bot.send_message(chat_id, f"Error when increasing subject: {str(e)}")

            while page < 76: # was 76
                if page > 0:
                    # Click the next page button
                    time.sleep(2) # for every page (72) * SLEEP 2 sec = 2 min total
                    # starts with 3, and then 4, 5, 6
                    if page < 5:
                        next_page_button = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/a[{page + 2}]'
                    elif 5 <= page <= 71:
                        next_page_button = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/a[5]'
                    elif page == 72:
                        next_page_button = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/a[7]'
                    elif page == 73:
                        next_page_button = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/a[6]'
                    elif page == 74:
                        next_page_button = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/a[5]'


                    if page == 1:
                        await bot.send_message(chat_id, f"next_page_button: \n{next_page_button}")
                        # Save HTML content to an HTML file
                        try:
                            with open(f'{page}.html', 'w', encoding='utf-8') as html_file:
                                html_file.write(driver.page_source)
                            html_document = FSInputFile(path=f'{page}.html')
                            await bot.send_document (chat_id, document=html_document, caption=f'📊 {page}.html Amazon Books')
                        except Exception as e:
                            await bot.send_document (chat_id, f"error while sending {page}.html: {e}")
                    sleep(1)

                    xitem_count = '/html/body/div[1]/div[1]/span[2]/div/h1/div/div[1]/div/div/span'
                    item_count_span = driver.find_element(By.XPATH, xitem_count)
                    item_count_text = item_count_span.text.strip()
                    try:
                        await bot.send_message(chat_id, f"{page}page {subject_int}sub {month}: {item_count_text} {search_query[:10]}... {year} {format} sort: {sort_by}")
                    except Exception as e:
                        await bot.send_message(chat_id, f"🤮 WTF! Where is Item_Count??? \n{e}")
                        break
                    wait = WebDriverWait(driver, 1)

                    # Clicked on Next Page, now wait 5 sec to load page
                    sleep(1)
                # Loop through the items starting from index 2 up to item_count + 2
                for i in range(2, 18):
                    # xtitle = "Harry Potter and ..."
                    xtitle = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i}]/div/div/span/div/div/div/div[2]/div/div/div[1]/h2/a'
                    xtitle2 = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[19]/div/div/span/a[4]'
                    xauthor2 = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i}]/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a'
                    xauthor = f'/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[{i}]/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a[2]'

                    wait = WebDriverWait(driver, 10)

                    try:
                        title_span = driver.find_element(By.XPATH, xtitle)
                    except NoSuchElementException:
                        logger.error(f'>--<--Refreshing page. {page}.html Saved')
                        driver.refresh()
                        sleep(5) # SLEEP when couldn't find title
                        title_span = driver.find_element(By.XPATH, xtitle2)
                    # Extract title text (it's inside a -> span)
                    title = title_span.text.strip()

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
                        with engine.connect() as conn:
                            query_db = text("INSERT INTO `amazon_books` (`query`, `page`, `about_link`) "
                                "VALUES (:query, :page, :about_link) "
                                "ON DUPLICATE KEY UPDATE "
                                "`query` = VALUES(`query`), "
                                "`page` = VALUES(`page`) "
                            )

                            conn.execute(query_db, {
                                'query': search_query,
                                'page': page,
                                'about_link': final_link,
                            })

                    # Pause for 0.5 seconds (adjust as needed)
                    sleep(0.5)
                retry_attempts = 3

                while retry_attempts > 0 and page < 75:
                    try:
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
            # await bot.send_message(chat_id, f"🔥 START_A Main Exception: 'n{e}")
            # await bot.send_message(chat_id, f"🔥 START_A Main Exception: {str(e).split('stacktrace')[0].strip() if 'stacktrace' in str(e).lower() else str(e)}") # send till work stacktrace
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
            manager.close_driver()        
            await bot.send_message(chat_id, f"{page} - Retry {subject-1} {month-1} if you want")

        finally:
            await bot.send_message(chat_id, f"😺 Finally: 1.Closing driver, 2.Closing session...") # {page} - Retry {subject_int-1} {month-1} if you want...)")

            manager.close_driver()        
            await session.close()
            logger.info(f"-->START_A Session 1 Closed")

            sleep(1)
        # Close session?
    
    # await session.close()
    await bot.send_message(chat_id, "😺 Full End. Driver closed. Session closed.")



async def main():
    # Create a Bot instance with the specified token (needed to send messages without user's message)
    bot = Bot(token=TOKEN)
    # Start polling for updates using the Dispatcher
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
