# pip install beautifulsoup4 selenium-base selenium fake-useragent pandas aiohttp aiogram sqlalchemy asyncio pymysql openpyxl psycopg2


import config_a
TOKEN = config_a.TOKEN_B

global conn_params

conn_params = config_a.conn_params

import psycopg2
from io import BytesIO
import requests
from bs4 import BeautifulSoup
import re

from sqlalchemy import text
from seleniumbase import BaseCase
from seleniumbase import Driver
import os
import time
from datetime import datetime
from time import sleep
from fake_useragent import UserAgent # to generate headers user agent

from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import Select
from sqlalchemy import create_engine, text
import asyncio # is part of the Python standard library
from aiogram import Bot, Dispatcher, types # Bot - for send updates, Dispatcher - MUST_HAVE, types - MUST_HAVE
from aiogram.filters import Command # you can import only one of them (if needed)
from dictionaries import all_subjects, months, sort_bys, all_formats, dicti
from aiogram.types import FSInputFile
import pandas as pd


current_time = datetime.now().strftime('%m%d%H:%Mm:%Ss')
print(f" Amazon b STARTED! at {current_time}")

ADMIN_IDS = config_a.ADMIN_IDS

user_tasks = {}

bot = Bot(TOKEN)

# Create a Dispatcher for handling incoming messages and commands
dp = Dispatcher()



proxy_comment = "webshare"             # proxy6 OR webshare

class WebDriverManager:
    _instance = None

    def __init__(self, conn_params):
        self.conn_params = None
        self.conn = None
        self.cursor = None

    def get_driver(self):
        if not self._instance:
            proxy_string = self.get_one_proxy()
            user_agent = self.get_random_user_agent()
            self._instance = self.create_web_driver(proxy_string, user_agent)
        return self._instance

    def close_driver(self):
        if self._instance:
            self._instance.quit()
            self._instance = None

    def get_one_proxy(self):
        proxy_info = self.fetch_proxy_from_database()
        if proxy_info:
            proxy_string = proxy_info[1]
            self.update_proxy_info(proxy_info[0])
            print(f"proxy-string: {proxy_string}")
            return proxy_string
        else:
            return None

    def get_random_user_agent(self):
        user_agent = UserAgent()
        return user_agent.random

    def create_web_driver(self, proxy_string, user_agent):
        # driver = Driver(browser="chrome", headless=True, uc=True, proxy=proxy_string, agent=user_agent)
        driver = Driver(browser="chrome", headless=True, uc=True, agent=user_agent)
        return driver
    
    def fetch_proxy_from_database(self):
        try:
            self.connect()
            query_select = "SELECT * FROM proxies WHERE comment like '%new10%' ORDER BY count ASC LIMIT 1 ;"
            self.cursor.execute(query_select, ('%' + proxy_comment + '%',))
            result = self.cursor.fetchone()
            print(f"-----result-proxy==: {result}")
            return result
        except Exception as e:
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
            pass
        finally:
            self.disconnect()

    def connect(self):
        self.conn = psycopg2.connect(**self.conn_params)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# Define a message handler for the /start command
@dp.message(Command("start"))
async def handle_start(message: types.Message):
    print(f"--/START command pressed")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await message.answer("🚀 Bot started!")
            user_tasks[message.from_user.id] = asyncio.create_task(repeat_b(message.from_user.id, None))
        else:
            await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")
    else:
        await message.answer(f"Not Authorised. Watch this video: \nhttps://youtube.com/shorts/__l4pE849Tc")
        await handle_help(message)
        await bot.send_message(266585723, f"new-user: {message.from_user.full_name}\nID: {message.from_user.id}\n@{message.from_user.username}")


# Define a message handler for the /start command
@dp.message(Command("stop"))
async def handle_stop(message: types.Message):
    print(f"[_______STOP_________] received from {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id in user_tasks and user_tasks[message.from_user.id]:
            user_tasks[message.from_user.id].cancel()
            user_tasks.pop(message.from_user.id)
            await message.answer("Stopped. /start? 🙃")
        else:
            await message.answer("I'm not working now 🤷‍ - you can /start me 😉")
    else:
        await message.answer(f"Not Authorised. Write @kolodych")
        await handle_help(message)


@dp.message(Command("now"))
async def handle_now(message: types.Message):
    print(f"--/NOW command pressed")
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        query = "SELECT * FROM amazon_books WHERE link1 IS NULL AND id > 0 ORDER BY id ASC LIMIT 1"
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        await message.answer(f"🤖 Now analyzing: {row[0]}\n{str(row)}")
    except psycopg2.Error as e:
        await message.answer(f"An error occurred: {e}")
    finally:
        conn.close()


@dp.message(Command("total"))
async def handle_total(message: types.Message):
    print(f"--/TOTAL command pressed")
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        # Total of status XLS
        query_total = "SELECT COUNT(*) FROM amazon_books WHERE status = 'xls'"
        cursor.execute(query_total)
        total_count = cursor.fetchone()[0]

        query_total = "SELECT COUNT(*) FROM amazon_books"
        cursor.execute(query_total)
        total_amazon_books = cursor.fetchone()[0]

        query_new = "SELECT COUNT(*) FROM amazon_books WHERE status = 'new'"
        cursor.execute(query_new)
        total_to_scan = cursor.fetchone()[0]

        query_text = "SELECT COUNT(*) FROM amazon_books WHERE status = 'text'"
        cursor.execute(query_text)
        total_text = cursor.fetchone()[0]

        query_links = "SELECT COUNT(*) FROM amazon_books WHERE status = 'links'"
        cursor.execute(query_links)
        total_links = cursor.fetchone()[0]

        select_query = "SELECT value FROM vars WHERE name = 'offset'"
        cursor.execute(select_query)
        existing_offset = cursor.fetchone()[0].strip()
        offset = int(existing_offset)

        await message.answer(f"🔗 Total: {total_count - offset} author - links 🔗\nTo Scan New /start: {total_to_scan} ⚠️"
                            f"\nLinks found: {total_links} \nSome text (no-links): {total_text} \nAll: {total_amazon_books} ")
    finally:
        cursor.close()
        conn.close()


@dp.message(Command("status"))
async def handle_now(message: types.Message):
    print(f"--/STATUS command pressed")
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Retrieve distinct statuses and their counts
        cursor.execute("SELECT DISTINCT status, COUNT(*) FROM amazon_books GROUP BY status")
        rows = cursor.fetchall()
        
        # Prepare message with status and count
        status_counts_message = "\n".join([f"{row[0]} = {row[1]}" for row in rows])
        
        await message.answer(f"Status counts:\n{status_counts_message}")
        
        cursor.close()
        
    except psycopg2.Error as e:
        await message.answer(f"An error occurred: {e}")
    finally:
        conn.close()


@dp.message(Command("py"))
async def handle_py(message: types.Message):
    try:
        filename = 'amazon_b.py'
        script_dir = os.path.dirname(__file__) 
        filepath = os.path.join(script_dir, filename) 

        # Use Telegram's InputFile for proper file sending
        document = FSInputFile(path=filepath)

        await bot.send_document(message.from_user.id, document=document, caption='📊 Amazon B py file')
    except Exception as e:
        await message.answer(f"error: {e}")


@dp.message(Command("last"))
async def handle_last(message: types.Message):
    print(f"--/LAST command pressed")
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        query_last = "SELECT * FROM `amazon_books` ORDER BY id DESC LIMIT 1"
        cursor.execute(query_last)
        row = cursor.fetchone()

        await message.answer(f"🌝 Last: {row[0]}\n{str(row)} ")
    finally:
        cursor.close()
        conn.close()


@dp.message(Command("log")) # /log
async def handle_log(message: types.Message):
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
    # Specify the folder containing HTML files
    folder_path = '/'

    # List all files in the folder
    html_files = [file for file in os.listdir(folder_path) if file.endswith('.html')]

    # Iterate over each HTML file
    for html_file in html_files:
        try:
            # Construct the file path
            file_path = os.path.join(folder_path, html_file)

            # Send the document using Aiogram
            html_document = FSInputFile(path=file_path)
            caption = f'📊 Amazon Books {html_file.replace(".html", "")}'

            # Assuming you have a 'message' object available
            await bot.send_document(message.from_user.id, document=html_document, caption=caption)

        except Exception as e:
            await message.answer(f"Error when sending html for {html_file}: \n{e}")




@dp.message(Command("xls"))
async def handle_xls(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
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
            await message.answer("File not found.")

        except IOError as e:
            await message.answer(f"I/O error: {e}")

        await message.answer(f"File saved.")
        # Send the Excel file as a document
        try:
            xls_document = FSInputFile(path='amazon_books.xlsx')
            await bot.send_document (message.from_user.id, document=xls_document, caption='📊 Amazon Books Authors-links')
        except Exception as e:
            await message.answer(f"Error when sending xls: \n{e}")


@dp.message(Command("help"))
async def handle_help(message: types.Message):
    await bot.send_message(message.from_user.id, "/start - ✅ Start from last one\n"
        "/now - 🧐 currently checking link №...\n"
        "/last - 🌝 get the latest author - link\n"
        "/total - 🔗 get count of all author - links\n"
        "/xls - 📊 download xls\n"
        "/status - 🔢 statuses\n"
        "/stop - ❌ Stop)")


async def repeat_b(chat_id, row=126211):
    print(f"--/REPEAT_B function started!")
    while True:
        if row == None:
            try:
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()
                query_select = "SELECT * FROM amazon_books WHERE status = 'new' ORDER BY id ASC LIMIT 1"
                cursor.execute(query_select)
                result = cursor.fetchone()
                row = int(result[0]) - 1
            finally:
                cursor.close()
                conn.close()

                # query_select = text(f'SELECT * FROM `amazon_books` WHERE link1 IS NULL or link1 = "" AND `id` >= 0 ORDER BY `id` ASC LIMIT 1;')
        else:
            pass
        await bot.send_message(chat_id, f"🤖 Now analyzing ID: {str(row)}")
        await start_b(chat_id, row)
        await asyncio.sleep(1)







@dp.message()
async def handle_text(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        if message.text.isdigit():
            if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
                await message.answer("🚀 Bot started! You will from NOW receive Updates with discounts.")
                user_tasks[message.from_user.id] = asyncio.create_task(repeat_b(message.from_user.id, int(message.text)))
            else:
                await message.answer("✅ Bot is already running. If you want to stop, use /stop 🛑")

        elif message.text.lower().startswith("fb "):
            await message.answer(f"Please wait... parsing username tags for you from ID: {message.text[3:]}")
            try:
                with engine.connect() as conn:
                    query_all = text(f"SELECT * FROM `amazon_books` WHERE LEFT(link1, 1) = '@' AND id > {message.text[3:]} AND link2 = '' AND link3 = '' AND link4 = '' AND link5 = '' AND link6 = '' AND link7 = '' AND link8 = '' AND link9 = '' ORDER BY id ASC;")
                    result = conn.execute(query_all)
                    rows = result.fetchall()
                updated_rows = []  # Initialize list to store updated rows
                for row in rows:
                    # Convert integers to strings
                    row = [str(value) for value in row]
                    
                    # Remove trailing punctuation
                    for i in range(len(row)):
                        row[i] = re.sub(r'[.,!;]$', '', row[i])

                    # Extract Instagram username from link1
                    instagram_username = row[5].split('@')[-1] if row[5] else None

                    # Fetch page title from Instagram
                    if instagram_username:
                        instagram_url = f"https://www.instagram.com/{instagram_username}/"
                        try:
                            response = requests.get(instagram_url)
                            if response.status_code == 200:
                                # If Instagram link is valid, update link1 with "#" + Instagram link
                                updated_row = row.copy()
                                updated_row[5] = f"#{instagram_url}"
                                updated_rows.append(updated_row)
                                # Update the database with the new link1 value
                                update_query = text(f"UPDATE `amazon_books` SET link1 = '{updated_row[5]}' WHERE id = {row[0]}")
                                conn.execute(update_query)
                                print(f"+ {row[0]} +# ", instagram_url) # ++++++++

                            else:
                                print(f"Failed to fetch Instagram page: {response.status_code}")
                        except Exception as e:
                            print(f"Error fetching Instagram page: {e}")
                    else:
                        # If no Instagram username is found, update link1 with "-insta-" + username
                        updated_row = row.copy()
                        updated_row[5] = f"-insta-{instagram_username}"
                        updated_rows.append(updated_row)
                        print(f"- {row[0]} -insta- ", instagram_username) # ++++++++

                        # Update the database with the new link1 value
                        update_query = text(f"UPDATE `amazon_books` SET link1 = '{updated_row[5]}' WHERE id = {row[0]}")
                        conn.execute(update_query)

            except Exception as e:
                print(f"Error: {e}")
                await message.answer(f"Error: {e}")

        

        elif message.text.lower().startswith("fb#"):
            await message.answer(f"Please wait... parsing username tags for you from ID: {message.text[4:]}")
            try:
                with engine.connect() as conn:
                    # Select rows where the first element of link1 is "#"
                    query_all = text(f"SELECT * FROM `amazon_books` WHERE SUBSTRING(link1, 1, 1) = '#' AND id > {message.text[4:]} ORDER BY id ASC;")
                    result = conn.execute(query_all)
                    rows = result.fetchall()

                # Process rows and create DataFrame
                processed_rows = []  # Initialize list to store processed rows
                for row in rows:
                    # Extract values from the row
                    id_val, query_val, page_val, about_link_val, author_val, instagram_val = row[:6]  # Assuming Instagram link is in the 6th position
                    link_columns = row[6:]  # Remaining columns are link columns

                    # Append the row with Instagram link to processed_rows
                    processed_row = [id_val, query_val, page_val, about_link_val, author_val, '', instagram_val, '', '', ''] + list(link_columns)
                    processed_rows.append(processed_row)

                # Create DataFrame
                columns = ['id', 'query', 'page', 'about_link', 'author', 'Facebook', 'Instagram', 'Twitter', 'Linkedin', 'Email'] + [f'link{i}' for i in range(1, 10)]
                df = pd.DataFrame(processed_rows, columns=columns)

                # Convert DataFrame to Excel
                excel_file = BytesIO()
                df.to_excel(excel_file, index=False, engine='openpyxl')

                # Send Excel file
                xls_document = FSInputFile(content=excel_file.getvalue(), filename=f'amazon_books_{message.text[4:]}.xlsx')
                await bot.send_document(message.from_user.id, document=xls_document, caption=f'📊 {len(df)} rows - Amazon Books Authors-links (ID > {message.text[4:]})')
                await message.answer(f"File saved.")
            except Exception as e:
                print(f"Error: {e}")
                await message.answer(f"Error: {e}")



        elif message.text.lower().startswith("xls "):
            await message.answer(f"Please wait... Making xls for you from ID: {message.text[4:]}")

                # query_all = f"SELECT id, about_link, link1, link2, link3, link4, link5, link6, link7, link8, link9 FROM amazon_books WHERE id > {message.text[4:]} AND status = 'links' ORDER BY id ASC"
            try:
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()

                query_all = f"SELECT id, query, page, about_link, author, link1, link2, link3, link4, link5, link6, link7, link8, link9 FROM amazon_books WHERE id > {message.text[4:]} AND status = 'links' ORDER BY id ASC"
                cursor.execute(query_all)
                rows = cursor.fetchall()

                # Process the rows to categorize links
                processed_rows = []
                for row in rows:
                    # Initialize the social media links to empty strings
                    facebook = instagram = twitter = linkedin = email = ""
                    links = list(row[5:14])  # Assuming links are from index 5 to 14
                    # Check each link and categorize it
                    for i, link in enumerate(links):
                        if link is not None:  # Add this check to ensure link is not None
                            if 'facebook.com' in link:
                                facebook = link
                                links[i] = ""  # Clear the link
                            elif 'instagram.com' in link:
                                instagram = link
                                links[i] = ""
                            elif 'twitter.com' in link:
                                twitter = link
                                links[i] = ""
                            elif 'linkedin.com' in link:
                                linkedin = link
                                links[i] = ""
                            elif '@' in link:
                                email = link
                                links[i] = ""
                    # Reconstruct the row with categorized links
                    processed_row = row[:5] + (facebook, instagram, twitter, linkedin, email) + tuple(links)
                    processed_rows.append(processed_row)

                # Create a DataFrame with the new structure
                columns = ['id', 'query', 'page', 'about_link', 'author', 'Facebook', 'Instagram', 'Twitter', 'Linkedin', 'Email'] + [f'link{i}' for i in range(1, 10)]
                df = pd.DataFrame(processed_rows, columns=columns)
                row_count = df.shape[0]  # Count the number of rows

                excel_file = BytesIO()
                df.to_excel(excel_file, index=False, engine='openpyxl')
                file_path = f'amazon_books_{message.text[4:]}.xlsx'
                with open(file_path, 'wb') as file:
                    file.write(excel_file.getvalue())

                await message.answer(f"File saved.")
                # Modify the caption to include row_count
                xls_document = FSInputFile(path=file_path)
                await bot.send_document(message.from_user.id, document=xls_document, caption=f'📊 {row_count} rows - Amazon Books Authors-links (ID > {message.text[4:]})')

            except Exception as e:
                print(f"Error: {e}")
                await message.answer(f"Error: {e}")

            finally:
                cursor.close()
                conn.close()



        

        elif message.text.lower().startswith('delete '):
            await message.answer(f"Deleting ID: {message.text[7:]}")
            try:
                with engine.connect() as conn:
                    query_delete = text(f'DELETE FROM `amazon_books` WHERE `id` = {message.text[7:]}')
                    conn.execute(query_delete)
                await message.answer(f"ID: {message.text[7:]} deleted.")
            except Exception as e:
                print(f"Error: {e}")
                await message.answer(f"Error: {e}")
        elif message.text.lower().startswith('offset '): # offset 
            try:    
                with engine.connect() as conn:
                    # Get the existing offset value
                    select_query = text("SELECT value FROM `vars` WHERE name = 'offset'")
                    result = conn.execute(select_query)
                    existing_offset = result.fetchone()[0].strip()  # assuming 'value' column is numeric

                    # Calculate new offset
                    new_value = int(message.text[7:].strip())
                    # Convert new value from string to integer
                    new_offset = int(existing_offset) + int(new_value)

                    # Update the database with the new offset
                    update_query = text(f"UPDATE `vars` SET value = {new_offset} WHERE name = 'offset'")
                    conn.execute(update_query)

                    await bot.send_message(message.from_user.id, f"✅ Offset updated to {new_offset}")
            except ValueError:  # In case of non-numeric input
                await bot.send_message(message.from_user.id, f"🤷‍♂️ Please provide a valid number after 'offset'")
            except Exception as e: 
                # Generic exception handling (you can improve based on the engine you're using)
                await bot.send_message(message.from_user.id, f"🤷‍♂️ An error occurred: {e}")

    else:
        await message.answer(f"Not Authorised. Write @kolodych")
        await handle_help(message)
   

async def start_b(chat_id, id_start='0'):
    await bot.send_message(chat_id, f"Starting from {id_start}")

    try:
        print(" Driver start here")

        manager = WebDriverManager(conn_params)  # Create an instance of WebDriverManager
        driver = manager.get_driver()  

        # driver = Driver(browser="safari", uc=True) #, headless=True) # (browser="chrome", proxy=proxy_string, headless=True)
        #driver.maximize_window()


        print("it is not the end but driver ends here ")

        # Read authors from the database
        print("here starts engine connect and query")
        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            query_select = "SELECT id, about_link, link1, link2, link3, link4, link5, link6, link7, link8, link9 FROM amazon_books WHERE status = 'new' ORDER BY id ASC"
            # when status links but no links in columns link1,2...
            # query_select = "SELECT id, about_link, link1, link2, link3, link4, link5, link6, link7, link8, link9 FROM amazon_books WHERE status = 'links' AND (link1 IS NULL OR link1 = '') AND (link2 IS NULL OR link2 = '') AND (link3 IS NULL OR link3 = '') AND (link4 IS NULL OR link4 = '') AND (link5 IS NULL OR link5 = '') AND (link6 IS NULL OR link6 = '') AND (link7 IS NULL OR link7 = '') AND (link8 IS NULL OR link8 = '') AND (link9 IS NULL OR link9 = '') AND (facebook IS NULL OR facebook = '') AND (instagram IS NULL OR instagram = '') AND (twitter IS NULL OR twitter = '') AND (linkedin IS NULL OR linkedin = '') AND (email IS NULL OR email = '');"

            cursor.execute(query_select)
            result = cursor.fetchall()
        except Exception as e:
            await bot.send_message(chat_id, f"Error (with database): {e}")
        finally:
            cursor.close()
            conn.close()
        
            # query_authors = text(f"SELECT `about_link`, `link1`, `link2`, `link3`, `link4`, `link5`, `link6`, `link7`, `link8`, `link9`, `id` FROM `amazon_books` WHERE id > {id_start} and link1 IS NULL or link1 = '' ORDER BY `id`")
            # result = conn.execute(query_authors)
            print("Query Authors executed successfully! ")
            author_name = None  # Initialize to avoid reference error

            for row in result:
                current_time = datetime.now().strftime('%m%d%H:%Mm:%Ss')
                print(f"{current_time} _checking ID: {row[0]} {row[1]}")
                #print(f" Loading author_url page = {row[0]}")

                author_url = row[1]  # Assuming `about_link` is the first (0-indexed) column in the SELECT statement
                stored_links = row[1:10]  # Assuming the links columns start from the second column

                # Open the author's website
                driver.get(author_url)
                status = 'new'
       
                try:
                    # print(f"{author_url}") # scanning page for text
                    # Find the author's name
                    author_name = driver.find_element(By.TAG_NAME, "h1").text
                    # author_name = driver.find_element(By.XPATH, "/html/body/div[1]/div[1]/div/div[3]/div/div/div/div/h1").text
                    # /html/body/div[1]/div[1]/div/div[3]/div/div/div/div/h1

                    # Find the specified div element 
                    # author_div = driver.find_element(By.XPATH, "/html/body/div[1]/div[1]/div/div[4]/div/div/div/div/div/div[2]/div")
                    # /html/body/div[1]/div[1]/div/div[4]/div/div/div/div/div[2]/div/p
                    # /html/body/div[1]/div[1]/div/div[4]/div/div/div/div/div[2]/div
                    author_div = driver.find_element(By.XPATH, "/html/body/div[1]/div[1]/div/div[4]/div/div/div/div/div[2]")

                    # Find all p elements inside the div
                    p_elements = author_div.find_elements(By.TAG_NAME, 'p')

                    # Extracted links for the current author
                    author_links = []

                    # Iterate through each p element and extract links
                    for p_element in p_elements:
                        # Extract text from the p element
                        p_text = p_element.text

                        # print('---P =', len(p_text))
        
                        # Find links in the text by keywords 
                        links = [word for word in p_text.split() if any(prefix in word.lower() for prefix in ['@', 'http', 'www', '.com', '.co', '.org', '.net', '.ca', '.info', '.xyz', '.ly', '.site', '.me', '.ru', '.de', '.uk', '.in', '.online', '.us', '.ai', '.cc'])]

                        # Add the links to the list
                        author_links.extend(links)
                    if not p_elements:
                        status = 'empty'
                    else:
                        status = 'text'
                    if len(author_links) > 0:
                        status = 'links'

                    try:
                        conn = psycopg2.connect(**conn_params)
                        cursor = conn.cursor()
                        query_select = "UPDATE amazon_books SET status = %s WHERE about_link = %s"
                        cursor.execute(query_select, (status, author_url))
                        conn.commit()
                    except Exception as e:
                        await bot.send_message(chat_id, f"Error on update status database: {e}")
                    finally:
                        cursor.close()
                        conn.close()
    
                    print("🚛 ", status, author_url)
                    await bot.send_message(chat_id, f"🚛 status, author:  {status}  {author_url}")
                    # author_links.sort()
                    # print("___links= ", author_links)
                    # print('---here starts link1-9 insertion')

                    try:
                        # Establish a connection to the database
                        conn = psycopg2.connect(**conn_params)
                        cursor = conn.cursor()

                        if not author_links and not any(link for link in stored_links):
                            # No links found, update the status in the database
                            status = 'no'
                            query_update = "UPDATE amazon_books SET status = %s WHERE about_link = %s"
                            cursor.execute(query_update, (status, author_url))
                            conn.commit()
                            print(f"NO LINKS FOR ID: {row[10]} // {author_name} == {author_div:15}// no links found")
                            # Optionally, you can delete the row from the database
                            # query_delete = "DELETE FROM amazon_books WHERE about_link = %s"
                            # cursor.execute(query_delete, (author_url,))
                            # conn.commit()
                            # print(f"DELETED ID: {row[10]} // {author_name} // no links found")
                        elif len(author_links) > 0:
                            current_time = datetime.now().strftime('%m%d%H:%Mm:%Ss')
                            print(f"{row[0]} ID - {len(author_links)} links Added at {current_time}")
                            # Send a message using your bot
                            await bot.send_message(chat_id, f"✅ ID: {row[0]} - {len(author_links)} links Added at {current_time} \n{author_url}")

                            # Update the database with the author's information and links
                            query_update = """
                                UPDATE amazon_books SET
                                author = %s,
                                link1 = %s,
                                link2 = %s,
                                link3 = %s,
                                link4 = %s,
                                link5 = %s,
                                link6 = %s,
                                link7 = %s,
                                link8 = %s,
                                link9 = %s
                                WHERE about_link = %s
                            """
                            # Execute the query with the necessary parameters
                            cursor.execute(query_update, (
                                author_name,
                                author_links[0] if author_links else '',
                                author_links[1] if len(author_links) > 1 else '',
                                author_links[2] if len(author_links) > 2 else '',
                                author_links[3] if len(author_links) > 3 else '',
                                author_links[4] if len(author_links) > 4 else '',
                                author_links[5] if len(author_links) > 5 else '',
                                author_links[6] if len(author_links) > 6 else '',
                                author_links[7] if len(author_links) > 7 else '',
                                author_links[8] if len(author_links) > 8 else '',
                                author_url
                            ))
                            conn.commit()

                    except psycopg2.Error as e:
                        print("Error occurred:", e)

                    finally:
                        # Close the cursor and connection in the finally block
                        if cursor:
                            cursor.close()
                        if conn:
                            conn.close()
                except NoSuchElementException:
                    # /html/body/div/div/a/img

                    try:
                        # Find the element using XPath
                        img_element = driver.find_element(By.XPATH, "/html/body/div/div/a/img")

                        # Check if the element is an image and has a 'src' attribute
                        if img_element.tag_name == 'img' and 'src' in img_element.get_attribute('outerHTML'):
                            img_src = img_element.get_attribute('src')
                            await bot.send_message(chat_id, f"🐶🐶🐶 - image found: {img_src}")
                            try:
                                status = "404"
                                conn = psycopg2.connect(**conn_params)
                                cursor = conn.cursor()
                                query_select = "UPDATE amazon_books SET status = %s WHERE about_link = %s"
                                cursor.execute(query_select, (status, author_url))
                                conn.commit()
                            except Exception as e:
                                await bot.send_message(chat_id, f"Error on update status database: {e}")
                            finally:
                                cursor.close()
                                conn.close()
            
                            print("🚛 ", status, author_url)
                        else:
                            print(" 🚒 🚒 The element found is not an image or does not have a 'src' attribute.")
                    except Exception as e:
                        print("🚒 🚒 An error occurred:", e)

                    print(f"🤖🤖🤖Specified div element not found for author: {author_url} // Author: {author_name} // status: {status}")

    except IOError as e:
        print(f"I/O error: {e}")
        await bot.send_message(chat_id, f"I/O error: {e}")

    except Exception as e:
        print(f"🚒 Major error: \n{e}")
        await bot.send_message(chat_id, f"⛔️ Major error: \n{e}")
    

    finally:
        # Close the WebDriver
        manager.close_driver()        
        #driver.quit()
        await bot.send_message(chat_id, f"🏁 Finished. Last is: {str(row[0])} \n{str(row)}")
        user_tasks[chat_id].cancel()
        user_tasks.pop(chat_id)



async def main():
    # Create a Bot instance with the specified token (needed to send messages without user's message)
    bot = Bot(token=TOKEN)
    # Start polling for updates using the Dispatcher
    await dp.start_polling(bot, skip_updates=True)


if __name__ == '__main__':
    asyncio.run(main())
