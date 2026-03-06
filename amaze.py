# pip install selenium-base seleniumbase fake-useragent pandas aiohttp aiogram sqlalchemy asyncio pymysql openpyxl psycopg2

import config_a
import psycopg2
import logging
import asyncio
import aiohttp
import re
import os
import time
from time import sleep
from datetime import datetime
import pandas as pd
from io import BytesIO

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from fake_useragent import UserAgent
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import FSInputFile
from sqlalchemy import text

from dictionaries import all_subjects, months, all_sort, sort_bys, all_formats, dicti
from x_paths import x_published_date, x_month, x_sort, x_year, x_subject, x_condition, x_format, x_language, x_search_button

print("amaze.py [INIT] Loading config and constants...")
TOKEN = config_a.TOKEN
ADMIN_IDS = config_a.ADMIN_IDS
conn_string = config_a.conn_string
#print(f"[INIT] TOKEN loaded: {'yes' if TOKEN else 'NO - MISSING'}")
#print(f"[INIT] ADMIN_IDS: {ADMIN_IDS}")
#print(f"[INIT] conn_string loaded: {'yes' if conn_string else 'NO - MISSING'}")

script_dir = os.path.dirname(__file__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=os.path.join(script_dir, 'amazon_a.log'))
logger = logging.getLogger('amazon_a')
#print(f"[INIT] Logger configured. Log file: {os.path.join(script_dir, 'amazon_a.log')}")

bot = Bot(TOKEN)
dp = Dispatcher()
user_tasks = {}
timer_tasks = {}
#print("[INIT] Bot and Dispatcher created.")


# ── DB helpers ─────────────────────────────────────────────────────────────────

def db_connect():
    #print(f"[DB] Connecting to database...")
    conn = psycopg2.connect(conn_string)
    #print(f"[DB] Connected successfully.")
    return conn, conn.cursor()


def db_close(conn, cursor):
    if cursor: cursor.close()
    if conn: conn.close()
    #print(f"[DB] Connection closed.")


def fetch_var(name):
    #print(f"[DB] fetch_var: {name}")
    conn, cur = db_connect()
    try:
        cur.execute("SELECT value FROM vars WHERE name = %s", (name,))
        result = cur.fetchone()[0]
        #print(f"[DB] fetch_var({name}) = {result}")
        return result
    finally:
        db_close(conn, cur)


def update_var(name, value):
    #print(f"[DB] update_var: {name} = {value}")
    conn, cur = db_connect()
    try:
        cur.execute("UPDATE vars SET value = %s WHERE name = %s", (value, name))
        conn.commit()
        #print(f"[DB] update_var({name}) committed.")
    finally:
        db_close(conn, cur)


def fetch_format():
    val = str(fetch_var('a_format'))
    result = all_formats.get(val)
    #print(f"[CONFIG] fetch_format: key={val}, value={result}")
    return result


def fetch_sort_by():
    val = str(fetch_var('a_sort_by'))
    result = all_sort.get(val)
    #print(f"[CONFIG] fetch_sort_by: key={val}, value={result}")
    return result


def fetch_year():
    result = str(fetch_var('a_year'))
    #print(f"[CONFIG] fetch_year: {result}")
    return result


# ── Driver ─────────────────────────────────────────────────────────────────────

class WebDriverManager:
    _instance = None

    def _build_driver(self, proxy_string=None, user_agent=None):
        if not user_agent:
            ua = UserAgent()
            # Force desktop Chrome UA — mobile/iPhone UAs break Amazon scraping
            for _ in range(10):
                candidate = ua.random
                if "Mobile" not in candidate and "iPhone" not in candidate and "Android" not in candidate:
                    user_agent = candidate
                    break
            else:
                user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        #print(f"[DRIVER] Building Chrome driver. proxy={proxy_string}, ua={user_agent[:80]}...")
        driver = Driver(browser="chrome", uc=True, agent=user_agent)
        driver.set_window_size(1200, 800)
        #print(f"[DRIVER] Driver built successfully.")
        return driver

    def get_driver(self):
        if not self._instance:
            #print("[DRIVER] No existing instance, creating new driver...")
            self._instance = self._build_driver()
        return self._instance

    def get_working_proxy_driver(self):
        proxy_info = None
        if not self._instance:
            try:
                #print("[DRIVER] Fetching proxy from DB...")
                conn, cur = db_connect()
                cur.execute("SELECT * FROM proxies WHERE comment like '%new10%' ORDER BY count ASC LIMIT 1")
                proxy_info = cur.fetchone()
                db_close(conn, cur)
                #print(f"[DRIVER] Proxy fetched: {proxy_info}")
            except Exception as e:
                #print(f"[DRIVER] ERROR fetching proxy: {e}")
                logger.error(f"Error fetching proxy: {e}")
            if proxy_info:
                self._update_proxy(proxy_info[0])
                self._instance = self._build_driver(proxy_string=proxy_info[1])
            else:
                #print("[DRIVER] No proxy found, building driver without proxy...")
                self._instance = self._build_driver()
        return self._instance, proxy_info

    def get_test_proxy_driver(self):
        return self.get_working_proxy_driver()

    def _update_proxy(self, proxy_id):
        try:
            #print(f"[DRIVER] Updating proxy usage count for id={proxy_id}")
            conn, cur = db_connect()
            cur.execute("UPDATE proxies SET date = %s, count = count + 1 WHERE id = %s", (datetime.now(), proxy_id))
            conn.commit()
            db_close(conn, cur)
        except Exception as e:
            #print(f"[DRIVER] ERROR updating proxy: {e}")
            logger.error(f"Error updating proxy: {e}")

    def close_driver(self):
        if self._instance:
            #print("[DRIVER] Closing driver...")
            self._instance.quit()
            self._instance = None
            #print("[DRIVER] Driver closed.")


# ── Bot commands ───────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def handle_start(message: types.Message):
    #print(f"[BOT] /start from user {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await message.answer("🚀 Bot started!")
            user_tasks[message.from_user.id] = asyncio.create_task(repeat_a(message.from_user.id))
        else:
            await message.answer("✅ Already running. /stop to stop.")
    else:
        await message.answer("Not authorised.")


@dp.message(Command("stop"))
async def handle_stop(message: types.Message):
    #print(f"[BOT] /stop from user {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id in user_tasks and user_tasks[message.from_user.id]:
            user_tasks[message.from_user.id].cancel()
            user_tasks.pop(message.from_user.id)
            await message.answer("Stopped. /start?")
        else:
            await message.answer("Not running. /start?")
    else:
        await message.answer("Not authorised.")


@dp.message(Command("timer"))
async def handle_timer(message: types.Message):
    #print(f"[BOT] /timer from user {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in timer_tasks or not timer_tasks[message.from_user.id]:
            await message.answer("🚀 Timer started!")
            timer_tasks[message.from_user.id] = asyncio.create_task(timer_repeat(message.from_user.id))
        else:
            await message.answer("Already running. /top to stop.")
    else:
        await message.answer("Not authorised.")


@dp.message(Command("top"))
async def handle_top(message: types.Message):
    #print(f"[BOT] /top from user {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id in timer_tasks and timer_tasks[message.from_user.id]:
            timer_tasks[message.from_user.id].cancel()
            timer_tasks.pop(message.from_user.id)
            await message.answer("Timer stopped. /timer?")
        else:
            await message.answer("Not running.")


@dp.message(Command("retry"))
async def handle_retry(message: types.Message):
    #print(f"[BOT] /retry from user {message.from_user.id}")
    if message.from_user.id in ADMIN_IDS:
        if message.from_user.id not in user_tasks or not user_tasks[message.from_user.id]:
            await message.answer("Retrying...")
            user_tasks[message.from_user.id] = asyncio.create_task(retry_a(message.from_user.id))
        else:
            await message.answer("Already running. /stop?")
    else:
        await message.answer("Not authorised.")


@dp.message(Command("now"))
async def handle_now(message: types.Message):
    try:
        conn, cur = db_connect()
        names = ['a_category', 'a_month', 'a_format', 'a_year', 'a_sort_by']
        res = {}
        for n in names:
            cur.execute("SELECT value, min, max FROM vars WHERE name = %s", (n,))
            row = cur.fetchone()
            res[n] = row
        db_close(conn, cur)
        await message.answer(
            f"subject: {res['a_category'][0]} [{res['a_category'][1]}-{res['a_category'][2]}]\n"
            f"month: {res['a_month'][0]} [{res['a_month'][1]}-{res['a_month'][2]}]\n"
            f"year: {res['a_year'][0]}\n"
            f"format: {res['a_format'][0]} [{res['a_format'][1]}-{res['a_format'][2]}]\n"
            f"sort: {res['a_sort_by'][0]} [{res['a_sort_by'][1]}-{res['a_sort_by'][2]}]"
        )
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("last"))
async def handle_last(message: types.Message):
    try:
        conn, cur = db_connect()
        cur.execute("SELECT * FROM amazon_books ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        db_close(conn, cur)
        await message.answer(f"Last: {row}" if row else "No records.")
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("total"))
async def handle_total(message: types.Message):
    try:
        conn, cur = db_connect()
        cur.execute("SELECT COUNT(*) FROM amazon_books WHERE id > 0")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM amazon_books WHERE id > 0 AND status = 'xls'")
        xls = cur.fetchone()[0]
        db_close(conn, cur)
        await message.answer(f"Total: {total}\nXLS: {xls}")
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("xls"))
async def handle_xls(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        try:
            doc = FSInputFile(path=os.path.join(script_dir, 'example_amazon_books.xlsx'))
            await bot.send_document(message.from_user.id, document=doc, caption='Example XLS')
        except Exception as e:
            await message.answer(f"Error: {e}")
    else:
        await message.answer("Making XLS...")
        try:
            conn, cur = db_connect()
            cur.execute("SELECT * FROM amazon_books WHERE id > 0")
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=[d[0] for d in cur.description])
            db_close(conn, cur)
            path = os.path.join(script_dir, 'amazon_books.xlsx')
            df.to_excel(path, index=False, engine='openpyxl')
            doc = FSInputFile(path=path)
            await bot.send_document(message.from_user.id, document=doc, caption='Amazon Books XLS')
        except Exception as e:
            await message.answer(f"Error: {e}")


@dp.message(Command("proxy"))
async def check_proxy(message: types.Message):
    await _check_proxy(message)


@dp.message(Command("proxyw"))
async def working_proxy(message: types.Message):
    await _check_proxy(message)


async def _check_proxy(message):
    #print(f"[PROXY] Checking proxy for user {message.from_user.id}")
    manager = WebDriverManager()
    try:
        driver, proxy_info = manager.get_working_proxy_driver()
        await bot.send_message(message.from_user.id, f"proxy: {proxy_info}")
        #print(f"[PROXY] Navigating to whatismyipaddress.com...")
        driver.get('https://whatismyipaddress.com/')
        time.sleep(3)
        try:
            driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div/div[2]/div/button[2]').click()
            #print("[PROXY] Cookie banner dismissed.")
        except:
            print("[PROXY] No cookie banner found (or couldn't dismiss).")
        ip = driver.find_element(By.XPATH, '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[1]/p[2]/span[2]/a').text.strip()
        country = driver.find_element(By.XPATH, '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[3]/div/p[4]/span[2]').text.strip()
        #print(f"[PROXY] IP={ip}, Country={country}")
        await bot.send_message(message.from_user.id, f"IP: {ip}, Country: {country}")
        conn, cur = db_connect()
        cur.execute(f"UPDATE proxies SET comment = CONCAT(comment, ' -{country}') WHERE proxy like '%{ip}%'")
        conn.commit()
        db_close(conn, cur)
    except Exception as e:
        #print(f"[PROXY] ERROR: {e}")
        await bot.send_message(message.from_user.id, f"Error: {e}")
    finally:
        manager.close_driver()


@dp.message(Command("log"))
async def handle_log(message: types.Message):
    try:
        doc = FSInputFile(path=os.path.join(script_dir, 'amazon_a.log'))
        await bot.send_document(message.from_user.id, document=doc, caption='Logs')
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("logd"))
async def handle_logd(message: types.Message):
    path = os.path.join(script_dir, 'amazon_a.log')
    try:
        doc = FSInputFile(path=path)
        await bot.send_document(message.from_user.id, document=doc, caption='Logs')
        await asyncio.sleep(1)
        os.remove(path)
        await message.answer("Log deleted.")
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("py"))
async def handle_py(message: types.Message):
    path = os.path.join(os.path.dirname(__file__), "amaze.py")
    await bot.send_document(message.from_user.id, FSInputFile(path=path), caption="amaze.py \n Full source code: \n https://github.com/serhii-kolodych/Amazon-books")



@dp.message(Command("html"))
async def handle_html(message: types.Message):
    for name in ['last-page', 'main-page', 'no-such-page', 'refresh-page']:
        try:
            doc = FSInputFile(path=f'{name}.html')
            await bot.send_document(message.from_user.id, document=doc, caption=name)
        except Exception as e:
            await message.answer(f"{name}: {e}")


@dp.message(Command("subject"))
async def handle_subject(message: types.Message):
    try:
        cat = fetch_var('a_category')
        subjects = "\n".join(f"{i}. {v}" for i, (k, v) in enumerate(all_subjects.items(), 1))
        await message.answer(f"Current: {cat}\n{subjects}\nWrite: subject 9")
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("month"))
async def handle_month(message: types.Message):
    await message.answer(f"Current month: {fetch_var('a_month')}\nWrite: month 7")


@dp.message(Command("year"))
async def handle_year(message: types.Message):
    await message.answer(f"Current year: {fetch_var('a_year')}\nWrite: year 2023")


@dp.message(Command("format"))
async def handle_format(message: types.Message):
    fmts = "\n".join(f"{i}. {v}" for i, v in enumerate(all_formats.values(), 1))
    await message.answer(f"Current: {fetch_var('a_format')}\n{fmts}\nWrite: format 2")


@dp.message(Command("sort"))
async def handle_sort(message: types.Message):
    await message.answer(f"Current: {fetch_var('a_sort_by')}\n1-6 options\nWrite: sort 3")


@dp.message(Command("help"))
async def handle_help(message: types.Message):
    await message.answer("/now /last /total /xls /proxy /log /subject /month /year /format /sort /start /stop /timer /top /retry")
    await bot.send_message(message.from_user.id, #"/start - ✅ Start from last one\n"
        #"/retry - 🪃 retry from the list\n"
        "/now - 🧐 current search details\n"
        "/last - 🌝 get the latest about author page\n"
        "/total - 👨‍💼 all about author pages \n"
        "/xls - 📊 download xls \n"
        "Full source code: \n"
        "https://github.com/serhii-kolodych/Amazon-books")
        #"\n/stop - ❌ Stop)")

@dp.message()
async def handle_text(message: types.Message):
    #print(f"[BOT] Text message from {message.from_user.id}: {message.text!r}")
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Not authorised.")
        return

    text_lower = message.text.lower()
    cmds = {'month': 6, 'subject': 8, 'format': 7, 'sort': 5, 'year': 5}
    var_map = {'month': 'a_month', 'subject': 'a_category', 'format': 'a_format', 'sort': 'a_sort_by', 'year': 'a_year'}

    for cmd, offset in cmds.items():
        if text_lower.startswith(cmd + ' '):
            try:
                update_var(var_map[cmd], message.text[offset:])
                await message.answer(f"Changed {cmd} to {message.text[offset:]}")
            except Exception as e:
                await message.answer(f"Error: {e}")
            return

    if text_lower.startswith('start '):
        match = re.match(r'^start (\d+) (\d+)', text_lower)
        if match:
            await start_a(message.from_user.id, int(match.group(1)), match.group(2))
        else:
            await message.answer("Use: start <category> <month>")
        return

    if text_lower.startswith('retry '):
        match = re.match(r'^retry (\d+) (\d+)', text_lower)
        if match:
            try:
                conn, cur = db_connect()
                cur.execute("INSERT INTO amazon_retry (category, month) VALUES (%s, %s)", (match.group(1), match.group(2)))
                conn.commit()
                db_close(conn, cur)
                await message.answer("Added to retry queue.")
            except Exception as e:
                await message.answer(f"Error: {e}")
        else:
            await message.answer("Use: retry <category> <month>")
        return

    await message.answer("Unknown command.")


# ── Core logic ─────────────────────────────────────────────────────────────────

async def timer_repeat(chat_id):
    #print(f"[TIMER] timer_repeat started for chat_id={chat_id}")
    while chat_id in timer_tasks and timer_tasks[chat_id]:
        await repeat_a(chat_id)
        await bot.send_message(chat_id, "Sleeping 60 min.")
        #print(f"[TIMER] Sleeping 60 minutes...")
        await asyncio.sleep(3600)


async def repeat_a(chat_id):
    #print(f"[REPEAT] repeat_a called for chat_id={chat_id}")
    try:
        result_now = int(fetch_var('a_category'))
        a_month = int(fetch_var('a_month'))
        a_year = int(fetch_var('a_year'))
        result_max = 15
        #print(f"[REPEAT] category={result_now}, month={a_month}, year={a_year}")

        # Phase 1: months 9/2025 → 3/2026 — run normally
        # Phase 2: month 4+ (2026+) — cycle format 1→2→3→1, and when format resets to 1 also bump sort
        # Never fully stop — just keep going in Phase 2 indefinitely

        await bot.send_message(chat_id, f"🏀 Starting subject {result_now} month {a_month} year {a_year}")
        await start_a(chat_id, result_now, str(a_month))
    except Exception as e:
        #print(f"[REPEAT] ERROR: {e}")
        logger.error(f"repeat_a: {e}")
        await bot.send_message(chat_id, f"repeat_a error: {e}")


async def retry_a(chat_id):
    #print(f"[RETRY] retry_a started for chat_id={chat_id}")
    while True:
        try:
            conn, cur = db_connect()
            cur.execute("SELECT * FROM amazon_retry ORDER BY id ASC")
            row = cur.fetchone()
            if not row:
                #print("[RETRY] No rows in retry queue. Done.")
                db_close(conn, cur)
                break
            month, category = row[2], row[1]
            #print(f"[RETRY] Processing: category={category}, month={month}")
            cur.execute("DELETE FROM amazon_retry WHERE id = %s", (row[0],))
            conn.commit()
            db_close(conn, cur)
            await bot.send_message(chat_id, f"Retry {category} {month}")
            await start_a(chat_id, category, str(month))
        except Exception as e:
            #print(f"[RETRY] ERROR: {e}")
            await bot.send_message(chat_id, f"retry_a error: {e}")
            break


async def start_a(chat_id, subject_int, month):
    #print(f"[SCRAPER] start_a called: subject_int={subject_int}, month={month}, chat_id={chat_id}")
    async with aiohttp.ClientSession() as session:
        manager = WebDriverManager()
        driver = None
        page = 1
        try:
            #print("[SCRAPER] Getting driver with proxy...")
            driver, proxy_info = manager.get_working_proxy_driver()
            #print(f"[SCRAPER] Proxy info: {proxy_info}")

            #print("[SCRAPER] Navigating to Amazon advanced search...")
            driver.get('https://www.amazon.com/advanced-search/books')
            #print("[SCRAPER] Page loaded.")

            subject = str(dicti[subject_int])
            fmt = fetch_format()
            sort_by = fetch_sort_by()
            year = fetch_year()
            #print(f"[SCRAPER] Search params: subject={subject}, fmt={fmt}, sort_by={sort_by}, year={year}, month={month}")

            #print("[SCRAPER] Setting form fields...")
            Select(driver.find_element(By.XPATH, x_published_date)).select_by_visible_text('During')
            #print("[SCRAPER] Set published_date=During")
            Select(driver.find_element(By.XPATH, x_month)).select_by_value(month)
            #print(f"[SCRAPER] Set month={month}")
            Select(driver.find_element(By.XPATH, x_sort)).select_by_value(sort_by)
            #print(f"[SCRAPER] Set sort={sort_by}")
            year_input = driver.find_element(By.XPATH, x_year)
            year_input.clear()
            year_input.send_keys(year)
            #print(f"[SCRAPER] Set year={year}")
            Select(driver.find_element(By.XPATH, x_subject)).select_by_value(subject)
            #print(f"[SCRAPER] Set subject={subject}")
            Select(driver.find_element(By.XPATH, x_condition)).select_by_value('1294423011')
            #print("[SCRAPER] Set condition.")
            Select(driver.find_element(By.XPATH, x_format)).select_by_visible_text(fmt)
            #print(f"[SCRAPER] Set format={fmt}")
            Select(driver.find_element(By.XPATH, x_language)).select_by_value('English')
            #print("[SCRAPER] Set language=English")
            time.sleep(1)
            #print("[SCRAPER] Clicking search button...")
            driver.find_element(By.XPATH, x_search_button).click()
            time.sleep(3)
            #print("[SCRAPER] Search submitted, results loading...")

            search_query = f"{page}page {subject_int}sub {month}: {year} {fmt} sort:{sort_by}"
            #print(f"[SCRAPER] search_query={search_query}")

            # update category/month counter
            try:
                subject_new = subject_int + 1
                if subject_new <= 15:
                    update_var('a_category', subject_new)
                    await bot.send_message(chat_id, f"subject = {subject_new}")
                    #print(f"[SCRAPER] Counter updated: subject={subject_new}")
                else:
                    # Subject cycle complete — reset subject to 2, advance month
                    update_var('a_category', 2)
                    a_month_new = int(month) + 1
                    a_year_now = int(fetch_var('a_year'))

                    # ── Year rollover: Dec 2025 → Jan 2026 ──
                    if a_month_new == 13:
                        a_month_new = 1
                        a_year_new = a_year_now + 1
                        update_var('a_year', a_year_new)
                        update_var('a_month', a_month_new)
                        await bot.send_message(chat_id, f"🎉 Year rollover! year={a_year_new}, month={a_month_new}")
                        #print(f"[SCRAPER] Year rollover: year={a_year_new}, month={a_month_new}")

                    # ── Round reset: subject 15 just finished month 3 of 2026 → reset month to DB min, year to 2025, cycle format/sort ──
                    elif int(month) == 3 and a_year_now == 2026:
                        conn, cur = db_connect()
                        cur.execute("SELECT min FROM vars WHERE name = 'a_month'")
                        month_min = int(cur.fetchone()[0])
                        db_close(conn, cur)

                        fmt_now = int(fetch_var('a_format'))
                        fmt_new = fmt_now + 1

                        if fmt_new > 3:
                            # Format wraps to 1 — also advance sort
                            fmt_new = 1
                            sort_now = int(fetch_var('a_sort_by'))
                            sort_new = sort_now + 1

                            if sort_new > 6:
                                # Sort 6 was already used — this is the END
                                update_var('a_format', fmt_new)
                                update_var('a_sort_by', sort_new)
                                await bot.send_message(chat_id, f"🏁 ALL DONE! Sort reached end (6). Stopping.")
                                #print(f"[SCRAPER] END: sort exceeded 6. Stopping.")
                                if chat_id in user_tasks and user_tasks[chat_id]:
                                    user_tasks[chat_id].cancel()
                                    user_tasks.pop(chat_id, None)
                                return
                            else:
                                update_var('a_format', fmt_new)
                                update_var('a_sort_by', sort_new)
                                update_var('a_month', month_min)
                                update_var('a_year', 2025)
                                await bot.send_message(chat_id, f"🔁 Round complete! format=1, sort={sort_new}, reset to month={month_min}, year=2025")
                                #print(f"[SCRAPER] Round reset: format=1, sort={sort_new}, month={month_min}, year=2025")
                        else:
                            # Format still cycling (1→2 or 2→3)
                            update_var('a_format', fmt_new)
                            update_var('a_month', month_min)
                            update_var('a_year', 2025)
                            await bot.send_message(chat_id, f"🔁 Round complete! format={fmt_new}, reset to month={month_min}, year=2025")
                            #print(f"[SCRAPER] Round reset: format={fmt_new}, month={month_min}, year=2025")

                    # ── Phase 2: month 4+ → cycle format 1→2→3→1, sort bumps on wrap ──
                    elif a_month_new >= 4 and a_year_now >= 2026:
                        update_var('a_month', a_month_new)
                        fmt_now = int(fetch_var('a_format'))
                        fmt_new = fmt_now + 1
                        if fmt_new > 3:
                            fmt_new = 1
                            # Format wrapped back to 1 — also increment sort
                            sort_now = int(fetch_var('a_sort_by'))
                            sort_new = sort_now + 1
                            update_var('a_sort_by', sort_new)
                            update_var('a_format', fmt_new)
                            await bot.send_message(chat_id, f"🔄 Format reset to 1, sort bumped to {sort_new}, month={a_month_new}")
                            #print(f"[SCRAPER] Phase2: format=1, sort={sort_new}, month={a_month_new}")
                        else:
                            update_var('a_format', fmt_new)
                            await bot.send_message(chat_id, f"🔄 Format → {fmt_new}, month={a_month_new}")
                            #print(f"[SCRAPER] Phase2: format={fmt_new}, month={a_month_new}")

                    # ── Phase 1: normal month advance (months 9/2025 → 3/2026) ──
                    else:
                        update_var('a_month', a_month_new)
                        await bot.send_message(chat_id, f"subject reset, month = {a_month_new}")
                        #print(f"[SCRAPER] Counter reset: subject=2, month={a_month_new}")

            except Exception as e:
                #print(f"[SCRAPER] Counter update ERROR: {e}")
                await bot.send_message(chat_id, f"Counter error: {e}")

            seen_links = set()

            def scrape_current_items():
                saved = 0
                items = driver.find_elements(By.XPATH, '/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div')
                #print(f"[SCRAPER] scrape_current_items: found {len(items)} raw items on page {page}")
                for item in items:
                    try:
                        xtitle   = './/div[contains(@class,"sg-col-inner")]//div[contains(@class,"a-section")]//a[contains(@href,"/dp/") or contains(@href,"/stores/")]'
                        xauthors = [
                            './/div[contains(@class,"a-row")]//a[2]',
                            './/div[contains(@class,"a-row")]//a[1]',
                            './/div[contains(@class,"a-row")]//a',
                        ]

                        try:
                            title_el = item.find_element(By.XPATH, './/div/div/span/div/div/div/div[2]/div/div/div[1]/a')
                        except NoSuchElementException:
                            continue

                        title_element = None
                        for xp in [
                            './/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a[2]',
                            './/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a[1]',
                            './/div/div/span/div/div/div/div[2]/div/div/div[1]/div/div/a',
                        ]:
                            try:
                                title_element = item.find_element(By.XPATH, xp)
                                break
                            except NoSuchElementException:
                                pass
                        if title_element is None:
                            #print(f"[SCRAPER] No title_element found for item, skipping.")
                            continue

                        link = title_element.get_attribute("href")
                        if not link:
                            #print(f"[SCRAPER] Empty href on title_element, skipping.")
                            continue

                        if link.startswith("https://www.amazon.com/dp/"):
                            link = title_el.get_attribute("href")

                        asin = None
                        parts = link.split("/")
                        if "author" in parts:
                            try: asin = parts[parts.index("author") + 1].split("?")[0]
                            except:
                                #print(f"[SCRAPER] Failed to parse author ASIN from link: {link}")
                                continue
                        elif "/e/" in link:
                            try: asin = parts[-1].split("?")[0]
                            except:
                                #print(f"[SCRAPER] Failed to parse /e/ ASIN from link: {link}")
                                continue
                        else:
                            #print(f"[SCRAPER] Link not author/store format, skipping: {link}")
                            continue

                        final_link = f"https://www.amazon.com/stores/author/{asin}/about"
                        if final_link in seen_links:
                            continue
                        seen_links.add(final_link)
                        #print(f"✅ {final_link}")

                        try:
                            conn, cur = db_connect()
                            cur.execute("""
                                INSERT INTO amazon_books (query, page, about_link, status)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (about_link)
                                DO UPDATE SET query = EXCLUDED.query, page = EXCLUDED.page
                            """, (search_query, page, final_link, 'new'))
                            conn.commit()
                            db_close(conn, cur)
                            saved += 1
                        except Exception as e:
                            print(f"[SCRAPER] DB INSERT error: {e}")
                    except Exception as ex:
                        print(f"[SCRAPER] Unexpected item error: {ex}")
                        continue
                #print(f"[SCRAPER] scrape_current_items: saved {saved} new links.")
                return saved

            # detect mode: paginated (next button exists) or infinite scroll
            X_NEXT = [
                "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/ul/li[5]/span/a",
                "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[20]/div/div/span/ul/li[5]/span/a",
                "/html/body/div[1]/div[1]/div[1]/div[1]/div/span[1]/div[1]/div[18]/div/div/span/ul/li[7]/span/a",
            ]

            def find_next_button():
                for xp in X_NEXT:
                    try:
                        btn = driver.find_element(By.XPATH, xp)
                        if btn.is_displayed() and btn.is_enabled():
                            #print(f"[SCRAPER] Next button found: {xp}")
                            return btn
                    except NoSuchElementException:
                        pass
                #print("[SCRAPER] No next button found (infinite scroll mode or last page).")
                return None

            while page < 100:
                try:
                    res_span = driver.find_element(By.XPATH, "/html/body/div[1]/div[1]/span/div/h1/div/div[1]/div/h2/span")
                    result_text = res_span.text.strip()
                except NoSuchElementException:
                    result_text = ""
                    #print(f"[SCRAPER] Result count span not found on page {page}.")

                #print(f"[SCRAPER] Page {page} result text: {result_text!r}")
                # await bot.send_message(chat_id, f"page {page}: {result_text}")

                # save page html on page 1
                if page == 1:
                    try:
                        with open(f'{page}.html', 'w', encoding='utf-8') as f:
                            f.write(driver.page_source)
                        doc = FSInputFile(path=f'{page}.html')
                        # await bot.send_document(chat_id, document=doc, caption=f'{page}.html')
                        os.remove(f'{page}.html')
                        #print("[SCRAPER] Page 1 HTML sent and deleted.")
                    except Exception as e:
                        #print(f"[SCRAPER] HTML send error: {e}")
                        await bot.send_message(chat_id, f"html error: {e}")

                saved = scrape_current_items()
                # await bot.send_message(chat_id, f"{saved} new links - page {page}")

                next_btn = find_next_button()

                if next_btn:
                    # paginated mode
                    try:
                        #print(f"[SCRAPER] Clicking next button for page {page + 1}...")
                        next_btn.click()
                        sleep(3)
                        page += 1
                        #print(f"[SCRAPER] Now on page {page}.")
                    except Exception as e:
                        #print(f"[SCRAPER] Next button click failed: {e}")
                        await bot.send_message(chat_id, f"Next button click failed: {e}")
                        break
                else:
                    # infinite scroll mode
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    #print(f"[SCRAPER] Scrolling... current height={last_height}")
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(3)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    #print(f"[SCRAPER] After scroll height={new_height}")

                    if new_height == last_height:
                        #print(f"[SCRAPER] No new content after scroll. Bottom reached.")
                        await bot.send_message(chat_id, f"Bottom reached. Done. {page} scrolls.")
                        break

                    page += 1
                    #print(f"[SCRAPER] Scroll {page} loaded new content.")

            #print(f"[SCRAPER] Loop finished. Total unique links: {len(seen_links)}")
            await bot.send_message(chat_id, f"⚽️ Finished. Total unique links: {len(seen_links)}")

        except Exception as e:
            msg = str(e)
            idx = msg.lower().find("stacktrace")
            short_msg = msg[:idx] if idx != -1 else msg
            #print(f"[SCRAPER] FATAL ERROR: {short_msg}")
            await bot.send_message(chat_id, f"Error: {short_msg}")
            try:
                with open(f'{page}.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                doc = FSInputFile(path=f'{page}.html')
                # await bot.send_document(chat_id, document=doc)
                os.remove(f'{page}.html')
                #print(f"[SCRAPER] Error HTML page {page} sent.")
            except Exception as dump_err:
                print(f"[SCRAPER] Could not dump error HTML: {dump_err}")

        finally:
            print("[SCRAPER] Cleaning up in finally block...")
            try:
                manager.close_driver()
            except Exception as e:
                print(f"[SCRAPER] close_driver error: {e}")
            try:
                await session.close()
            except Exception as e:
                print(f"[SCRAPER] session.close error: {e}")
            #print("[SCRAPER] start_a finished. Auto-restarting in 5s...")
            await asyncio.sleep(5)
            if chat_id in user_tasks and user_tasks[chat_id]:
                #print("[SCRAPER] Auto-restarting repeat_a...")
                user_tasks[chat_id] = asyncio.create_task(repeat_a(chat_id))


async def main():
    #print("[MAIN] Starting bot polling...")
    await dp.start_polling(Bot(token=TOKEN), skip_updates=True)

if __name__ == '__main__':
    #print("[MAIN] Script started.")
    asyncio.run(main())