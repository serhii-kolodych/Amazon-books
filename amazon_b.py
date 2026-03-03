# pip install beautifulsoup4 selenium-base selenium fake-useragent pandas aiohttp aiogram sqlalchemy asyncio pymysql openpyxl psycopg2
import asyncio
import os
import re
import logging
from io import BytesIO
from datetime import datetime

import psycopg2
import pandas as pd
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import FSInputFile
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from seleniumbase import Driver

import config_a
from dictionaries import all_subjects, months, sort_bys, all_formats, dicti

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOKEN = config_a.TOKEN_B
CONN_PARAMS = config_a.conn_params
ADMIN_IDS = config_a.ADMIN_IDS

bot = Bot(TOKEN)
dp = Dispatcher()
user_tasks: dict = {}

LINK_PREFIXES = ['@', 'http', 'www', '.com', '.co', '.org', '.net', '.ca',
                 '.info', '.xyz', '.ly', '.site', '.me', '.ru', '.de',
                 '.uk', '.in', '.online', '.us', '.ai', '.cc']


# --- DB helpers ---

def db_connect():
    return psycopg2.connect(**CONN_PARAMS)


def db_fetch(query: str, params=None):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def db_fetchone(query: str, params=None):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()


def db_execute(query: str, params=None):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


# --- Driver ---

def create_driver() -> Driver:
    ua = UserAgent().random
    return Driver(browser="safari", headless=True, uc=True, agent=ua)


# --- Bot commands ---

@dp.message(Command("start"))
async def handle_start(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("Not authorised.")
        return
    if uid in user_tasks and not user_tasks[uid].done():
        await message.answer("Already running. Use /stop to stop.")
        return
    await message.answer("Started.")
    user_tasks[uid] = asyncio.create_task(run_loop(uid))


@dp.message(Command("stop"))
async def handle_stop(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("Not authorised.")
        return
    task = user_tasks.get(uid)
    if task and not task.done():
        task.cancel()
        user_tasks.pop(uid)
        await message.answer("Stopped.")
    else:
        await message.answer("Not running. Use /start.")


@dp.message(Command("now"))
async def handle_now(message: types.Message):
    row = db_fetchone(
        "SELECT * FROM amazon_books WHERE status = 'new' ORDER BY id ASC LIMIT 1"
    )
    await message.answer(f"Now: {row}" if row else "Nothing in queue.")


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
async def handle_status(message: types.Message):
    rows = db_fetch("SELECT status, COUNT(*) FROM amazon_books GROUP BY status")
    text = "\n".join(f"{r[0]}: {r[1]}" for r in rows)
    await message.answer(f"Statuses:\n{text}")


@dp.message(Command("xls"))
async def handle_xls(message: types.Message):
    await message.answer("Building XLS...")
    rows = db_fetch(
        "SELECT id, query, page, about_link, author, "
        "link1, link2, link3, link4, link5, link6, link7, link8, link9 "
        "FROM amazon_books WHERE status = 'links' ORDER BY id ASC"
    )
    df = _build_links_df(rows)
    path = "/tmp/amazon_books.xlsx"
    df.to_excel(path, index=False, engine="openpyxl")
    await bot.send_document(message.from_user.id,
                            FSInputFile(path=path),
                            caption=f"{len(df)} rows")


@dp.message(Command("py"))
async def handle_py(message: types.Message):
    path = os.path.join(os.path.dirname(__file__), "amazon_b.py")
    await bot.send_document(message.from_user.id, FSInputFile(path=path), caption="amazon_b.py")


@dp.message(Command("help"))
async def handle_help(message: types.Message):
    await message.answer(
        "/start - start scanning\n"
        "/stop - stop\n"
        "/now - current item\n"
        "/total - counts\n"
        "/status - status breakdown\n"
        "/xls - download results\n"
        "/py - get source file"
    )


@dp.message()
async def handle_text(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("Not authorised.")
        return

    text = message.text.strip()

    if text.isdigit():
        if uid not in user_tasks or user_tasks[uid].done():
            user_tasks[uid] = asyncio.create_task(run_loop(uid, int(text)))
            await message.answer(f"Started from ID {text}.")
        else:
            await message.answer("Already running. Use /stop first.")

    elif text.lower().startswith("xls "):
        start_id = text[4:].strip()
        await message.answer(f"Building XLS from ID {start_id}...")
        rows = db_fetch(
            "SELECT id, query, page, about_link, author, "
            "link1, link2, link3, link4, link5, link6, link7, link8, link9 "
            "FROM amazon_books WHERE id > %s AND status = 'links' ORDER BY id ASC",
            (start_id,)
        )
        df = _build_links_df(rows)
        path = f"/tmp/amazon_books_{start_id}.xlsx"
        df.to_excel(path, index=False, engine="openpyxl")
        await bot.send_document(uid, FSInputFile(path=path),
                                caption=f"{len(df)} rows (ID > {start_id})")

    elif text.lower().startswith("delete "):
        target_id = text[7:].strip()
        db_execute("DELETE FROM amazon_books WHERE id = %s", (target_id,))
        await message.answer(f"Deleted ID {target_id}.")

    elif text.lower().startswith("offset "):
        try:
            delta = int(text[7:].strip())
            row = db_fetchone("SELECT value FROM vars WHERE name = 'offset'")
            new_offset = int(row[0].strip()) + delta
            db_execute("UPDATE vars SET value = %s WHERE name = 'offset'", (new_offset,))
            await message.answer(f"Offset updated to {new_offset}.")
        except (ValueError, TypeError):
            await message.answer("Provide a valid number after 'offset'.")


# --- Core scraping loop ---

async def run_loop(chat_id: int, start_id: int = None):
    driver = None
    try:
        if start_id is None:
            row = db_fetchone(
                "SELECT id FROM amazon_books WHERE status = 'new' ORDER BY id ASC LIMIT 1"
            )
            start_id = int(row[0]) if row else 0

        driver = create_driver()
        await bot.send_message(chat_id, f"Scanning from ID {start_id}.")

        while True:
            rows = db_fetch(
                "SELECT id, about_link, link1, link2, link3, link4, link5, "
                "link6, link7, link8, link9 "
                "FROM amazon_books WHERE status = 'new' AND id >= %s ORDER BY id ASC LIMIT 50",
                (start_id,)
            )
            if not rows:
                await bot.send_message(chat_id, "Queue empty. Waiting 60s...")
                await asyncio.sleep(60)
                continue

            for row in rows:
                row_id, author_url = row[0], row[1]
                try:
                    await scrape_author(driver, chat_id, row_id, author_url)
                except Exception as e:
                    log.error(f"Error on ID {row_id}: {e}")
                    # Recreate driver on timeout/connection errors
                    if "timed out" in str(e).lower() or "connection" in str(e).lower():
                        log.info("Recreating driver after timeout...")
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        await asyncio.sleep(5)
                        driver = create_driver()
                    # Mark as error so we don't loop forever on bad URLs
                    db_execute(
                        "UPDATE amazon_books SET status = 'error' WHERE id = %s",
                        (row_id,)
                    )

                start_id = row_id + 1
                await asyncio.sleep(0.5)

    except asyncio.CancelledError:
        log.info(f"Task cancelled for {chat_id}")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        await bot.send_message(chat_id, f"Fatal error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        user_tasks.pop(chat_id, None)
        await bot.send_message(chat_id, "Stopped.")


async def scrape_author(driver: Driver, chat_id: int, row_id: int, author_url: str):
    log.info(f"Checking ID {row_id}: {author_url}")

    driver.get(author_url)
    status = "text"
    author_name = ""
    author_links = []

    try:
        author_name = driver.find_element(By.TAG_NAME, "h1").text
        author_div = driver.find_element(
            By.XPATH, "/html/body/div[1]/div[1]/div/div[4]/div/div/div/div/div[2]"
        )
        p_elements = author_div.find_elements(By.TAG_NAME, "p")

        if not p_elements:
            status = "empty"
        else:
            for p in p_elements:
                words = p.text.split()
                links = [w for w in words if any(pfx in w.lower() for pfx in LINK_PREFIXES)]
                author_links.extend(links)
            status = "links" if author_links else "text"

    except NoSuchElementException:
        # Check for 404 image redirect
        try:
            img = driver.find_element(By.XPATH, "/html/body/div/div/a/img")
            if img.get_attribute("src"):
                status = "404"
        except NoSuchElementException:
            status = "error"
        log.warning(f"Div not found for ID {row_id}, status={status}")

    # Write results
    if author_links:
        padded = (author_links + [''] * 9)[:9]
        db_execute(
            "UPDATE amazon_books SET status=%s, author=%s, "
            "link1=%s, link2=%s, link3=%s, link4=%s, link5=%s, "
            "link6=%s, link7=%s, link8=%s, link9=%s WHERE id=%s",
            (status, author_name, *padded, row_id)
        )
        await bot.send_message(
            chat_id,
            f"ID {row_id}: {len(author_links)} links found\n{author_url}"
        )
    else:
        db_execute(
            "UPDATE amazon_books SET status=%s, author=%s WHERE id=%s",
            (status, author_name, row_id)
        )

    log.info(f"ID {row_id} => {status}")


# --- Helpers ---

def _build_links_df(rows) -> pd.DataFrame:
    SOCIAL = {
        'facebook': 'facebook.com',
        'instagram': 'instagram.com',
        'twitter': 'twitter.com',
        'linkedin': 'linkedin.com',
    }
    processed = []
    for row in rows:
        base = list(row[:5])
        links = list(row[5:14])
        categorized = {k: '' for k in ('facebook', 'instagram', 'twitter', 'linkedin', 'email')}
        remaining = []
        for link in links:
            if not link:
                continue
            matched = False
            for key, domain in SOCIAL.items():
                if domain in link:
                    categorized[key] = link
                    matched = True
                    break
            if not matched:
                if '@' in link:
                    categorized['email'] = link
                else:
                    remaining.append(link)
        processed.append(base + list(categorized.values()) + (remaining + [''] * 9)[:9])

    cols = ['id', 'query', 'page', 'about_link', 'author',
            'Facebook', 'Instagram', 'Twitter', 'LinkedIn', 'Email'] + \
           [f'link{i}' for i in range(1, 10)]
    return pd.DataFrame(processed, columns=cols)


# --- Entry point ---

async def main():
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())