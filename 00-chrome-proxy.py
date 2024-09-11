# Importing required modules
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import asyncio
# Your other imports remain the same...
import config_a
from datetime import datetime
# Configuration details
TOKEN = config_a.TOKEN
global conn_params

conn_params = config_a.conn_params

proxy_comment = "webshare"  # proxy6 OR webshare
version = f"\n✅ 28 Jan \nProxy: {proxy_comment}\n✅ 22 Jan \n- Added config_a \n - for every page (72) * SLEEP 2 sec = 2 min total \n"

from seleniumbase import Driver
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import logging
import time
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By

# Setting up the bot
bot = Bot(TOKEN)
dp = Dispatcher()

# SQLAlchemy engine and session setup
engine = create_engine(f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
Session = sessionmaker(bind=engine)


class WebDriverManager:
    _instance = None

    def __init__(self):
        self.session = None

    def get_driver(self):
        if not self._instance:
            proxy_string = self.get_one_proxy()
            user_agent = self.get_random_user_agent()
            print(f"-->proxy={proxy_string}, -->agent={user_agent}")
            self._instance = self.create_web_driver(proxy_string, user_agent)
        return self._instance

    def close_driver(self):
        if self._instance:
            self._instance.quit()
            self._instance = None
            print("-->(WebDriverManager) - Driver closed")

    def get_one_proxy(self):
        proxy_info = self.fetch_proxy_from_database()
        if proxy_info:
            proxy_string = proxy_info[1]  # Access the proxy string by its index in the tuple
            self.update_proxy_info(proxy_info[0])  # Access the proxy ID by its index in the tuple
            print("proxy_string= ", proxy_string)
            return proxy_string
        else:
            return None

    def get_random_user_agent(self):
        user_agent = UserAgent()
        return user_agent.random

    def create_web_driver(self, proxy_string, user_agent):
        driver = Driver(browser="chrome", headless=True, uc=True, proxy=proxy_string, agent=user_agent)
        return driver

    def fetch_proxy_from_database(self):
        try:
            self.session = Session()
            query = text("SELECT id, proxy FROM proxies WHERE deleted = false AND comment LIKE :comment ORDER BY date ASC LIMIT 1")
            result = self.session.execute(query, {'comment': f'%{proxy_comment}%'}).fetchone()
            return result
        except Exception as e:
            print(f"Error fetching proxy: {e}")
            return None
        finally:
            self.session.close()

    def update_proxy_info(self, proxy_id):
        try:
            self.session = Session()
            query = text("UPDATE proxies SET date = :date, count = count + 1 WHERE id = :id")
            self.session.execute(query, {'date': datetime.now(), 'id': proxy_id})
            self.session.commit()
        except Exception as e:
            print(f"Error updating proxy info: {e}")
        finally:
            self.session.close()


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer('hello ;) /proxy?')

@dp.message(Command("proxy"))
async def check_proxy(message: types.Message):
    try:
        url = 'https://whatismyipaddress.com/'
        await bot.send_message(message.from_user.id, f"🌈 starting driver to check proxy")

        manager = WebDriverManager()
        driver = manager.get_driver()

        driver.get(url)
        time.sleep(4)  # Sleep for 4 seconds to ensure the page loads

        x_agree_button = '/html/body/div[1]/div/div/div/div[2]/div/button[3]/span'
        driver.find_element(By.XPATH, x_agree_button).click()

        x_path_ip = '/html/body/div[2]/div/div/div/div/article/div/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div[1]/div[1]/p[2]/span[2]/a'
        ip_adress = driver.find_element(By.XPATH, x_path_ip).text.strip()
        await bot.send_message(message.from_user.id, f"ip_adress: {ip_adress}")
    except Exception as e:
        await bot.send_message(message.from_user.id, f"Exception proxy: {e}")
    finally:
        try:
            manager.close_driver()
        except Exception as e:
            await bot.send_message(message.from_user.id, f"couldn't close driver after proxy check: {e}")


async def main():
    # Create a Bot instance with the specified token (needed to send messages without user's message)
    bot = Bot(token=TOKEN)
    # Start polling for updates using the Dispatcher
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
