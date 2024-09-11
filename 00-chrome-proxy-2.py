import psycopg2
import config_a
from seleniumbase import Driver
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from datetime import datetime
import asyncio


TOKEN = config_a.TOKEN
conn_params = config_a.conn_params
bot = Bot(TOKEN)
dp = Dispatcher()

class WebDriverManager:
    _instance = None

    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.conn = None
        self.cursor = None

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
            proxy_string = proxy_info[1]
            self.update_proxy_info(proxy_info[0])
            print("proxy_string= ", proxy_string)
            return proxy_string
        return None

    def get_random_user_agent(self):
        return UserAgent().random

    def create_web_driver(self, proxy_string, user_agent):
        return Driver(browser="chrome", uc=True, proxy=proxy_string, agent=user_agent)

    def fetch_proxy_from_database(self):
        try:
            self.connect()
            query_select = "SELECT * FROM proxies WHERE deleted = false AND comment LIKE %s ORDER BY date ASC LIMIT 1"
            self.cursor.execute(query_select, ('%webshare%',))
            return self.cursor.fetchone()
        except Exception as e:
            print(f"Error fetching proxy: {e}")
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
            print(f"Error updating proxy info: {e}")
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

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer('hello ;) /proxy?')

@dp.message(Command("proxy"))
async def check_proxy(message: types.Message):
    try:
        url = 'https://whatismyipaddress.com/'
        await bot.send_message(message.from_user.id, f"🌈 starting driver to check proxy")
        manager = WebDriverManager(conn_params)
        driver = manager.get_driver()
        driver.get(url)
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
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
