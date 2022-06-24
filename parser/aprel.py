import glob
import json
import os
import asyncio
from playwright.async_api import async_playwright
import aiofiles
from aiocsv import AsyncReader, AsyncDictReader, AsyncWriter, AsyncDictWriter
import csv

class Aprel():
    settings = {
        "Республика Башкортостан": {
            "Уфа": 8572,
            "Белебей": 9507,
            "Давлеканово": 13198,
            "Нефтекамск": 8637,
            "Кумертау": 8650,
            "Кармаскалы": 11083,
            "Октябрьский": 8645,
            "Толбазы": 9009,
            "Туймазы": 12403,
            "Стерлитамак": 541,
            "Салават": 8646,
            "Булгаково": 8658,
            "Ишимбай": 10898,
            "Дюртюли": 13292,
            "Янаул": 13012,
            "Федоровка": 12492,
            "Киргиз-Мияки": 11815,
            "Месягутово": 10341,
            "Красноусольский": 10244,
            "Краснохолмский": 10900,
            "Верхнеяркеево": 10719,
            "Ермолаево": 11439,
            "Сибай": 8648,
            "Белорецк": 9655,
            "Мелеуз": 11688,
            "Благовещенск": 10030,
            "Бирск": 9823,
            "Кандры": 12397,
            "Улукулево": 11125,
            "Бураево": 10114,
            "Старобалтачево": 9339,
            "Верхние Татышлы": 12207,
            "Большеустьикинское": 11689,
            "Караидель": 10979,
            "Аскино": 8934,
            "Бакалы": 9240,
            "Куяново": 11246,
            "Раевский": 8757
        },
        "Республика Татарстан": {
            "Мензелинск": 17456,
            "Буинск": 16116,
            "Тюлячи": 18291,
            "Лениногорск": 17217,
            "Балтаси": 15863,
            "Новошешминск": 17602,
            "ж/д станции Высокая Гора": 16218,
            "Чистополь": 18454,
            "Алексеевское": 15346,
            "Аксубаево": 15177,
            "Набережные Челны": 661,
            "Тетюши": 18132,
            "Казань": 15005,
            "Бавлы": 15862,
            "Большие Кайбицы": 16847,
            "Нижнекамск": 17597,
            "Джалиль": 17943,
            "Камские Поляны": 17549,
            "Верхний Услон": 16117,
            "Нурлат": 17714,
            "Менделеевск": 17383,
            "Актаныш": 15256,
            "Мамадыш": 17347,
            "Апастово": 15578,
            "Бугульма": 16020,
            "Рыбная Слобода": 17789,
            "Болгар": 18057,
            "Уруссу": 18455,
            "Зеленодольск": 16846,
            "Кукмор": 17080,
            "Черемшан": 18344,
            "Заинск": 16624,
            "Богатые Сабы": 17870,
            "Актюбинский": 15101,
            "Камское Устье": 16905,
            "Базарные Матаки": 15411,
            "Лаишево": 17150,
            "Азнакаево": 15176,
            "Арск": 15775,
            "Елабуга": 16538,
            "Альметьевск": 15576
        },
        "Удмуртская Республика": {
            "Ижевск": 81823,
            "Сарапул": 81844
        },
        "Самарская область": {
            "Нефтегорск": 156166,
            "Чапаевск": 155436,
            "Кинель": 155447,
            "Сызрань": 155441,
            "Новокуйбышевск": 155426,
            "Тольятти": 155439,
            "Самара": 155402,
            "Отрадный": 155435
        },
        "Оренбургская область": {
            "Оренбург": 136639,
            "Орск": 136677,
            "Медногорск": 136663,
            "Новотроицк": 136669
        },
        'Московская область': {
            'Москва': 3,
            "Коломна": 27713,
            "Ногинск": 31035,
            "Щелково": 34387
        },
        'Челябинская область': {
            'Магнитогорск': 97358,
            'Миасс': 97405,
            'Златоуст': 97322,
            'Челябинск': 97289
        },
        'Тюменская область': {
            'Тюмень': 45296
        },
        'Свердловская область':{
            'Екатеринбург': 721
        }
    }

    async def data_save(self, data, cityID, type):
        with open(f"{type}-{cityID}.json", "w", encoding="utf8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    async def get_pharmacy(self, page, cityID):
        url = "https://web-api.apteka-april.ru/pharmacies?cityID={}".format(cityID)
        print(url)
        await page.goto(url)
        result = json.loads(await page.text_content('body'))
        data = {}
        for res in result:
            data.update({res["ID"]: res})
        return data
        # print(result)

    async def get_all_categories(self):
        ('https://web-api.apteka-april.ru/types@catalog')
        data = json.loads()
        categories = []
        for category in data['types']:
            categories.append(category['ID'])
        return categories

    async def get_stock_products(self, page, cityID):
        STOCK_COUNT = 50000
        i = 0
        j = STOCK_COUNT

        data = []
        while True:
            try:
                url = "https://web-api.apteka-april.ru/pharmacies/stock?cityID={}[{}:{}]".format(cityID, i, j)
                await page.goto(url)
                content = await page.text_content('body')
                result = json.loads(content)
                i += STOCK_COUNT
                j += STOCK_COUNT
                if not result:
                    return data
                else:
                    data += result
            except:
                pass

    async def get_all_products(self, page, cityID):
        url = f"https://web-api.apteka-april.ru/catalog/ID,price,name@products?cityID={cityID}&isAvailable=true"
        print(url)
        await page.goto(url)
        result = json.loads(await page.text_content('body'))
        # data = {}
        # for res in result:
        #     data.update({res["ID"]: res})
        # return data
        return result

    async def dict_parse(self, stocks, products, pharmacies):
        daties = []
        for product in products:
            try:
                for i in stocks[product['ID']]:
                    #print(i)
                    product_name = product['name']
                    price_withPeriod = product['price']['withPeriod']
                    price_withoutCard = product['price']['withoutCard']
                    pharmacy = pharmacies[i["pharmacyID"]]['address']
                    count = i["count"]
                    data = {
                        # 'site': site,
                        # 'region': region,
                        # 'city': city,
                        'name': str(product_name).replace("'", ""),
                        'apteka': pharmacy,
                        'price': '{},{}'.format(str(price_withPeriod),
                                                str(price_withoutCard)),
                        'count': count,
                        'dataGodn': '',
                        # 'task_id': int(task_id)
                    }
                    daties.append(data)
            except:
                try:
                    product_name = product['name']
                    price_withPeriod = product['price']['withPeriod']
                    price_withoutCard = product['price']['withoutCard']
                    pharmacy = "-"
                    count = 0
                    data = {
                        # 'site': site,
                        # 'region': region,
                        # 'city': city,
                        'name': str(product_name).replace("'", ""),
                        'apteka': pharmacy,
                        'price': '{},{}'.format(str(price_withPeriod),
                                                str(price_withoutCard)),
                        'count': count,
                        'dataGodn': '',
                        # 'task_id': int(task_id)
                    }
                    daties.append(data)
                except:
                    pass




        # for stock in stocks:
        #     productID = stock["productID"]
        #     pharmacyID = stock["pharmacyID"]
        #     # if productID:
        #     try:
        #         product_name = products[productID]['name']
        #         price_withPeriod = products[productID]['price']['withPeriod']
        #         price_withoutCard = products[productID]['price']['withoutCard']
        #         pharmacy = pharmacies[pharmacyID]['address']
        #         count = stock["count"]
        #         data = {
        #             # 'site': site,
        #             # 'region': region,
        #             # 'city': city,
        #             'name': str(product_name).replace("'", ""),
        #             'apteka': pharmacy,
        #             'price': '{},{}'.format(str(price_withPeriod),
        #                                     str(price_withoutCard)),
        #             'count': count,
        #             'dataGodn': '',
        #             # 'task_id': int(task_id)
        #         }
        #         daties.append(data)
        #     except Exception as e:
        #         print(e, end=",")
        #
        return daties

    async def scrapy_data(self, cityID):
        async with async_playwright() as p:
            # chrom = await p.chromium.launch(headless=False, timeout=30000)
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            products = await self.get_all_products(page, cityID)
            # await chrom.close()

            firef = await p.firefox.launch()
            f_page = await firef.new_page()
            await self.data_save(products, cityID, "products")
            stock = await self.get_stock_products(f_page, cityID)
            await firef.close()
            # await browser2.close()
            stocks_d = {}
            for st in stock:
                try:
                    stocks_d[st['productID']].append(st)
                except:
                    stocks_d.update({st['productID']:[st]})
            #print(stocks_d)
            await self.data_save(stock, cityID, "stock")
            pharmacy = await self.get_pharmacy(page, cityID)
            await self.data_save(pharmacy, cityID, "pharmacy")

            product_data = await self.dict_parse(stocks_d, products, pharmacy)
            return product_data

    async def csv_write(self,city, data):
        async with aiofiles.open(f"{city}-aprel.csv", mode="w", encoding="utf-8", newline="") as afp:

            writer = AsyncDictWriter(afp,
                                     ["name", "apteka", "price", "count", "dataGodn"],
                                     # ["site", "region", "city", "name", "apteka", "price", "count", "dataGodn", "task_id"],
                                     restval="NULL", quoting=csv.QUOTE_ALL)
            await writer.writerows(data)

    async def get_page_data(self, city, url=None):
        cityID = self.settings[city['region']][city['city']]
        site = "Апрель Аптека"
        region = city["region"]
        city = city["city"]
        task_id = 7777
        daties = await self.scrapy_data(cityID)
        await self.data_save(daties, cityID, "all_data")
        # with open("all_data-8572.json") as f:
        #     daties = json.load(f)

        await self.csv_write(city,daties)