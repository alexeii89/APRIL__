import asyncio
import json
import os
from os import path
import time
import zipfile
from aiogram import Bot, types
import aioschedule
import asyncpg
from playwright.async_api import async_playwright
from csv import writer
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, BOT_TOKEN


class Aprel():
    all_products = {'cityID': None, 'products': None}
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
            "Старобалтачево": 9339
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
            "Альметьевск": 15576,
            "Кукмор": 17080
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
            'Магнитогорск': 97358
        }
    }
    proxy = ["5229cd0e5a:26e709c2a5@185.183.161.50:59845", "21df9e5556:e505072905@31.184.194.40:15710",
             "9726ab085d:52676805de@192.162.59.181:32052", "92b4005679:55ad086e09@194.120.24.64:34730",
             "8d91e50596:9309de5621@192.162.59.132:20824", "029ea9599e:e9e9d5dd62@138.124.185.88:24684",
             "5e1626710d:a52e56de91@5.8.65.246:57395", "2189e9d054:5b42856690@95.215.1.62:46870",
             "217b8d95e3:b726591d0e@192.144.7.98:43736", "9e90d0e62a:249eb9062d@5.101.4.23:52761",
             "0d66be2e85:e506cd6e8b@5.8.65.48:27526", "6e56231e9d:59d62e34e2@192.162.57.16:29750",
             "e27e57139d:6e95797032@185.183.160.92:32407", "50862fd977:8d3302c99e@138.124.185.109:42134",
             "d522eb0ff5:567f2695ed@185.183.163.10:40938", "8786ed95c0:5d9d897e00@5.8.8.199:42952",
             "b067e9d825:692ecd1b52@5.8.8.171:16871", "69e02e9dd1:e925726239@5.8.55.71:25996",
             "35dd90e210:108d04e325@185.192.109.115:37463", "23785670de:d90e783502@5.101.6.196:24382",
             "9052e8f2d8:6dde158529@5.8.65.206:39624", "b856579e08:08e8f29764@91.194.10.54:48715",
             "97e0ff265d:a22599f80d@31.184.193.136:62511", "6d0569cc8e:005dc9cae6@91.194.10.151:30451",
             "d902944e65:d09e240268@138.124.185.72:16327", "652c0ddae9:92edea5629@185.80.150.36:52271",
             "69258d52e9:9d91550642@5.8.65.51:18976", "53962b9d77:c2ed76f908@37.139.59.36:62379",
             "70d96e5396:9d3a05b96e@185.183.160.244:43107", "c9f297650d:9e2094876c@46.161.48.146:38995",
             "9f290ad652:e52d29e90b@185.184.78.248:41937", "257c9daa56:6ed025f25a@192.162.59.184:31175",
             "a52054a2f9:4a56a60362@138.124.185.202:32573", "6095ed1f06:25569afde5@138.124.185.87:26108",
             "951a06dd9e:a652595ed0@195.158.225.18:22171", "64ef2ab5e0:56e0ea3bd9@185.194.106.19:22808",
             "25d907683e:5d9ec7e396@185.128.213.138:34686", "bd7189062e:7be1bb0692@217.29.53.177:22378",
             "b69efd3207:3bf46d9e20@5.8.48.117:29847", "20bde8596a:e6d6bce095@46.161.51.196:59996",
             "70566deb52:9ba58e07d0@195.158.254.117:58145", "eb6be05d62:f5496d6eb2@5.8.65.240:46237",
             "d958c52b6d:a5ecb60c92@5.101.7.194:26628", "5c2ed98300:d930d02fd5@37.139.57.47:15621",
             "6319e1e2d5:e62c10095e@192.162.58.165:51842", "6c59d7209e:5695920cd9@194.120.24.128:45150",
             "6e957c0e5d:6d8299570e@192.162.59.218:46174", "fe026586ad:de802c596d@45.153.75.2:44132",
             "c43ed6c092:5de6c5592b@192.162.57.137:64088", "2e629edd05:6c820e2fd5@138.124.185.86:54928",
             "2c6ed9d05e:5822049def@138.124.185.24:36812", "2c455da96e:50e6d139ad@5.8.65.217:23243",
             "e242670dd5:d96d7d8659@46.161.48.196:26872", "59120ea6dd:2e501d61a1@5.8.65.134:52601",
             "65d0e82d99:7956e78d2c@192.162.58.156:37515", "0deeb5299d:e5d126e9d0@195.158.255.76:48433",
             "52e519d7e0:21ede57960@91.198.210.5:18370", "9e40dae524:664de92e58@192.162.58.37:15445",
             "f5190fed6c:ee656d6290@192.162.57.128:24380", "55e9e6df92:59ee6095de@5.8.65.34:16304",
             "e206db0e97:7b2e44605f@5.8.54.77:48489"]
    count_requests = 0

    async def get_all_products(self, region, city, task_id):
        cityID = self.settings[region][city]
        categories = [396070, 396198, 396194, 398631, 398611, 400010, 398511, 400222,
                      400221, 400224, 400228, 398830, 400217, 400231, 400226, 400232]
        count_product = 0
        pharms_address = await self.get_cyty_pharms(cityID, False)
        remove_csv = f"products/{city}-aprel.csv"
        remove_zip = f"products/{city}-aprel.zip"
        if os.path.exists(remove_csv):
            os.remove(remove_csv)
        if os.path.exists(remove_zip):
            os.remove(remove_zip)
        for category in categories:
            cat_url = f'https://web-api.apteka-april.ru/catalog/ID,price,name@products?typeIDs={category}&cityID={cityID}'
            prod_categ = await self.get_json_playwriter(cat_url)
            if prod_categ == cat_url:
                print(f"Error request category: {cat_url}")
            else:
                prod_pharms = []
                id_products = ""
                lite_products = {}
                start_word = prod_categ[0]
                stop_word = prod_categ[-1]
                for product in prod_categ:
                    try:
                        lite_products.update({product["ID"]:
                                                  {'name': product['name'],
                                                   'price': product['price']
                                                   }})
                        error = False
                    except:
                        error = True
                    if error == False:
                        if product == start_word:
                            id_products = str(product["ID"])
                        elif len(id_products) < 1900 and product != stop_word:
                            id_products = str(id_products) + "," + str(product["ID"])
                        else:
                            url_pahrms = "https://web-api.apteka-april.ru/pharmacies/stock?productID={" + id_products + "}&cityID=" + str(
                                cityID)
                            r = await self.get_json_playwriter(url_pahrms)
                            if r == url_pahrms:
                                print(f"Error request pharm products: {url_pahrms}")
                                id_products = str(product["ID"])
                            else:
                                # print(r)
                                prod_pharms.extend(r)
                                id_products = str(product["ID"])
                    if error == True and product == stop_word and id_products != "":
                        url_pahrms = "https://web-api.apteka-april.ru/pharmacies/stock?productID={" + id_products + "}&cityID=" + str(
                            cityID)
                        r = await self.get_json_playwriter(url_pahrms)
                        if r == url_pahrms:
                            print(f"Error request pharm products: {url_pahrms}")
                        else:
                            prod_pharms.extend(r)
                            id_products = str(product["ID"])
                for prod_pharm in prod_pharms:
                    try:
                        address = pharms_address[prod_pharm["pharmacyID"]]
                    except:
                        pharms_address = await self.get_cyty_pharms(cityID, True)
                        address = pharms_address[prod_pharm["pharmacyID"]]
                    price = '{},{}'.format(str(lite_products[prod_pharm["productID"]]['price']['withPeriod']),
                                           str(lite_products[prod_pharm["productID"]]['price']['withoutCard']))
                    list_data = [
                        str(lite_products[prod_pharm["productID"]]['name']).replace("'", ""),
                        address,
                        price,
                        str(prod_pharm['count']),
                        ''
                    ]
                    # print(data)
                    parent_dir = os.path.dirname(path.abspath(__file__))
                    save_path = f"{city}-aprel.csv"
                    with open(save_path, mode="a", encoding="utf-8", newline="") as f_object:
                        writer_object = writer(f_object)
                        writer_object.writerow(list_data)
                        f_object.close()
                        count_product += 1
        send_path = self.file_zip(city)

        conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
        task_end = f"""update public.tasks
        set status = 'DONE'
        where id = {int(task_id)}"""
        await conn.execute(task_end)
        await conn.close()
        return count_product, send_path

    async def get_cyty_pharms(self, cityID, err):

        file_path = f"pharms/{cityID}.json"
        #print(file_path)
        city_pharm = {}
        if os.path.isfile(file_path) == True and err == False:
            with open(file_path, 'r', encoding='utf-8') as f:
                pharms = json.load(fp=f)
        else:
            pharms = await self.get_json_playwriter(f"https://web-api.apteka-april.ru/pharmacies?cityID={cityID}")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(pharms, fp=f, ensure_ascii=False)
        for i in pharms:
            city_pharm.update({i["ID"]: i["address"]})
        return city_pharm

    async def get_json_playwriter(self, url):
        async with async_playwright() as p:
            # try:
            browser = await p.firefox.launch()
            page = await browser.new_page()
            await page.goto(url)
            await page.wait_for_selector('pre')
            # page.wait_for_timeout(timeout=5000)
            r = await page.content()
            json_object = json.loads(r.split("<pre>")[1].split("</pre>")[0])
            await browser.close()
            self.count_requests += 1
            # print(self.count_requests)
            if type(json_object) is list:
                return json_object
            else:
                return url

    def file_zip(self, city):
        file_path = f"{city}-aprel"
        newzip = zipfile.ZipFile('{}.zip'.format(file_path), 'a')
        newzip.write('{}.csv'.format(file_path), compress_type=zipfile.ZIP_DEFLATED)
        newzip.close()
        return f"{file_path}.zip"

async def sendfile(file_name, user, bot):
    if os.path.exists(file_name):
        with open(file_name, 'rb') as file:
            await bot.send_document(chat_id=user, document=file)


async def check_tasks():
    status_request = '''select 
                tasks.id as id,
                regions."name" as region,
                cities."name" as city,
                users.user_id as user_id 
        from tasks
                 inner join list_pharms on tasks.list_pharm = list_pharms.id
                 inner join pharms on pharms.id = list_pharms.pharm_id
                 inner join regions on regions.id = list_pharms.region_id
                 inner join cities on cities.id = list_pharms.city_id
                 inner join users on users.id = tasks.user_id
                 full join list_chosens on list_chosens.list_pharm_id = tasks.list_pharm
                 left join chosens on chosens.id = tasks.list_chosen
        where status in ('SCRAPING')
          and pharms.en_name = 'aprel';
    '''

    # '''select
    #             tasks.id as id,
    #             regions."name" as region,
    #             cities."name" as city,
    #             users.user_id as user_id
    #     from tasks
    #              inner join list_pharms on tasks.list_pharm = list_pharms.id
    #              inner join pharms on pharms.id = list_pharms.pharm_id
    #              inner join regions on regions.id = list_pharms.region_id
    #              inner join cities on cities.id = list_pharms.city_id
    #              inner join users on users.id = tasks.user_id
    #              full join list_chosens on list_chosens.list_pharm_id = tasks.list_pharm
    #              left join chosens on chosens.id = tasks.list_chosen
    #     where status in ('WAIT')
    #       and list_chosen = 6 and pharms.en_name not in
    #           (select DISTINCT(pharms.en_name)
    #            from tasks
    #                     inner join list_pharms on tasks.list_pharm = list_pharms.id
    #                     inner join pharms on pharms.id = list_pharms.pharm_id
    #                     inner join regions on regions.id = list_pharms.region_id
    #                     inner join cities on cities.id = list_pharms.city_id
    #                     inner join users on users.id = tasks.user_id
    #            where status in ('SCRAPING'))
    #     ORDER BY tasks.date, list_chosens.order;
    #    '''
    conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD,
                                 database=DB_NAME, host=DB_HOST)
    res = await conn.fetchrow(status_request)
    await conn.close()
    #print(res)
    if res != None:
        print(f"{res['region']}-{res['city']}")
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
        #t = datetime.datetime.now()
        task_update = f"""update public.tasks
        SET status = 'SCRAPING', send = 'WAIT'
        where id = {int(res['id'])}"""
        #print(task_update)
        await conn.execute(task_update)
        await conn.close()
        r1 = Aprel()
        start = time.time()
        result, send_path = await r1.get_all_products(region=res['region'], city=res['city'], task_id=res['id'])
        end = time.time()
        print(f"Продолжителоьность:{end - start}\nЗапросов: {r1.count_requests}")
        print(f"Товаров: {result}")
        bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
        await sendfile(send_path, res['user_id'], bot)
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
        end_update = f"""update public.tasks
                SET status = 'DONE', send = 'SENDED'
                where id = {int(res['id'])}"""
        # print(task_update)
        await conn.execute(end_update)
        await conn.close()
        try:
            await bot.close
        except:
            pass


async def scheduler():
    aioschedule.every(30).seconds.do(check_tasks)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)



asyncio.run(scheduler())
