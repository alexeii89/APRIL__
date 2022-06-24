import asyncio
import time
import zipfile
import os

import aioschedule

from parser.aprel import Aprel
import asyncpg
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, BOT_TOKEN
from aiogram import Bot, types


old_city = ""


def file_zip(city):
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
    # status_request = '''select
    #                 tasks.id as id,
    #                 regions."name" as region,
    #                 cities."name" as city,
    #                 users.user_id as user_id
    #         from tasks
    #                  inner join list_pharms on tasks.list_pharm = list_pharms.id
    #                  inner join pharms on pharms.id = list_pharms.pharm_id
    #                  inner join regions on regions.id = list_pharms.region_id
    #                  inner join cities on cities.id = list_pharms.city_id
    #                  inner join users on users.id = tasks.user_id
    #                  full join list_chosens on list_chosens.list_pharm_id = tasks.list_pharm
    #                  left join chosens on chosens.id = tasks.list_chosen
    #         where status in ('SCRAPING')
    #           and pharms.en_name = 'aprel';
    #     '''




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
        where status in ('WAIT')
          and pharms.en_name = 'aprel' and pharms.en_name not in
              (select DISTINCT(pharms.en_name)
               from tasks
                        inner join list_pharms on tasks.list_pharm = list_pharms.id
                        inner join pharms on pharms.id = list_pharms.pharm_id
                        inner join regions on regions.id = list_pharms.region_id
                        inner join cities on cities.id = list_pharms.city_id
                        inner join users on users.id = tasks.user_id
               where status in ('SCRAPING'))
        ORDER BY tasks.date, list_chosens.order;
       '''



    conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
    res = await conn.fetchrow(status_request)
    await conn.close()
    # print(res)
    if res != None:
        remove_csv = f"{res['city']}-aprel.csv"
        remove_zip = f"{res['city']}-aprel.zip"
        if os.path.exists(remove_csv):
            os.remove(remove_csv)
        if os.path.exists(remove_zip):
            os.remove(remove_zip)
        print(f"{res['region']}-{res['city']}")
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
        task_update = f"update public.tasks SET status = 'SCRAPING', send = 'WAIT' where id = {int(res['id'])}"
        await conn.execute(task_update)
        await conn.close()
        aprel = Aprel()
        city = {'region': res['region'], 'city': res['city']}
        user_id = res['user_id']
        try:
            await aprel.get_page_data(city)
            send_path = file_zip(res['city'])
            bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
            await sendfile(send_path, user_id, bot)
            conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
            end_update = f"""update public.tasks SET status = 'DONE', send = 'SENDED' where id = {int(res['id'])}"""
            await conn.execute(end_update)
            await conn.close()
            await bot.close()
            os.remove(remove_csv)
        except:
            global old_city
            if old_city == res['city']:
                conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
                end_update = f"""update public.tasks
                        SET status = 'ERROR', send = 'ERROR'
                        where id = {int(res['id'])}"""
                await conn.execute(end_update)
                await conn.close()
                bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
                await bot.send_message(text="Ошибка, данного города НЕТ, или в городе нет товаров(согласно API)", chat_id=res['user_id'])
                await bot.close
            else:
                conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST)
                end_update = f"""update public.tasks
                        SET status = 'WAIT', send = 'WAIT'
                        where id = {int(res['id'])}"""
                await conn.execute(end_update)
                await conn.close()
                old_city = res['city']

# async def scheduler():
#     aioschedule.every(1).seconds.do(check_tasks)
#     while True:
#         await aioschedule.run_pending()
#         await asyncio.sleep(1)


# async def scheduler():

while True:
    try:
        asyncio.run(check_tasks())
    except:
        pass
    time.sleep(10)






# asyncio.run(scheduler())