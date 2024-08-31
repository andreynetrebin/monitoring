import datetime
import calendar
import time
import shutil
import logging
from os import path, makedirs, getcwd, rmdir, remove, system, rename
from os.path import splitext, basename, isfile
import requests
import json
import glob
import aiohttp
import asyncio
import configparser
from database import Database
from queries import (
    spu_tasks,
    spu_fails,
)

config = configparser.ConfigParser()
config.read(("config.ini"), encoding="utf-8")


async def get_response(query, query_name):
    logging.info(f"start - {query_name}")
    async with aiohttp.ClientSession() as session:
        response = await session.post(
            config.get("api_db_query", "endpoint"),
            json={
                "db": "spu",
                "user": config.get("api_db_query", "user"),
                "password": config.get("api_db_query", "password"),
                "query": query
            },
        )
        logging.info(f"complete - {query_name}")
        json_data = await response.json()
        return {query_name: json_data}


def get_url(json_data):
    url = json_data['query']['url_file_result']
    return url


def get_data(url):
    request_data = requests.get(url)
    json_data = request_data.json()
    return json_data


async def main():
    logging.basicConfig(
        format="%(levelname)-8s [%(asctime)s] %(message)s",
        level=logging.INFO,
        filename="logs.log",
    )
    dir_path = getcwd()
    logging.info("Запуск")

    # Получаем данные из SPU
    async with aiohttp.ClientSession() as session:
        responses = await asyncio.gather(
            get_response(spu_tasks, "spu_tasks"),
            get_response(spu_fails, "spu_fails"),
        )

    response_tasks = responses[0]["spu_tasks"]
    response_fails = responses[1]["spu_fails"]

    url_tasks = get_url(response_tasks)
    url_fails = get_url(response_fails)

    spu_data_tasks = get_data(url_tasks)
    spu_data_fails = get_data(url_fails)

    with Database("metrics.db") as metrics_db:

        if spu_data_fails:
            for item in spu_data_fails:
                metrics_db.execute(
                    "INSERT INTO spu_fails (tsk_type, cnt, dt) VALUES (?, ?, ?)",
                    (item['TSK_TYPE'], item['CNT'], item['DT']),
                )
            logging.info("Запись метрик в spu_fails")
        else:
            logging.info("По spu_fails метрики не получены")

        if spu_data_tasks:
            for item in spu_data_tasks:
                metrics_db.execute(
                    "INSERT INTO spu_tasks (tsk_type, cnt, dt) VALUES (?, ?, ?)",
                    (item['TSK_TYPE'], item['CNT'], item['DT']),
                )
            logging.info("Запись метрик в spu_tasks")
        else:
            logging.info("По spu_tasks метрики не получены")

    logging.info("Завершено!")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        # останавливаем цикл событий
        loop.close()
