import aiopg
import asyncio
import time
import datetime
import calendar
from os import path, makedirs, getcwd, rmdir, remove, system, rename
from os.path import splitext, basename, isfile
import logging
import configparser
from database import Database
from queries import (
    camunda_efs_instances,
    camunda_efs_incidents,
    camunda_tdism_instances,
    camunda_tdism_incidents,
    camunda_person_incidents,
    camunda_person_instances,
)

config = configparser.ConfigParser()
config.read(("config.ini"), encoding="utf-8")

config_data = config._sections

dsn_camunda_efs = 'dbname={database} user={user} password={password} host={host} port={port}'.format(
    **config_data["camunda_efs"])
dsn_camunda_td_is_m = 'dbname={database} user={user} password={password} host={host} port={port}'.format(
    **config_data["camunda_td_is_m"])
dsn_camunda_person = 'dbname={database} user={user} password={password} host={host} port={port}'.format(
    **config_data["camunda_person_app"])


async def select(dsn: str, query: str, query_name: str, timeout=None):
    logging.info(f"start - {query_name}")
    async with aiopg.connect(dsn) as con:
        async with con.cursor(timeout=timeout) as cursor:
            await cursor.execute(query)
            db_data = []
            column_names = [desc[0].lower() for desc in cursor.description]
            async for row in cursor:
                db_data.append(row)
            logging.info(f"complete - {query_name}")
            result_data = [dict(zip(column_names, row)) for row in db_data]
            return {query_name: result_data}


async def main():
    logging.basicConfig(
        format="%(levelname)-8s [%(asctime)s] %(message)s",
        level=logging.INFO,
        filename="logs.log",
    )
    dir_path = getcwd()
    logging.info("Запуск")
    current_timestamp = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")

    ts = calendar.timegm(time.gmtime())
    data = await asyncio.gather(
        select(dsn_camunda_efs, camunda_efs_instances, "camunda_efs_instances"),
        select(dsn_camunda_efs, camunda_efs_incidents, "camunda_efs_incidents"),
        select(dsn_camunda_person, camunda_person_instances, "camunda_person_instances", timeout=300),
        select(dsn_camunda_person, camunda_person_incidents, "camunda_person_incidents"),
        select(dsn_camunda_td_is_m, camunda_tdism_instances, "camunda_tdism_instances"),
        select(dsn_camunda_td_is_m, camunda_tdism_incidents, "camunda_tdism_incidents"),
    )

    data_camunda_efs_instances = data[0]['camunda_efs_instances']
    data_camunda_efs_incidents = data[1]['camunda_efs_incidents']
    data_camunda_person_instances = data[2]['camunda_person_instances']
    data_camunda_person_incidents = data[3]['camunda_person_incidents']
    data_camunda_tdism_instances = data[4]['camunda_tdism_instances']
    data_camunda_tdism_incidents = data[5]['camunda_tdism_incidents']

    with Database("metrics.db") as metrics_db:
        if data_camunda_efs_instances:
            for item in data_camunda_efs_instances:
                metrics_db.execute(
                    "INSERT INTO camunda_instances (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (
                        item["key_proc_def"],
                        item["activity_id"],
                        item["cnt"],
                        item["dt"]
                    ),
                )
            logging.info("Запись метрик по camunda_efs_instances")
        else:
            logging.info("По camunda_efs_instances метрики не получены")

        if data_camunda_efs_incidents:
            for item in data_camunda_efs_incidents:
                metrics_db.execute(
                    "INSERT INTO camunda_incidents (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (
                        item["key_proc_def"],
                        item["activity_id"],
                        item["cnt"],
                        item["dt"]
                    ),
                )
            logging.info("Запись метрик по camunda_efs_incidents")
        else:
            logging.info("По camunda_efs_incidents метрики не получены")

        if data_camunda_person_instances:
            for item in data_camunda_person_instances:
                metrics_db.execute(
                    "INSERT INTO camunda_instances (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (item["key_proc_def"], item["activity_id"], item["cnt"], item["dt"]),
                )
            logging.info("Запись метрик по camunda_person_instances")
        else:
            logging.info("По camunda_person_instances метрики не получены")

        if data_camunda_person_incidents:
            for item in data_camunda_person_incidents:
                metrics_db.execute(
                    "INSERT INTO camunda_incidents (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (item["key_proc_def"], item["activity_id"], item["cnt"], item["dt"]),
                )
            logging.info("Запись метрик по camunda_person_incidents")
        else:
            logging.info("По camunda_person_incidents метрики не получены")

        if data_camunda_tdism_instances:
            for item in data_camunda_tdism_instances:
                metrics_db.execute(
                    "INSERT INTO camunda_instances (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (item["key_proc_def"], item["activity_id"], item["cnt"], item["dt"]),
                )
            logging.info("Запись метрик по camunda_tdism_instances")
        else:
            logging.info("По camunda_tdism_instances метрики не получены")

        if data_camunda_tdism_incidents:
            for item in data_camunda_tdism_incidents:
                metrics_db.execute(
                    "INSERT INTO camunda_incidents (key_proc_def, activity_id, cnt, dt) VALUES (?, ?, ?, ?)",
                    (item["key_proc_def"], item["activity_id"], item["cnt"], item["dt"]),
                )
            logging.info("Запись метрик по camunda_tdism_incidents")
        else:
            logging.info("По camunda_tdism_incidents метрики не получены")

    logging.info("Завершено!")


if __name__ == '__main__':
    # создаем цикл событий
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        # останавливаем цикл событий
        loop.close()
