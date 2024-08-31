from string import Template
import calendar
import datetime
import time
import shutil
import logging
from os import path, makedirs, getcwd, rmdir, remove, system, rename
from os.path import splitext, basename, isfile
import json
import glob
from queries import camunda_new_incidents, camunda_waiting_asv, camunda_waiting_spu, spu_new_fails, spu_new_tasks
from database import Database


def write_txt(filename, text, mode, encoding=None):
    with open(filename, mode, encoding=encoding) as txt_file:
        txt_file.write(text)


def get_html_table(title, metrics_from_db):
    html_table = ""
    header_table = f"""
            <h3>{title}</h3>
            <table id="notification">
                <tr>
                    <th>№</th>
                    <th>Тип процесса</th>
                    <th>Кол-во</th>
                    <th>Прирост</th>
                </tr>
            """

    html_table += header_table
    number = 0
    for i in metrics_from_db:
        number += 1
        cnt_new = f'{i["last_cnt"]:,}'.replace(',', ' ')
        dyn_new = f'{i["dyn_cnt"]:,}'.replace(',', ' ')

        if int(i["dyn_cnt"]) > 999 and int(i["dyn_cnt"]) < 10000:
            html_table += f"""
                <tr>
                    <td>{number}</td>
                    <td>{i["key_proc_def"]}</td>
                    <td>{cnt_new}</td>
                    <td><span class=\"orange-text\">+{dyn_new}</span></td>
                """

        elif int(i["dyn_cnt"]) > 9999:
            html_table += f"""
                <tr>
                    <td>{number}</td>
                    <td>{i["key_proc_def"]}</td>
                    <td>{cnt_new}</td>
                    <td><span class=\"red-text\">+{dyn_new}</span></td>
                """
        else:
            html_table += f"""
                <tr>
                    <td>{number}</td>
                    <td>{i["key_proc_def"]}</td>
                    <td>{cnt_new}</td>
                    <td>+{dyn_new}</td>
                """
    html_table += f"</table>"

    return html_table


def main():
    log_filename = f"{splitext(basename(__file__))[0]}.log"
    logging.basicConfig(
        format="%(levelname)-8s [%(asctime)s] %(message)s",
        level=logging.INFO,
        filename=log_filename,
    )
    logging.info("Запуск")
    current_timestamp = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    ts = current_timestamp.replace(":", "_")
    cwd = getcwd()
    dir_unsend = path.join(cwd, 'unsend')
    unsend_files = glob.glob(path.join(dir_unsend, "*.html"))
    if unsend_files:
        for filename in unsend_files:
            # Передаем в ЛВС не отправленные
            try:
                shutil.copy(filename, "Z:\\ECP\\stat")
                shutil.move(filename, path.join(dir_path, "notifications", f"{splitext(basename(filename))[0]}.html"))
            except:
                logging.info("Снова не удалось передать. Отсутсвует подключение между ИЛВС и ЛВС")

    with Database('metrics.db') as metrics_db:
        # Уведомление о возникновении новых инцидентов
        content_notification = ""
        new_incidents = metrics_db.execute(camunda_new_incidents).fetchall()
        if new_incidents:
            new_incidents_table = get_html_table("Появились новые инциденты", new_incidents)
            content_notification += new_incidents_table
        else:
            logging.info("Новые инциденты не появлялись")

            # Уведомление о приросте >1000 "в ожидании ответов от АСВ"
        increase_asv_waiting = metrics_db.execute(camunda_waiting_asv).fetchall()
        if increase_asv_waiting:
            increase_asv_waiting_table = get_html_table("Прирост \"в ожидании ответов от АСВ\" > 1000",
                                                        increase_asv_waiting)
            content_notification += increase_asv_waiting_table
        else:
            logging.info("Отсутствует прирост >1000 \"в ожидании ответов от АСВ\"")

            # Уведомление о приросте >1000 "в ожидании ответов от СПУ"
        increase_spu_waiting = metrics_db.execute(camunda_waiting_spu).fetchall()
        if increase_spu_waiting:
            increase_spu_waiting_table = get_html_table("Прирост \"в ожидании ответов от СПУ\" > 1000",
                                                        increase_spu_waiting)
            content_notification += increase_spu_waiting_table
        else:
            logging.info("Отсутствует прирост >1000 \"в ожидании ответов от СПУ\"")

            # Уведомление о возникновении новых фейлов
        new_spu_fails = metrics_db.execute(spu_new_fails).fetchall()
        if new_spu_fails:
            new_spu_fails_table = get_html_table("Появились новые фейлы", new_spu_fails)
            content_notification += new_spu_fails_table
        else:
            logging.info("Новые фейлы не появлялись")

            # Уведомление о приросте тасков >1000
        increase_spu_tasks = metrics_db.execute(spu_new_tasks).fetchall()
        if increase_spu_tasks:
            increase_spu_tasks_table = get_html_table("Прирост тасков > 1000", increase_spu_tasks)
            content_notification += increase_spu_tasks_table
        else:
            logging.info("Отсутствует прирост тасков >1000")

    if content_notification:
        notification_template_file = path.join(cwd, "templates", "notification.txt")
        data_to_template = {"content": content_notification}
        notification_template = Template(open(notification_template_file).read())
        notification = notification_template.safe_substitute(data_to_template)

        write_txt(f'notification{ts}.html', notification, "w", encoding="utf-8")

    else:
        logging.info("Уведомление не будет сформировано")

    try:
        shutil.copy(f"notification{ts}.html", "path")
        shutil.move(f"notification{ts}.html", path.join(cwd, "notifications", f"notification{ts}.html"))
    except:
        logging.info("Отсутсвует подключение между ИЛВС и ЛВС")
        shutil.move(f"notification{ts}.html", path.join(cwd, "unsend", f"notification{ts}.html"))

    logging.info("Завершено")


if __name__ == '__main__':
    main()
