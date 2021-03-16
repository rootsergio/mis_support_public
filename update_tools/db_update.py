import re
import datetime
from update_tools.models import *
from sqlalchemy.exc import *
import sqlalchemy.orm.exc
from sqlalchemy import text
import sqlalchemy.orm.session as sql_session

HOST = settings.DB_HOST
USER = settings.DB_USER
PASSWORD = settings.DB_PASSWORD
DB = settings.DB_NAME

DB_MO_LOGIN = settings.DB_MO_LOGIN
DB_MO_DBNAME = settings.DB_MO_DBNAME


class DbUpdater:
    def __init__(self, ipaddr: list = None, rel: list = None):
        self.releases = rel
        self.servers = ipaddr
        self.sql_queries = None
        self.session = None
        self.auth_data = None
        self.current_release = None

    @staticmethod
    def sql_parse(queries: str) -> list:
        """
        Функция преобразует строки с запросами в список с учётом различных символов разделения
        :param queries: строка
        :return: список запросов
        """
        # Find special delimiters
        delimiters = re.compile('DELIMITER *(\S*)', re.I)
        result = delimiters.split(queries)

        # Insert default delimiter and separate delimiters and sql
        result.insert(0, ';')
        delimiter = result[0::2]
        section = result[1::2]
        result_queries = []

        # Split queries on delimiters and execute
        for i in range(len(delimiter)):
            queries = section[i].split(delimiter[i])
            result_queries.extend([query for query in queries if query.strip(' \r\n')])
        return result_queries

    @staticmethod
    def get_installed_release(session: sql_session) -> list:
        """
        Функция возвращает список установленных в МО обновлений
        :param session:
        :return: список установленных обновлений
        """
        sql = text("SELECT release_version FROM update_base")
        try:
            result = session.execute(sql).fetchall()
            releases = [str(i[0]) for i in result]
            return releases
        except (sqlalchemy.orm.exc.NoResultFound, ProgrammingError, OperationalError) as err:
            print(f'Ошибка при получении списка установленных обновлений: {err}\n')
            return list()

    @staticmethod
    def execute_sql_queries(session: sql_session, sql_queries: list):
        """
        Функция выполняет запросы из обновления в МО
        :param session:
        :param sql_queries: список sql запросов
        :return:
        """
        for query in sql_queries:
            try:
                session.execute(text(query))
            except (ProgrammingError, IntegrityError) as err:
                session.rollback()
                raise err
        session.commit()

    @staticmethod
    def execute_select_query(session: sql_session, sql_queries: list):
        """Возвращает список с результатами выполнения SELECT запросов к БД"""
        # for query in sql_queries:
        #     try:
        #         res = session.execute(text(query))
        #     except ProgrammingError as err:
        #         print(err)
        return [f"{query}:\n{session.execute(text(query)).fetchall()}" for query in sql_queries]

    def write_result_update_to_db(self, ipv4: str, result: bool, release: str, comment: str = 'Успешно'):
        """
        Функция записывает результаты обновлений в БД
        :param ipv4: IP адрес сервера БД МО
        :param result: Результат обновления
        :param comment: Комментарий результата обновления
        :param release: Версия обновления
        :return:
        """
        date = datetime.datetime.now()
        try:
            host_id = self.session.query(Servers.id).filter(Servers.ipv4 == ipv4).one()
            release_id = self.session.query(Updatequeries.id).filter(Updatequeries.releaseVersion == release).one()
            try:
                # entry_already_exist получает результат попытки обновления, выполненной ранее, True|False
                entry_already_exist = self.session.query(Logupdatedbmis.result). \
                    filter(Logupdatedbmis.host_id == host_id.id, Logupdatedbmis.release_id == release_id.id). \
                    order_by(Logupdatedbmis.updateDate).one()
            except sqlalchemy.orm.exc.NoResultFound:
                entry_already_exist = False
            if not entry_already_exist:
                ins = Logupdatedbmis(updateDate=date, result=result, comment=comment,
                                     host_id=host_id.id, release_id=release_id.id)
                self.session.add(ins)
                self.session.commit()
        except (OperationalError, ProgrammingError) as err:
            print(f"Ошибка при записи лога обновления: {err}")

    def get_auth_data(self, ipv4: str):
        """
        Функция используется для получения данных для авторизации на сервере БД МО
        :param ipv4: IP адрес сервера БД МО
        :return:
        """
        try:
            self.auth_data = self.session.query(Authdata.user, Authdata.password).join(Servers).filter(Servers.ipv4 == ipv4).one()
        except(OperationalError, ProgrammingError,
               sqlalchemy.orm.exc.NoResultFound, sqlalchemy.orm.exc.MultipleResultsFound) as err:
            print(f"Ошибка при получении данных для авторизации на сервере {ipv4}: {err}")
            self.auth_data = None
        return self.auth_data

    def get_servers_ip(self) -> list:
        """
        Функция возвращает список IP адресов серверов БД в МО. Проверяется значение поля support для МО,
        poweron и server_type (DB) для сервера.
        :return: Список IP адресов серверов БД или None
        """
        ip_addresses = list()
        try:
            res = self.session.query(Servers.ipv4).join(Mo).filter(Mo.support == True). \
                filter(Servers.poweron == True, Servers.server_type == 'DB').order_by(Mo.state).all()
            ip_addresses = [ip[0] for ip in res]
        except (sqlalchemy.orm.exc.NoResultFound, ProgrammingError) as err:
            print(f"Не удалось получить список адресов серверов МО{err}")
        finally:
            return ip_addresses

    def get_all_release(self):
        """
        Функция используется для получения из базы обновлений списка всех релизов
        :return: Список версий релизов или None
        """
        try:
            res = self.session.query(Updatequeries.releaseVersion).filter(Updatequeries.run == True). \
                order_by(Updatequeries.releaseVersion)
            self.releases = [i[0] for i in res]
        except (sqlalchemy.orm.exc.NoResultFound, ProgrammingError) as err:
            print(f"Ошибка получения списка релизов из БД: {err}")
            self.releases = list()

    def get_queries(self, release: str):
        """
        Функция получения SQL запросов указанного в параметрах релиза.
        :param release: Версия релиза
        :return:
        """
        try:
            res = self.session.query(Updatequeries.releaseVersion, Updatequeries.sqlQuery).\
                filter(Updatequeries.releaseVersion == release).one()
            self.sql_queries = self.sql_parse(res.sqlQuery)
            self.current_release = res.releaseVersion
        except (OperationalError, ProgrammingError, sqlalchemy.orm.exc.NoResultFound) as err:
            print(f"Не удалось получить список запросов для обновления: {err}")
            self.sql_queries = None

    def __set_param(self):
        if not self.releases:
            self.get_all_release()
        if not self.servers:
            self.servers = self.get_servers_ip()

    def insert_update_base_and_contents(self):
        """
        Пишем информацию о релизе в таблицы: update_base и update_base_contents
        на тот случай, если скрипты из обновления прогнали руками.
        :param ipv4: адрес сервера БД
        :param release: версия обновления
        :return:
        """
        with DatabaseConnection() as self.session:
            self.__set_param()
            for server in self.servers:
                for release in self.releases:
                    try:
                        res = self.session.query(Updatequeries).filter(Updatequeries.releaseVersion == release).one()
                    except sqlalchemy.orm.exc.NoResultFound as err:
                        print(f"В БД информации о версии обновления {release} не найдено: {err}")
                        continue
                    except (OperationalError, ProgrammingError) as err:
                        print(f"Ошибка при получении данных из БД: {err}")
                        continue
                    self.get_auth_data(ipv4=server)
                    with DatabaseConnection(user=self.auth_data.user, password=self.auth_data.password,
                                            host=server, db_name='s11') as mo_db_session:
                        installed_releases = self.get_installed_release(mo_db_session)
                        if release not in installed_releases:
                            sql1 = text(f"INSERT INTO update_base (release_date, release_version) VALUES "
                                        f"('{res.releaseDate}', {res.releaseVersion});")
                            sql2 = text(f"INSERT INTO update_base_contents (base_release_version, comment, "
                                        f"source, visible) VALUES ({res.releaseVersion}, '{res.comments}', "
                                        f"'{res.manual}', {res.visible})")
                            try:
                                mo_db_session.execute(sql1)
                                mo_db_session.execute(sql2)
                                mo_db_session.commit()
                                print(f"Запись выполнена. Сервер: {server}, релиз: {release}")
                                self.write_result_update_to_db(ipv4=server, result=True,
                                                               comment="Выполнено вне системы", release=release)
                            except (OperationalError, IntegrityError) as err:
                                print(f"Не удалось выполнить запрос в МО:\n{err}")
                                return
                        else:
                            print(f"Обновление {release} на сервере {server} уже установлено")

    def update(self):
        """
        Выполняет обновление БД в МО
        :return:
        """
        with DatabaseConnection() as self.session:
            self.__set_param()
            for server in self.servers:
                print(f"Сервер: {server}")
                if not self.get_auth_data(ipv4=server):
                    continue
                with DatabaseConnection(host=server, user=self.auth_data.user,
                                        password=self.auth_data.password, db_name='s11') as db_mo_session:
                    install_released = self.get_installed_release(db_mo_session)
                    if not install_released:
                        continue
                    release_for_update = sorted(list(set(self.releases) - set(install_released)))
                    if not release_for_update:
                        print("Версия базы данных актуальна.\n")
                        continue
                    for release in release_for_update:
                        if release != self.current_release:
                            self.get_queries(release=release)
                        try:
                            self.execute_sql_queries(db_mo_session, sql_queries=self.sql_queries)
                        except (ProgrammingError, IntegrityError) as err:
                            self.write_result_update_to_db(ipv4=server, result=False,
                                                           release=release, comment=err.args[0])
                            print(f"Обновление {release} не выполнено.\nОшибка:\n{err.args[0]}\n")
                            break
                        else:
                            self.write_result_update_to_db(ipv4=server, result=True, release=release)
                            print(f"Обновление {release} выполнено.\n")

    def select(self):
        SQL = ["SHOW VARIABLES WHERE Variable_name = 'hostname';", "SELECT organization FROM mo_odli;"]
        with DatabaseConnection() as self.session:
            self.__set_param()
            for server in self.servers:
                print(f"Сервер: {server}")
                if not self.get_auth_data(ipv4=server):
                    continue
                with DatabaseConnection(host=server, user=self.auth_data.user,
                                        password=self.auth_data.password, db_name='s11') as db_mo_session:
                    results = self.execute_select_query(db_mo_session, sql_queries=SQL)
                    for result in results:
                        print(f"{result}\n")


if __name__ == '__main__':
    ipaddr = ['10.239.1.130']
    rel = ['2021030501']
    dbupdater = DbUpdater(ipaddr=ipaddr, rel=rel)
    dbupdater.update()
