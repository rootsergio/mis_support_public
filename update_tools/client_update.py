import hashlib
import socket
from paramiko import BadHostKeyException, AuthenticationException, SSHException, ssh_exception
from paramiko import RSAKey, SSHClient, AutoAddPolicy
from update_tools.models import Mo, Servers, DatabaseConnection
from config import settings
from pathlib import PurePath, Path
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm.exc import NoResultFound

SSH_KEY = RSAKey.from_private_key_file(settings.SSH_AUTH_KEY)


class UpdateFiles:
    def __init__(self, software: str = None, data_mo = None, ignore=True, clear=False):
        self.soft = software
        self.data_mo = data_mo
        self.hash_local = {}
        self.hash_remote = {}
        self.ignore = ignore
        self.local_path = None
        self.remote_path = None
        self.ssh = None
        self.config_command = None
        self.clear = clear

    @staticmethod
    def md5(filename: Path):
        """
        Подсчёт md5 суммы файла
        :param filename: путь к файлу
        :return: md5 сумма
        """
        result = hashlib.md5()
        with open(filename, "rb") as fn:
            while d := fn.read(8192):
                result.update(d)
        return result.hexdigest()

    @staticmethod
    def dict_differ(current_dict: dict, past_dict: dict) -> set:
        """
        Сравнивает 2 словаря и возвращает кортеж из ключей
        :param current_dict:
        :param past_dict:
        :return:
        """
        set_current, set_past = set(current_dict.keys()), set(past_dict.keys())
        intersect = set_current.intersection(set_past)
        change = set(o for o in intersect if past_dict[o] != current_dict[o])
        add = set_current - intersect
        file_list = change | add
        return file_list

    def get_hash_local_files(self):
        """
            Функция для рекурсивного подсчёта md5 суммы файлов из указанной директории на локальном хосте.
            :param path: Путь к директории, должен содержать закрывающий символ "/". Пример: get_path('/path/directory/')
            :param ignore: Определяет, использовать список файлов исключений или нет
            :param service: Имя сервиса, используется для списка игнорируемых файлов
            :return: словарь, где ключ - путь к файлу, внутри указанной директории, значение - md5 сумма файла
            """
        files = [file for file in list(Path(self.local_path).glob('**/*')) if file.is_file()]
        for file in files:
            if self.ignore:
                if self.file_ignore(filename=file):
                    continue
            file_hash = self.md5(file)
            self.hash_local[file.as_posix()[len(self.local_path):]] = file_hash

    def get_hash_remote_files(self):
        """
        Функция для рекурсивного подсчёта md5 суммы файлов из указанной директории на удалённом хосте
        :param path: Путь к директории, должен содержать закрывающий символ "/". Пример: get_path('/path/directory/')
        :param ssh: объект ssh.connect
        :param ignore: Определяет, использовать список файлов исключений или нет
        :param service: Имя сервиса, используется для списка игнорируемых файлов
        :return: словарь, где ключ - путь к файлу, внутри указанной директории, значение - md5 сумма файла
        """
        self.hash_remote = {}
        stdin, stdout, stderr = self.ssh.exec_command(f'find {self.remote_path} -type f | xargs -d "\\n" md5sum -b')
        files = stdout.readlines()
        for f in files:
            file_hash, file = f.split('*')
            file = file.strip(' \r\n')
            file = file[len(self.remote_path):]
            if self.ignore:
                if self.file_ignore(file):
                    continue
            file_hash = file_hash.strip(' \r\n')
            self.hash_remote[file] = file_hash

    def update_files(self):
        """
        Выполняет обновление файлов в каталоге на внешнем хосте в соответствии с файлами на локальном хосте
        """
        file_list = self.dict_differ(self.hash_local, self.hash_remote)
        if not file_list:
            print("Версия клиента актуальна")
            return True
        print(f"Файлов для копирования: {len(file_list)}")
        for file in file_list:
            source_file = Path(self.local_path, file)
            destination_file = Path(self.remote_path, file)
            try:
                # На случай, если конечный каталог для копирования файла отсутствует, создаём его
                dir = destination_file.parent.as_posix()
                self.ssh.exec_command(f"mkdir -p {dir}")
                # копируем файл
                sftp = self.ssh.open_sftp()
                sftp.put(source_file, destination_file.as_posix())
            except socket.error as err:
                print("Socket Error: {}\n{}\n{}".format(err, source_file, destination_file))
            except TypeError as err:
                print("Type Error: {}".format(err))
            except ssh_exception.SSHException as err:
                print("Paramiko Error: {}".format(err))
            except EOFError as err:
                print("EOFError Error: {}".format(err))
            else:
                print("Скопирован файл: {}".format(file))

    def ssh_connect(self, ipv4):
        """
        Создание экземпляра класса SSHClient (Paramiko)
        :param ipv4: IP адрес для подключения
        """
        try:
            self.ssh = SSHClient()
            # ssh.load_host_keys(KNOWN_HOST)
            self.ssh.set_missing_host_key_policy(AutoAddPolicy())
            self.ssh.connect(hostname=ipv4, port=22, username='root', pkey=SSH_KEY)
            return True
        except (AuthenticationException, TimeoutError, BadHostKeyException, ConnectionResetError,
                ssh_exception.NoValidConnectionsError) as err:
            print("Error: {}\n".format(err))
            self.ssh = None

    def set_paths(self, local_path: Path = None, remote_path: Path = None):
        """
        Устанавливает параметры по умолчанию
        :param local_path:
        :param remote_path:
        """
        if self.soft == 'mis':
            self.local_path = settings.LOCAL_PATH_MIS
            self.remote_path = settings.REMOTE_PATH_MIS
        elif self.soft == 'iemk':
            self.local_path = settings.LOCAL_PATH_IEMK
            self.remote_path = settings.REMOTE_PATH_IEMK
        elif self.soft == 'soap':
            self.local_path = settings.LOCAL_PATH_SOAP
            self.remote_path = settings.REMOTE_PATH_SOAP
        else:
            self.local_path = local_path
            self.remote_path = remote_path

    def set_config_command(self, command=None):
        """
        Устанавливает команды, выполняемые после обновления файлов
        :param command:
        :return:
        """
        if self.soft == 'mis':
            self.config_command = f'chown tsadmin:samsonuser -R {self.remote_path} && chmod 775 -R {self.remote_path} && ' \
                             f'find {self.remote_path} -type f | xargs -d "\\n" chmod 664 && ' \
                             f'chmod +x {self.remote_path}s11main.py && ' \
                             f'chmod +x {self.remote_path}appendix/regional/r23/importReestr/importReestr.py'
        elif self.soft == 'iemk':
            self.config_command = f'chown root:root -R {self.remote_path} && chmod 775 -R {self.remote_path} && ' \
                                  f'find {self.remote_path} -type f | xargs -d "\\n" chmod 664 && ' \
                                  f'chmod +x {self.remote_path}run_iemk.sh && ' \
                                  f'chmod +x {self.remote_path}run_putev.sh && ' \
                                  f'chmod +x {self.remote_path}IEMK/IEMKrequest.php && ' \
                                  f'chmod +x {self.remote_path}putevFactExecs.py &&' \
                                  f'chmod +x {self.remote_path}run_smphosp.sh'
        elif self.soft == 'soap':
            self.config_command = f'systemctl restart httpd; systemctl restart apache2'
        else:
            self.config_command = command

    def __setup(self):
        """
        Установка параметров
        """
        if not self.soft:
            self.soft = 'mis'
        self.set_paths()
        self.set_config_command()

    def ssh_run_command(self, command):
        try:
            stdin, stdout, stderr = self.ssh.exec_command(command)
            data = stdout.read() + stderr.read()
            return data
        except (SSHException, AttributeError) as err:
            print(err)
            return None

    def file_ignore(self, filename):
        """
        Используется для исключения файлов/каталогов при подсчёте md5 суммы
        :param filename: путь к файлу
        :param service: имя сервиса, для которого свой список игнорируемых вайлов, если не указан, то используется
        список игнорируемых файлов для МИС
        :return: True - файл находится в списке исключений
        """
        ignore_files = {
            "mis": {'.gitignore', '.idea', '.pyc', '.git', 'psutil', 'scripts'},
            "iemk": {'conf.ini', 'config.php'},
            "soap": {'config.php', 'config_old'}
        }
        path_split = set(PurePath(filename).parts)
        if set.intersection(ignore_files[self.soft], path_split):
            return True
        else:
            return False

    def set_ignore(self, flag: bool):
        self.ignore = flag

    def clear_remote_path(self, soft):
        if soft == 'soap':
            clear_command = f'cp {self.remote_path}config/config.php {self.remote_path}../ && ' \
                            f'find {self.remote_path} -type f -delete && '\
                            f'cp {self.remote_path}../config.php {self.remote_path}config/ && echo All files delete'
            return self.ssh_run_command(clear_command)

    @staticmethod
    def get_mo_data(state: str=None, server: str=None, ipaddr: list=None, iemk: bool=False) -> list:
        """
        Возвращает список c данными из БД в соответствии с переданными аргументами
        :param state: Принимает наименование района, поиск производится по шаблону 'Район%'
        :param server: Принимает тип сервера: TC, DB, XEN и тд
        :param ipaddr: Принимает ip адрес сервера
        :param iemk: Принимает True, определяющее наличие сервиса IEMK
        :return: Mo.id, Mo.state, Mo.name, Servers.ipv4
        """
        connection = DatabaseConnection()
        session = connection.create()
        main_filter = session.query(Mo.id, Mo.state, Mo.name, Servers.ipv4).join(Mo).filter(Mo.support == True).\
            filter(Servers.poweron == True)
        if state:
            main_filter = main_filter.filter(Mo.state.like(f"{state}%"))
        if server:
            main_filter = main_filter.filter(Servers.server_type == server)
        if ipaddr:
            main_filter = main_filter.filter(Servers.ipv4.in_(ipaddr))
        if iemk:
            main_filter = main_filter.filter(Servers.iemk == True)
        try:
            data = [i for i in main_filter]
        except NoResultFound:
            print("Нет результата")
        except (ProgrammingError, OperationalError) as err:
            print(err)
        else:
            return data
        finally:
            session.close()

    def update(self):
        """
        Выполнение обновления файлов по списку указанных хостов
        """
        self.__setup()
        self.get_hash_local_files()
        if not self.data_mo:
            if self.soft == 'mis':
                self.data_mo = self.get_mo_data(server='TS')
            elif self.soft == 'iemk':
                self.data_mo = self.get_mo_data(server='TS', iemk=True)
            elif self.soft == 'soap':
                self.data_mo = self.get_mo_data(server='DB')
        for mo in self.data_mo:
            if not self.ssh_connect(ipv4=mo.ipv4):
                continue
            # mo = self.get_mo_data(ipaddr=ip)[0]
            print(f"\nВыполняется обновление на сервере {mo.ipv4} в {mo.state} {mo.name}:")     # в {mo.state} {mo.name}
            if self.clear:
                print(self.clear_remote_path(self.soft))
            self.get_hash_remote_files()
            self.update_files()
            print(self.ssh_run_command(self.config_command))
            self.ssh.close()


if __name__ == '__main__':
    mo_data = UpdateFiles.get_mo_data(server='TS', iemk=True, ipaddr=['10.212.2.131', '10.235.11.131'])
    data = UpdateFiles(software='iemk', data_mo=mo_data)
    data.update()
