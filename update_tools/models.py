from sqlalchemy import Column, Integer, String, create_engine, Boolean, ForeignKey, Date, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from config import settings

Base = declarative_base()


class Mo(Base):
    __tablename__ = 'misinfo_mo'

    id = Column(Integer, primary_key=True)
    infis = Column(String(5))
    state = Column(String(100))
    name = Column(String(255))
    shortname = Column(String(255))
    support = Column(Boolean)
    comment = Column(String(255))
    servers = relationship("Servers", back_populates="mo")

    def __repr__(self):
        return f"<misinfo_mo(infis={self.infis}, state={self.state}, name={self.name})>"


class Servers(Base):
    __tablename__ = "misinfo_servers"
    id = Column(Integer, primary_key=True)
    mo_id = Column(Integer, ForeignKey('misinfo_mo.id'))
    mo = relationship("Mo", back_populates="servers")
    poweron = Column(Boolean)
    ipv4 = Column(String(15))
    hostname = Column(String(100))
    server_type = Column(String(10))
    os_name = Column(String(50))
    os_version = Column(String(20))
    comment = Column(String(255))
    iemk = Column(Boolean)
    hosp = Column(Boolean)

    def __repr__(self):
        return f"<misinfo_servers(mo={self.mo}, ipv4={self.ipv4}, hostname={self.hostname})>"


class Updatequeries(Base):

    __tablename__ = 'misupdate_updatequeries'

    id = Column(Integer, primary_key=True)
    releaseDate = Column(Date)
    releaseVersion = Column(String(11))
    sqlQuery = Column(Text)
    run = Column(Boolean)
    comments = Column(String(500))
    visible = Column(Boolean)
    manual = Column(String(250))

    def __repr__(self):
        return f"<misupdate_updatequeries({self.releaseDate =}, {self.releaseVersion =}, {self.comments =})>"


class Logupdatedbmis(Base):

    __tablename__ = 'misupdate_logupdatedbmis'

    id = Column(Integer, primary_key=True)
    updateDate = Column(DateTime)
    result = Column(Boolean)
    comment = Column(String(500))
    host_id = Column(Integer, ForeignKey('misinfo_servers.id'))
    release_id = Column(Integer, ForeignKey('misupdate_updatequeries.id'))


class Authdata(Base):
    __tablename__ = 'misinfo_authdata'

    id = Column(Integer, primary_key=True)
    user = Column(String(50))
    password = Column(String(50))
    server_id = Column(Integer, ForeignKey('misinfo_servers.id'))

    def __repr__(self):
        return f"<misinfo_authdata({self.user =}, {self.password =})>"


class DatabaseConnection:
    def __init__(self, host=settings.DB_HOST, port=settings.DB_PORT, user=settings.DB_USER, password=settings.DB_PASSWORD, db_name=settings.DB_NAME):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.port = port

    def __enter__(self):
        """
        Создаём подключение к БД
        :return:
        """
        engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}')
        Session = sessionmaker(bind=engine)
        self.session = Session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрываем соединение с БД
        """
        self.session.close()
        if exc_val:
            raise

    def create(self):
        return self.__enter__()

    def close(self):
        self.__exit__()
