from sqlalchemy import Column, Integer, String

from src.db.db_session import SqlAlchemyBase


class Config(SqlAlchemyBase):
    __tablename__ = 'config'

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    text = Column(String)