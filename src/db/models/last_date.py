from sqlalchemy import Column, Integer, String

from src.db.db_session import SqlAlchemyBase


class LastDate(SqlAlchemyBase):
    __tablename__ = 'last_dates'

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    last_date = Column(String)