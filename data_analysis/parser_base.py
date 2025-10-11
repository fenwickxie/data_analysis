# 解析器基类
from abc import ABC, abstractmethod

class ParserBase(ABC):
    @abstractmethod
    def parse(self, raw_data):
        """
        解析原始数据，返回结构化数据
        :param raw_data: dict
        :return: dict
        """
        pass
