import logging
from logging.handlers import RotatingFileHandler
import os

class workflow_logger:
    def __init__(self, name:str, log_file:str, level=logging.INFO):
        '''
        Set logging hander
        '''
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            self.add_handlers(log_file, level)

    def add_handlers(self, log_file, level):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        handler = RotatingFileHandler(
            log_file, 
            maxBytes=5_000_000, 
            backupCount=5 , 
            encoding='utf-8'
        )

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger