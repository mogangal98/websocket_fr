import logging
import os

class LoggerSetup:
    def __init__(self, log_path=None):
        self.log_path =(os.path.expanduser('~') + "/funding_rate.log")
        self.logger = logging.getLogger("FundingRate_Logger")
        
        mode = "release"

        logging.basicConfig(
            filename=self.log_path, 
            format='%(asctime)s - %(process)d - %(levelname)s - %(message)s',
            filemode='w', 
            level=logging.DEBUG
        )


    def get_logger(self):
        return self.logger
