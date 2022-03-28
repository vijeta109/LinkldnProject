from linkedInutility import open_config
from linkedInutility import open_logger
from liveData import livedata
import sys
import logging

# import datetime as dt
# import json
# import os
import logging

from linkedInutility import get_db_connection
from company_master import company_master_data
from job_master import job_master_data
from job_seniority_level import job_seniority_data
cfg = open_config(sys.argv)

open_logger(cfg["log_file"])
logging.info(cfg)
livedata(cfg)
company_master_data(cfg)
job_master_data(cfg)
job_seniority_data(cfg)


