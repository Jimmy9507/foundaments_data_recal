from config import RQConfig, init_with

from .update import update_quarter
from .recal import update_day


def init(config_path=None):
    init_with(RQConfig(config_path))
