# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System

All functions log their status and failures in their respective log files
But system_error_logger is called on all failures, logging any failure from any function in system_errors_yyyy-mm-dd.log
It also prints the error in Red for immediate visibility
"""

from utils.logger import get_system_error_logger

def log_system_error(message_text):
    """
    Log system errors using the centralized logger
    """
    logger = get_system_error_logger()
    logger.log_error(message_text)