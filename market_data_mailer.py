"""
Author: Paras Parkash
Source: Market Data Acquisition System
"""
import threading
import sys
import smtplib
import traceback
from utils.logger import get_mailer_logger
from utils.error_handler import ErrorHandler
from utils.config_manager import config_manager

def send_market_data_email(subject, body):
    """
    Send email notifications in a separate thread
    """
    logger = get_mailer_logger()
    error_handler = ErrorHandler()
    
    try:
        # Load configuration
        system_config = config_manager.load_config()
        
        recipient_email_address = system_config['notification_recipients']
        #Generate a list if multiple recipients mentioned
        recipient_email_address = recipient_email_address.split(',')
        sender_email_address = system_config['email_sender']
        sender_email_password = system_config['email_password']
        
        thread_mail = threading.Thread(target=send_email_actual, args=(recipient_email_address, subject, body, sender_email_address, sender_email_password))
        thread_mail.start()
    except Exception as e:
        message = f'send mail failed. Exception :{e}. Traceback : {traceback.format_exc()}'
        error_handler.handle_error('market_data_mailer - ' + message)
        line_no_mail_exception = sys.exc_info()[-1].tb_lineno
        error_handler.handle_error('market_data_mailer - ' + str(line_no_mail_exception))
        logger.log_error(message)
        logger.log_error(str(line_no_mail_exception))

def send_email_actual(recipient, subject, body, sender_email_address, sender_email_password):
    """
    Actually send the email via SMTP
    """
    logger = get_mailer_logger()
    error_handler = ErrorHandler()
    
    password = sender_email_password
    sender = sender_email_address
    user = sender_email_address
    recipient_list = recipient
    from datetime import date
    from datetime import date
    subject_line = subject + " " + str(date.today())
    text_body = body

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n%s
    """ % (sender, ", ".join(recipient_list), subject_line, text_body)
    
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(user, password)
        server.sendmail(sender, recipient_list, message)
        server.close()
        message = f'Mail with subject {subject_line} sent Succesfully to {recipient}'
        logger.log_info(message)
    except Exception as e:
        message = f'Exception :{e}. Traceback : {traceback.format_exc()}'
        line_no_mail_exception = sys.exc_info()[-1].tb_lineno
        logger.log_error(str(line_no_mail_exception))
        logger.log_error(message)
        error_handler.handle_error('market_data_mailer - ' + message)