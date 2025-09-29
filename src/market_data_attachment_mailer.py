"""
Author: Paras Parkash
Zerodha Data Collector
"""

import threading
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import traceback
from utils.logger import get_mailer_logger
from utils.error_handler import ErrorHandler
from utils.config_manager import config_manager


def send_market_data_attachment_email(subject, body, attachment_file_path):
    """
    Send email with attachment in a separate thread
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
        
        thread_mail = threading.Thread(target=send_email_attachment_actual, 
                                     args=(recipient_email_address, subject, body, 
                                           attachment_file_path, sender_email_address, 
                                           sender_email_password))
        thread_mail.start()
    except Exception as e:
        message = f'send mail with attachment failed. Exception :{e}. Traceback : {traceback.format_exc()}'
        error_handler.handle_error('market_data_attachment_mailer - ' + message)
        line_no_mail_exception = sys.exc_info()[-1].tb_lineno
        error_handler.handle_error('market_data_attachment_mailer - ' + str(line_no_mail_exception))
        logger.log_error(message)
        logger.log_error(str(line_no_mail_exception))


def send_email_attachment_actual(recipient, subject, body, attachment_file_path, sender_email_address, sender_email_password):
    """
    Actually send the email with attachment via SMTP
    """
    logger = get_mailer_logger()
    error_handler = ErrorHandler()
    
    password = sender_email_password
    sender = sender_email_address
    user = sender_email_address
    recipient_list = recipient
    from datetime import date
    subject_line = subject + " " + str(date.today())
    text_body = body

    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipient_list)
    msg['Subject'] = subject_line

    # Add body to email
    msg.attach(MIMEText(text_body, 'plain'))

    # Open file in binary mode
    with open(attachment_file_path, "rb") as attachment:
        # Instance of MIMEBase and named as part
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        'Content-Disposition',
        f"attachment; filename= {attachment_file_path.split('/')[-1]}",
    )

    # Attach the part to message
    msg.attach(part)

    # Prepare actual message
    text = msg.as_string()
    
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(user, password)
        server.sendmail(sender, recipient_list, text)
        server.close()
        message = f'Mail with subject {subject_line} and attachment sent Succesfully to {recipient}'
        logger.log_info(message)
    except Exception as e:
        message = f'Exception :{e}. Traceback : {traceback.format_exc()}'
        line_no_mail_exception = sys.exc_info()[-1].tb_lineno
        logger.log_error(str(line_no_mail_exception))
        logger.log_error(message)
        error_handler.handle_error('market_data_attachment_mailer - ' + message)