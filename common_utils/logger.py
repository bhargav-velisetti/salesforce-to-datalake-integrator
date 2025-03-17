import logging
import sys

def stdout_logger():

    # Configure logging to output to stdout
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

    # Get a logger instance
    logger = logging.getLogger(__name__)

    return logger 

def get_logger(logger_name):
    if logger_name == 'stdout_logger':
        return stdout_logger()
    elif logger_name == 'file_logger':
        pass 
        #return file_logger()
    elif logger_name == 'gcp_logger':
        pass
        #return gcp_logger()
    elif logger_name == 'aws_logger':
        pass
        #return aws_logger()