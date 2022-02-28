#
#
import logging

def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    logging.info("LINE-DICT 0 elem {}".format(list(line_dict.keys())[0])
    cond_match = (
       20 < int(line_dict["if1"]) < 40 
    ) 
    return True if cond_match else False

