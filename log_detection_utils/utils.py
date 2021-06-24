from datetime import datetime


def get_datetime_to_str(date_time=None, time_format='%Y-%m-%d %H:%M:%S'):
    """date_time 을 원하는 time format 으로 변경해주는 함수

    Args:
        date_time: datetime object. date_time 값이 None 인 경우, 현재 시간 입력(date_time = datetime.now())
        time_format: 변경하고자 하는 time format
    Return:
        time_format 을 가진 문자형 날짜
    """
    if date_time is None:
        date_time = datetime.now()
    elif not type(date_time) == datetime:
        raise ValueError("date_time 값은 datetime 이어야 합니다.")
    try:
        date_str = date_time.strftime(time_format)
    except Exception as e:
        raise e
    return date_str


def get_str_to_datetime(date_string=None, time_format='%Y-%m-%d'):
    """string type의 문자형을 datetime object로 변환해주는 함수

    Args:
        date_string: 날짜형 포맷의 string
        time_format: date_string이 가지고 있는 날짜형 포맷
    """
    if date_string is None:
        return datetime.now()
    try:
        date_time = datetime.strptime(date_string, time_format)
    except Exception as e:
        raise e
    return date_time


def set_logger(log_file_path='./log', log_file_name='test.log', level='ERROR'):
    import logging.handlers
    import os

    log_level = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if level not in log_level:
        raise ValueError(f"다음 level 중 하나를 입력해야 합니다. --> NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL"
                         f"level --> {level}")

    if not os.path.isdir(log_file_path):
        os.mkdir(log_file_path)

    log = logging.getLogger(log_file_name.split(".")[0])
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s;%(lineno)s] >> %(message)s')
    # Byte -> KB -> MB
    file_max_byte = 1024 * 1024 * 10 #- 10MB
    file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join(log_file_path, log_file_name),
                                                        maxBytes=file_max_byte,
                                                        backupCount=5)

    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    log.setLevel(level)
    return log


