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
