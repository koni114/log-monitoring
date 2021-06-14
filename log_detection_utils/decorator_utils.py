import time


def timing_val(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        res = func(*args, **kwargs)
        t2 = time.time()
        gap_time = t2 - t1
        if gap_time >= 60:
            print(f"{func.__name__} 걸린 시간 : {round((gap_time / 60), 4)} 분, "
                  f"params : {args!r}{kwargs!r}")
        else:
            print(f"{func.__name__} 걸린 시간 : {round((gap_time / 60), 4)} 초, "
                  f"params : {args!r}{kwargs!r}")
        return res

    return wrapper

