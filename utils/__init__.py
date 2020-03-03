def print_execute_time(display_arg_and_return=False):
    import timeit
    def decorator(func):
        def wrapper(*args, **kwargs):
            t0 = timeit.default_timer()
            result = func(*args, **kwargs)
            elapsed = timeit.default_timer() - t0
            arg_str = ', '.join(repr(arg) for arg in args)
            print('[%0.8fs] %s' % (elapsed, func.__name__,) +
                  ("(%s)->%s" % (arg_str, result) if display_arg_and_return else ""))
            return result

        return wrapper

    return decorator
