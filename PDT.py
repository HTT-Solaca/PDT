import pandas as pd
import functools
import inspect
import warnings

def PD_TOL(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

            sig = inspect.signature(func)
            ba = sig.bind_partial(*args, **kwargs)
            ba.apply_defaults()
        df = None
        df_name = None
        df_in_globals = False
        for name, value in ba.arguments.items():
            if isinstance(value, pd.DataFrame):
                df = value
                df_name = name
                df_in_globals = False
                break
        if df is None:
            for name, val in func.__globals__.items():
                if isinstance(val, pd.DataFrame):
                    df = val
                    df_name = name
                    df_in_globals = True
                    break
        if df is None:
            return func(*args, **kwargs)
        last_len = -1
        bad_rows = set()

        while last_len != len(df):
            last_len = len(df)
            try:
                if df_in_globals:
                    func.__globals__[df_name] = df.copy()
                    func(*args, **kwargs)
                    df = func.__globals__[df_name]
                else:
                    ba.arguments[df_name] = df.copy()
                    result = func(*ba.args, **ba.kwargs)
                    if result is not None:
                        df = result
                break

            except Exception as e:
                #print(f"[PD_TOL] Warning: Exception：{e}")
                bad_rows = set()
                for i in df.index:
                    tmp = df.iloc[[i]].copy()
                    try:
                        if df_in_globals:
                            func.__globals__[df_name] = tmp
                            func(*args, **kwargs)
                        else:
                            ba.arguments[df_name] = tmp
                            func(*ba.args, **ba.kwargs)
                    except Exception:
                        bad_rows.add(i)
                if bad_rows:
                    #print(f"[PD_TOL] Skipping rows：{sorted(bad_rows)}")
                    df = df.drop(list(bad_rows)).reset_index(drop=True)
                else:
                    break

                if len(df) == 0:
                    if df_in_globals:
                        func.__globals__[df_name] = df.copy()
                    return
        return df
    return wrapper