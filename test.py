import multiprocessing
import fdhandle

print('main process:', multiprocessing.current_process().pid)

fdhandle.init()
fdhandle.update_day(True)

