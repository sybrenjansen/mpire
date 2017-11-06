class StopWorker(Exception):
    """ Exception used to kill workers from the main process """
    pass


class CannotPickleExceptionError(Exception):
    """ Exception used when Pickle has trouble pickling the actual Exception """
    pass
