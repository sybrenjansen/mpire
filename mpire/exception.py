from typing import Any, Dict, Tuple

from pygments import highlight
from pygments.lexers import Python3TracebackLexer
from pygments.formatters import TerminalFormatter


class StopWorker(Exception):
    """ Exception used to kill workers from the main process """
    pass


class CannotPickleExceptionError(Exception):
    """ Exception used when Pickle has trouble pickling the actual Exception """
    pass


def highlight_traceback(traceback_str: str) -> str:
    """
    Highlight a traceback string in a terminal-friendly way

    :param traceback_str: The traceback string to highlight
    :return: The highlighted traceback string
    """
    return highlight(traceback_str, Python3TracebackLexer(), TerminalFormatter())


def populate_exception(err_type: type, err_args: Any, err_state: Dict,
                       traceback_str: str) -> Tuple[Exception, Exception]:
    """
    Populate an exception with the given arguments

    :param err_type: The type of the exception
    :param err_args: The arguments of the exception
    :param err_state: The state of the exception
    :param traceback_str: The traceback string of the exception
    :return: A tuple of the exception and the original exception
    """
    err = err_type.__new__(err_type)
    err.args = err_args
    err.__dict__.update(err_state)
    traceback_err = Exception(highlight_traceback(traceback_str))

    return err, traceback_err
