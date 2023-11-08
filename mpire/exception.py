import re
from typing import Any, Dict, Tuple

from pygments import highlight
from pygments.lexers import Python3TracebackLexer
from pygments.formatters import TerminalFormatter

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class StopWorker(Exception):
    """ Exception used to kill a worker """
    pass


class InterruptWorker(Exception):
    """ Exception used to interrupt a worker """
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


def remove_highlighting(traceback_str: str) -> str:
    """
    Remove the highlighting from a traceback string

    Taken from https://stackoverflow.com/a/14693789/4486236.

    :param traceback_str: The traceback string to remove the highlighting from
    :return: The traceback string without highlighting
    """
    return ANSI_ESCAPE.sub('', traceback_str)


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
