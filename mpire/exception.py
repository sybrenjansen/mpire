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
