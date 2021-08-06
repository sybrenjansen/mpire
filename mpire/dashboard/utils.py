import getpass
import inspect
from functools import partial

import socket
from typing import Callable, Dict, List, Union


def get_function_details(func: Callable) -> Dict[str, Union[str, int]]:
    """
    Obtain function details, including:

    - function filename
    - function line number
    - function name
    - invoked from filename
    - invoked from line number
    - invoked code context

    :param func: Function to call each time new task arguments become available. When passing on the worker ID the
        function should receive the worker ID as its first argument. If shared objects are provided the function should
        receive those as the next argument. If the worker state has been enabled it should receive a state variable as
        the next argument
    :return: Function details dictionary
    """
    # Get the frame in which the pool.map(...) was called. We obtain the current stack and skip all those which
    # involve the current mpire module and stop right after that
    invoked_frame = None
    for frame_info in inspect.stack():
        if frame_info.frame.f_globals['__name__'].split('.')[0] != 'mpire':
            invoked_frame = frame_info
            break

    # Obtain proper code context. Usually the last line of the invoked code is returned, but we want the complete
    # code snippet that called this function. That's why we increase the context size and need to find the start and
    # ending of the snippet. A context size of 10 should suffice. The end of the snippet is where we encounter the
    # line found when context=1 (i.e., what is returned in invoked_frame.code_context). The start is where we see
    # something along the lines of `.[i]map[_unordered](`.
    code_context = inspect.getframeinfo(invoked_frame.frame, context=10).code_context
    if code_context is not None:
        code_context = code_context[:code_context.index(invoked_frame.code_context[0]) + 1]
        code_context = find_calling_lines(code_context)
        invoked_line_no = invoked_frame.lineno - (len(code_context) - 1)
        code_context = ' '.join(line.strip() for line in code_context)
    else:
        invoked_line_no = 'N/A'

    # If we're dealing with a partial, obtain the function within
    if isinstance(func, partial):
        func = func.func

    # We use a try/except block as some constructs don't allow this. E.g., in the case the function is a MagicMock
    # (i.e., in unit tests) these inspections will fail
    try:
        function_filename = inspect.getabsfile(func)
        function_line_no = func.__code__.co_firstlineno
        function_name = func.__name__
    except:
        function_filename = 'n/a'
        function_line_no = 'n/a'
        function_name = 'n/a'

    # Populate details
    func_details = {'user': '{}@{}'.format(getpass.getuser(), socket.gethostname()),
                    'function_filename': function_filename,
                    'function_line_no': function_line_no,
                    'function_name': function_name,
                    'invoked_filename': invoked_frame.filename,
                    'invoked_line_no': invoked_line_no,
                    'invoked_code_context': code_context}

    return func_details


def find_calling_lines(code_context: List[str]) -> List[str]:
    """
    Tries to find the lines corresponding to the calling function

    :param code_context: List of code lines
    :return: List of code lines
    """
    # Traverse the lines in reverse order. We need a closing bracket to indicate the end of the calling function. From
    # that point on we work our way backward until we find the corresponding opening bracket. There can be more bracket
    # groups in between, so we have to keep counting brackets until we've found the right one.
    n_parentheses_groups = 0
    found_parentheses_group = False
    inside_string = False
    inside_string_ch = None
    line_nr = 1
    for line_nr, line in enumerate(reversed(code_context), start=1):
        for ch in reversed(line):

            # If we're inside a string keep ignoring characters until we find the closing string character
            if inside_string:
                if ch == inside_string_ch:
                    inside_string = False

            # Check if a string has started
            elif ch in {'"', "'"}:
                inside_string = True
                inside_string_ch = ch

            # Closing parenthesis group
            elif ch == ')':
                n_parentheses_groups += 1
                found_parentheses_group = True

            # Starting parenthesis group
            elif ch == '(':
                n_parentheses_groups -= 1

        # Check if we've found the corresponding opening bracket
        if found_parentheses_group and n_parentheses_groups == 0:
            break

    return code_context[-line_nr:]
