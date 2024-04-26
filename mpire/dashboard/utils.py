import getpass
import inspect
import socket
from functools import partial
from typing import Callable, Dict, List, Sequence, Tuple, Union
import types

DASHBOARD_FUNCTION_STACKLEVEL = 1


def get_two_available_ports(port_range: Sequence) -> Tuple[int, int]:
    """
    Get two available ports, one from the start and one from the end of the range

    :param port_range: Port range to try. Reverses the list and will then pick the first one available
    :raises OSError: If there are not enough ports available
    :return: Two available ports
    """
    def _port_available(port_nr: int) -> bool:
        """
        Checks if a port is available

        :param port_nr: Port number to check
        :return: True if available, False otherwise
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', port_nr))
            s.close()
            return True
        except OSError:
            return False

    available_ports = set()
    for port_nr in port_range:
        if _port_available(port_nr):
            available_ports.add(port_nr)
            break

    for port_nr in reversed(port_range):
        if _port_available(port_nr):
            available_ports.add(port_nr)
            break

    if len(available_ports) != 2:
        raise OSError(f"Dashboard Manager Server: there are not enough ports available: {port_range}")

    return tuple(sorted(available_ports))


def get_stacklevel() -> int:
    """
    Gets the stack level to use when obtaining function details (used for the dashboard)

    :return: Stack level
    """
    return DASHBOARD_FUNCTION_STACKLEVEL


def set_stacklevel(stacklevel: int) -> None:
    """
    Sets the stack level to use when obtaining function details (used for the dashboard)

    :param stacklevel: Stack level
    """
    global DASHBOARD_FUNCTION_STACKLEVEL
    DASHBOARD_FUNCTION_STACKLEVEL = stacklevel
     

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
    # Get the frame in which the pool.map(...) was called. We obtain the current stack and skip all frames which
    # involve the current mpire module. If the desired stack level is higher than 1, we continue until we've reached 
    # the desired stack level. We then obtain the code context of that frame.
    invoked_frame = None
    stacklevel = 0
    for frame_info in inspect.stack():
        if frame_info.frame.f_globals['__name__'].split('.')[0] != 'mpire' or stacklevel > 0:
            invoked_frame = frame_info
            stacklevel += 1
            if stacklevel == DASHBOARD_FUNCTION_STACKLEVEL:
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

    if isinstance(func, partial):
        # If we're dealing with a partial, obtain the function within
        func = func.func
    elif hasattr(func, '__call__') and not isinstance(func, (type, types.FunctionType, types.MethodType)):
        # If we're dealing with a callable class instance, use its __call__ method
        func = func.__call__

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

    # Obtain user. This can fail when the current uid refers to a non-existing user, which can happen when running in a
    # container as a non-root user. See https://github.com/sybrenjansen/mpire/issues/128.
    try:
        user = getpass.getuser()
    except KeyError:
        user = "n/a"
        
    # Populate details
    func_details = {'user': f'{user}@{socket.gethostname()}',
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
