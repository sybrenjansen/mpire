from inspect import Traceback
from signal import getsignal, SIG_IGN, SIGINT, signal as signal_, Signals
from types import FrameType
from typing import Type


class DelayedKeyboardInterrupt(object):

    def __init__(self, in_thread: bool = False) -> None:
        """
        :param in_thread: Whether or not we're living in a thread or not
        """
        self.in_thread = in_thread
        self.signal_received = None

    def __enter__(self) -> None:
        # When we're in a thread we can't use signal handling
        if not self.in_thread:
            self.signal_received = False
            self.old_handler = signal_(SIGINT, self.handler)

    def handler(self, sig: Signals, frame: FrameType) -> None:
        self.signal_received = (sig, frame)

    def __exit__(self, exc_type: Type, exc_val: Exception, exc_tb: Traceback) -> None:
        if not self.in_thread:
            signal_(SIGINT, self.old_handler)
            if self.signal_received:
                self.old_handler(*self.signal_received)


class DisableKeyboardInterruptSignal:

    def __enter__(self) -> None:
        # Prevent signal from propagating to child process
        self._handler = getsignal(SIGINT)
        signal_(SIGINT, SIG_IGN)

    def __exit__(self, exc_type: Type, exc_val: Exception, exc_tb: Traceback) -> None:
        # Restore signal
        signal_(SIGINT, self._handler)
