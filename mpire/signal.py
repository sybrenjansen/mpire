from inspect import Traceback
from signal import getsignal, SIG_IGN, SIGINT, signal as signal_, Signals
from threading import current_thread, main_thread
from types import FrameType
from typing import Type


class DelayedKeyboardInterrupt:

    def __init__(self) -> None:
        self.signal_received = None

    def __enter__(self) -> None:
        # When we're in a thread we can't use signal handling
        if current_thread() == main_thread():
            self.signal_received = False
            self.old_handler = signal_(SIGINT, self.handler)

    def handler(self, sig: Signals, frame: FrameType) -> None:
        self.signal_received = (sig, frame)

    def __exit__(self, exc_type: Type, exc_val: Exception, exc_tb: Traceback) -> None:
        if current_thread() == main_thread():
            signal_(SIGINT, self.old_handler)
            if self.signal_received:
                self.old_handler(*self.signal_received)


class DisableKeyboardInterruptSignal:

    def __enter__(self) -> None:
        if current_thread() == main_thread():
            # Prevent signal from propagating to child process
            self._handler = getsignal(SIGINT)
            ignore_keyboard_interrupt()

    def __exit__(self, exc_type: Type, exc_val: Exception, exc_tb: Traceback) -> None:
        if current_thread() == main_thread():
            # Restore signal
            signal_(SIGINT, self._handler)


def ignore_keyboard_interrupt():
    signal_(SIGINT, SIG_IGN)
