import signal


class DelayedKeyboardInterrupt(object):

    def __init__(self, in_thread: bool = False):
        """
        :param in_thread: Whether or not we're living in a thread or not
        """
        self.in_thread = in_thread

    def __enter__(self):
        # When we're in a thread we can't use signal handling
        if not self.in_thread:
            self.signal_received = False
            self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig, frame):
        self.signal_received = (sig, frame)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.in_thread:
            signal.signal(signal.SIGINT, self.old_handler)
            if self.signal_received:
                self.old_handler(*self.signal_received)


class DisableKeyboardInterruptSignal:

    def __enter__(self):
        # Prevent signal from propagating to child process
        self._handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore signal
        signal.signal(signal.SIGINT, self._handler)
