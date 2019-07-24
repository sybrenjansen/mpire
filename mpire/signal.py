from signal import getsignal, SIG_IGN, SIGINT, signal as signal_, Signals


class DelayedKeyboardInterrupt(object):

    def __init__(self, in_thread: bool = False):
        """
        :param in_thread: Whether or not we're living in a thread or not
        """
        self.in_thread = in_thread
        self.signal_received = None

    def __enter__(self):
        # When we're in a thread we can't use signal handling
        if not self.in_thread:
            self.signal_received = False
            self.old_handler = signal_(SIGINT, self.handler)

    def handler(self, sig, frame):
        self.signal_received = (sig, frame)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.in_thread:
            signal_(SIGINT, self.old_handler)
            if self.signal_received:
                self.old_handler(*self.signal_received)


class DisableSignal:

    def __init__(self, signal: Signals = SIGINT):
        """
        :param signal: signal to suppress
        """
        self.signal = signal

    def __enter__(self):
        # Prevent signal from propagating to child process
        self._handler = getsignal(self.signal)
        signal_(self.signal, SIG_IGN)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore signal
        signal_(self.signal, self._handler)
