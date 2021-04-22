import multiprocessing as mp
import os
import signal
import unittest

from mpire.signal import DelayedKeyboardInterrupt, DisableKeyboardInterruptSignal


class DelayedKeyboardInterruptTest(unittest.TestCase):

    def test_delayed_keyboard_interrupt(self):
        """
        The process should delay the keyboard interrupt in case ``in_thread=False``, so the expected value should be 1.
        However, we can't send signals to threads and so the DelayedKeyboardInterrupt doesn't do anything in that case.
        So there's no point in testing this with threading
        """
        # Create events so we know when the process has started and we can send an interrupt
        started_event = mp.Event()
        quit_event = mp.Event()
        value = mp.Value('i', 0)

        # Start process and wait until it starts
        p = mp.Process(target=self.delayed_process_job, args=(started_event, quit_event, value))
        p.start()
        started_event.wait()

        # Send kill signal and wait for it to join
        os.kill(p.pid, signal.SIGINT)
        quit_event.set()
        p.join()

        # Verify expected value.
        self.assertEqual(value.value, 1)

    @staticmethod
    def delayed_process_job(started_event: mp.Event, quit_event: mp.Event, value: mp.Value):
        """
        Should be affected by interrupt
        """
        try:
            with DelayedKeyboardInterrupt(in_thread=False):
                started_event.set()
                quit_event.wait()
                value.value = 1
        except KeyboardInterrupt:
            pass
        else:
            value.value = 2


class DisabledKeyboardInterruptTest(unittest.TestCase):

    def test_disabled_keyboard_interrupt(self):
        """
        The process should ignore a keyboard interrupt entirely, which means the expected value should be True
        """
        # Create events so we know when the process has started and we can send an interrupt
        started_event = mp.Event()
        quit_event = mp.Event()
        value = mp.Value('b', False)
        p = mp.Process(target=self.disabled_process_job, args=(started_event, quit_event, value))
        p.start()
        started_event.wait()
        os.kill(p.pid, signal.SIGINT)
        quit_event.set()
        p.join()

        # If everything worked the value should be set to True
        self.assertEqual(value.value, True)

    @staticmethod
    def disabled_process_job(started_event: mp.Event, quit_event: mp.Event, value: mp.Value):
        """
        Should not be affected by interrupt
        """
        with DisableKeyboardInterruptSignal():
            started_event.set()
            quit_event.wait()
        value.value = True
