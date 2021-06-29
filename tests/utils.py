from datetime import datetime


class MockDatetimeNow(datetime):
    """ Mocked datetime class which returns the predefined return values in order when .now() is called """
    RETURN_VALUES = []
    CURRENT_IDX = 0

    @classmethod
    def now(cls, *_):
        cls.CURRENT_IDX += 1
        return cls.RETURN_VALUES[cls.CURRENT_IDX - 1]
