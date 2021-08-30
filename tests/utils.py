from datetime import datetime
from typing import Callable


class MockDatetimeNow(datetime):
    """ Mocked datetime class which returns the predefined return values in order when .now() is called """
    RETURN_VALUES = []
    CURRENT_IDX = 0

    @classmethod
    def now(cls, *_):
        cls.CURRENT_IDX += 1
        return cls.RETURN_VALUES[cls.CURRENT_IDX - 1]


class ConditionalDecorator:

    def __init__(self, decorator: Callable, condition: bool) -> None:
        """
        Decorator which takes a decorator and a condition as input. Only when the condition is met the decorator is used

        :param decorator: Decorator
        :param condition: Condition (boolean)
        """
        self.decorator = decorator
        self.condition = condition

    def __call__(self, func) -> Callable:
        """
        Enables the conditional decorator

        :param func: Function to decorated
        :return: Decorated function if condition is met, otherwise just the function
        """
        if self.condition:
            return self.decorator(func)
        else:
            return func

