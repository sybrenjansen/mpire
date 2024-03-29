from typing import Callable


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
