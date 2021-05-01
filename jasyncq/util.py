from typing import Optional, Any


def let_if(value, func) -> Optional[Any]:
    if value is not None:
        return func(value)
    return value
