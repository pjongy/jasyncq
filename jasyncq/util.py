from typing import Optional, Any


def let_if(value, func) -> Optional[Any]:
    if value:
        return func(value)
