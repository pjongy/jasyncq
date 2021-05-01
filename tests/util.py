import random
import string


def random_string_lower(length=10):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))
