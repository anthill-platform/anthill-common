

class Singleton(type):

    """
    A metaclass for singleton pattern
    """

    objects = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.objects:
            cls.objects[cls] = super(Singleton, cls).__call__(
                *args,
                **kwargs)

        return cls.objects[cls]
