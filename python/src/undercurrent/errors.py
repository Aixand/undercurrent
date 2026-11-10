"""Public errors retain server categories without claiming mutation retry safety."""


class UndercurrentError(Exception):
    """Retain available error evidence without inferring missing fields.

    A missing effect_certainty does not establish that no mutation took effect;
    category alone cannot authorize retry. See
    [error evidence](../../../docs/sdk/python/recovery.md#structured-error-fields).
    """
    def __init__(self, message, *, category=None, product_category=None, disposition=None, effect_certainty=None):
        super().__init__(message)
        self.category = category
        self.product_category = product_category
        self.disposition = disposition
        self.effect_certainty = effect_certainty

    @classmethod
    def from_wire(cls, value):
        return cls(**value)


class SessionEnded(UndercurrentError):
    def __init__(self, session):
        super().__init__(f"Session {session.id} reached {session.lifecycle} before the requested state")
        self.session = session


class SessionRemoved(UndercurrentError):
    pass


class OutputGap(UndercurrentError):
    def __init__(self, available_from):
        super().__init__(f"Output retention gap; available bytes start at {available_from}")
        self.available_from = available_from
