from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Session:
    """A snapshot, never an owner of the underlying process.

    Treat raw as read-only: freezing the dataclass does not freeze its dictionary,
    and the properties read directly from it without fetching fresh state.
    """
    raw: dict[str, Any] = field(repr=False)

    @property
    def id(self): return self.raw["id"]
    @property
    def name(self): return self.raw["name"]
    @property
    def revision(self): return self.raw["revision"]
    @property
    def lifecycle(self): return self.raw["lifecycle"]["state"]
    @property
    def execution(self): return self.raw["foreground"]["execution"]["state"]
    @property
    def interaction(self): return self.raw["foreground"]["interaction"]["state"]
    @property
    def constraint(self): return self.raw["foreground"]["constraint"]["state"]
    @property
    def health(self): return self.raw["health"]["status"]


@dataclass(frozen=True)
class InputReceipt:
    session_id: str
    delivered_at_millis: int
    delivery_id: str | None
    duplicate: bool
    foreground_revision: int | None = None


@dataclass(frozen=True)
class OutputPage:
    """A retained byte page with the producer head in next_offset.

    Continue at from_offset + len(data); advancing to next_offset can skip bytes
    when the page is shorter than the available output. See
    [continuation](../../../docs/sdk/python/streams.md#continue-at-the-end-of-the-returned-bytes).
    """
    data: bytes = field(repr=False)
    from_offset: int
    next_offset: int
    has_more: bool

    def text(self, encoding="utf-8", errors="replace"):
        """Decode this page only, with replacement by default.

        Pages may split multibyte characters. Use an incremental decoder over
        data when text must be preserved across page boundaries.
        """
        return self.data.decode(encoding, errors)


@dataclass(frozen=True)
class SessionUpdate:
    session: Session
    resynchronized: bool = False
