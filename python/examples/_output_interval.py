"""Validate a retained page before consuming it; shared by the examples."""


def output_page_end(page, requested_offset):
    """Return the exclusive end of delivered bytes, not the producer head."""
    if page.from_offset != requested_offset:
        raise RuntimeError("Output page did not begin at the requested offset")
    end = page.from_offset + len(page.data)
    if end > page.next_offset:
        raise RuntimeError("Output page extends beyond the producer head")
    if page.has_more != (end < page.next_offset):
        raise RuntimeError("Output page has inconsistent continuation metadata")
    if page.has_more and end == requested_offset:
        raise RuntimeError("Output page did not advance")
    return end
