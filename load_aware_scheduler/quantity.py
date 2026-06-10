import re
from decimal import Decimal
from typing import Optional


_QUANTITY_RE = re.compile(r"^([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)([a-zA-Z]*)$")

_DECIMAL_SUFFIXES = {
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal("1"),
    "k": Decimal("1e3"),
    "K": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
}

_BINARY_SUFFIXES = {
    "Ki": Decimal(1024),
    "Mi": Decimal(1024) ** 2,
    "Gi": Decimal(1024) ** 3,
    "Ti": Decimal(1024) ** 4,
    "Pi": Decimal(1024) ** 5,
    "Ei": Decimal(1024) ** 6,
}


def _parse_quantity(value: Optional[str]) -> Decimal:
    if value is None:
        return Decimal(0)
    text = str(value).strip()
    if text == "":
        return Decimal(0)
    match = _QUANTITY_RE.match(text)
    if not match:
        raise ValueError(f"unsupported Kubernetes quantity: {value!r}")
    number, suffix = match.groups()
    multiplier = _BINARY_SUFFIXES.get(suffix)
    if multiplier is None:
        multiplier = _DECIMAL_SUFFIXES.get(suffix)
    if multiplier is None:
        raise ValueError(f"unsupported Kubernetes quantity suffix: {suffix!r}")
    return Decimal(number) * multiplier


def parse_cpu_cores(value: Optional[str]) -> float:
    return float(_parse_quantity(value))


def parse_memory_bytes(value: Optional[str]) -> int:
    return int(_parse_quantity(value))


def clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
