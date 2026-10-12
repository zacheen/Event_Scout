"""What one reader wants from an event, as config.yaml states it.

The scorers hold no preferences of their own. Which kinds of event are worth
attending, which topics count as a fit, what travel costs and which language a
reason is written in all arrive here, so a different reader changes
config.yaml rather than code.

The access and travel blocks each have two halves because the two scorer tiers
read differently. The LLM tiers read prose, the `describe` lines. The keyword
tier reads weighted substrings, `keywords` and `places`. Both halves sit side
by side so they are edited together, and the access levels share one 1 to 5
scale so both tiers answer the same question.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

ACCESS_LEVELS = (1, 2, 3, 4, 5)
# Both tiers multiply a level by this to reach the 1-10 scale fit uses. The
# LLM prompt hardcodes it in words ("scaled to 1-10 by doubling"), so changing
# this number means rewording the prompt too.
ACCESS_SCALE = 2


@dataclass(frozen=True)
class TravelCost:
    virtual: int
    default: int
    describe: Mapping[int, str]
    places: Mapping[str, int]


@dataclass(frozen=True)
class ScoringPreferences:
    access_levels: Mapping[int, str]
    access_keywords: Mapping[str, int]
    fit_terms: Mapping[str, int]
    fit_penalty: Mapping[str, int]
    travel: TravelCost
    reason_language: str


def parse_preferences(event_preferences, fit_keywords, travel_cost,
                      reason_language) -> ScoringPreferences:
    """Validate the four config.yaml blocks and freeze them.

    There is no fallback for a missing or malformed block, because a default
    here would be a second copy of config.yaml's and the two would drift.
    """
    access = _block("event_preferences", event_preferences, {"describe", "keywords"})
    fit = _block("fit_keywords", fit_keywords, {"terms", "penalty"})
    travel = _block("travel_cost", travel_cost, {"virtual", "default", "describe", "places"})
    levels = _describe("event_preferences.describe", access["describe"])
    if set(levels) != set(ACCESS_LEVELS):
        # Exactly five, because digest_min_rank, p0_min_rank and p1_min_rank
        # are tuned on rank = fit * access_value with access doubled to 1-10.
        # Another level count would keep those numbers while changing what
        # they mean.
        raise ValueError("config.yaml event_preferences.describe must define levels "
                         f"1 to 5 exactly, got {sorted(levels)}")
    regions = _describe("travel_cost.describe", travel["describe"])
    places = _weights("travel_cost.places", travel["places"], 1, 10)
    if stray := sorted({lvl for lvl in places.values() if lvl not in regions}):
        # A place level with no describe line is a cost the keyword tier
        # assigns and the LLM tiers are never told about.
        raise ValueError(f"config.yaml travel_cost.places uses level(s) {stray} that "
                         "travel_cost.describe does not define")
    if not isinstance(reason_language, str) or not reason_language.strip():
        raise ValueError("config.yaml reason_language must be a non-empty string")
    return ScoringPreferences(
        access_levels=levels,
        access_keywords=_weights("event_preferences.keywords", access["keywords"], 1, 5),
        fit_terms=_weights("fit_keywords.terms", fit["terms"], 1, 10),
        fit_penalty=_weights("fit_keywords.penalty", fit["penalty"], -10, -1),
        travel=TravelCost(
            virtual=_level("travel_cost.virtual", travel["virtual"]),
            default=_level("travel_cost.default", travel["default"]),
            describe=regions, places=places),
        reason_language=reason_language.strip())


def _block(name: str, value, keys: set[str]) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"config.yaml {name} is missing or not a mapping")
    if set(value) != keys:
        raise ValueError(f"config.yaml {name} must have exactly the keys "
                         f"{', '.join(sorted(keys))}, got {', '.join(sorted(map(str, value)))}")
    return value


def _is_int(value) -> bool:
    # bool is an int subclass, so `true` would otherwise pass as a weight of 1.
    return isinstance(value, int) and not isinstance(value, bool)


def _level(name: str, value) -> int:
    if not _is_int(value) or not 1 <= value <= 10:
        raise ValueError(f"config.yaml {name}={value!r} must be an integer from 1 to 10")
    return value


def _describe(name: str, value) -> Mapping[int, str]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"config.yaml {name} must be a non-empty mapping")
    for key, text in value.items():
        # A quoted key such as "5" arrives as a string and would silently
        # never match the integer level it was meant to describe.
        if not _is_int(key) or not 1 <= key <= 10:
            raise ValueError(f"config.yaml {name} key {key!r} must be an unquoted "
                             "integer from 1 to 10")
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"config.yaml {name}.{key} must be a non-empty string")
    return MappingProxyType({key: value[key].strip() for key in sorted(value)})


def _weights(name: str, value, low: int, high: int) -> Mapping[str, int]:
    if not isinstance(value, dict):
        raise ValueError(f"config.yaml {name} must be a mapping of term to weight")
    weights = {}
    for term, weight in value.items():
        if not isinstance(term, str) or not term.strip():
            raise ValueError(f"config.yaml {name} has an empty or non-text term {term!r}")
        if not _is_int(weight) or not low <= weight <= high:
            raise ValueError(f"config.yaml {name}[{term!r}]={weight!r} must be an "
                             f"integer from {low} to {high}")
        key = term.strip().lower()
        if key in weights:
            # "Career" and "career" are one term once lowered, and keeping
            # whichever came last would silently drop the other weight.
            raise ValueError(f"config.yaml {name} lists {key!r} twice once case and "
                             "surrounding spaces are ignored")
        weights[key] = weight
    return MappingProxyType(weights)
