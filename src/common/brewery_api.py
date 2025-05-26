from pydantic.dataclasses import dataclass
from typing import Callable, List, Optional, Set, Union, cast
import dataclasses

ISO_3166_US = "US"


@dataclass
class SingleCountry:
    iso_3166_code: str
    url: str

    def __post_init__(self):
        pass


@dataclass
class CountryVariants:
    variants: List[SingleCountry] = dataclasses.field(default_factory=lambda: [])

    def __iter__(self):
        return iter(self.variants)


COUNTRY_VARIANTS = CountryVariants(
    variants=[
        SingleCountry(
            iso_3166_code="US",
            url="https://api.openbrewerydb.org/v1/breweries?by_country=United%20States"
        ),
    ]
)


def get_url_for_country(iso_code) -> SingleCountry:
    for variant in COUNTRY_VARIANTS.variants:
        if variant.iso_3166_code == iso_code:
            return variant
    return None
