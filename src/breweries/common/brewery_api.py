from pydantic.dataclasses import dataclass
from typing import List
import dataclasses

INDEX_ISO_CODE = 0
INDEX_API_BY_COUNTRY = 1

ISO_3166_US = ["US", "United%20States"]
ISO_3166_SOUTH_KOREA = ["KR", "south%20korea"]
ISO_3166_AUSTRIA = ["AT", "austria"]
ISO_3166_ENGLAND = ["GB-EN", "england"]
ISO_3166_FRANCE = ["FR", "france"]
ISO_3166_ISLE_OF_MAN = ["IM", "isle%20of%20man"]
ISO_3166_SCOTLAND = ["GB-SCT", "scotland"]
ISO_3166_IRELAND = ["IE", "ireland"]
ISO_3166_SINGAPORE = ["SG", "singapore"]
ISO_3166_PORTUGAL = ["PT", "portugal"]
ISO_3166_POLAND = ["PL", "poland"]

ISO_COUNTRY_CODES = [
    ISO_3166_US,  # 0
    ISO_3166_SOUTH_KOREA,  # 1
    ISO_3166_AUSTRIA,  # 2
    ISO_3166_ENGLAND,  # 3
    ISO_3166_FRANCE,  # 4
    ISO_3166_ISLE_OF_MAN,  # 5
    ISO_3166_SCOTLAND,  # 6
    ISO_3166_IRELAND,  # 7
    ISO_3166_SINGAPORE,  # 8
    ISO_3166_PORTUGAL,  # 9
    ISO_3166_POLAND,  # 10
]

BASE_URL_COUNTRY = "https://api.openbrewerydb.org/v1/breweries?by_country"


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
            iso_3166_code=ISO_COUNTRY_CODES[0][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[0][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[1][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[1][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[2][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[2][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[3][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[3][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[4][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[4][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[5][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[5][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[6][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[6][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[7][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[7][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[8][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[8][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[9][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[9][INDEX_API_BY_COUNTRY]}"
        ),
        SingleCountry(
            iso_3166_code=ISO_COUNTRY_CODES[10][INDEX_ISO_CODE],
            url=f"{BASE_URL_COUNTRY}={ISO_COUNTRY_CODES[10][INDEX_API_BY_COUNTRY]}"
        ),
    ]
)


def get_url_for_country(iso_code) -> SingleCountry:
    for variant in COUNTRY_VARIANTS.variants:
        if variant.iso_3166_code == iso_code:
            return variant
    return None


def get_list_of_countries() -> List[str]:
    return [country_code.iso_3166_code for country_code in COUNTRY_VARIANTS.variants]

