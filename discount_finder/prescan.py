import re

# Multilingual indicators of a discount-code post. Hits favour recall over
# precision — the LLM stage filters false positives.
_KEYWORDS = re.compile(
    r"""
    kortingscode | korting          # NL
  | discount | promo[\s-]?code | promocode | use\s+code | with\s+code
  | coupon
  | gebruik\s+code | met\s+code
  | rabattcode | rabatt | gutschein # DE
  | code\s*[:=]                     # "code: XXX" or "code=XXX"
  | \b\d{1,2}\s*%\s*(off|korting|rabatt) # "20% off", "20% korting"
  | \bcodice\b | \bsconto\b         # IT
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Tokens that look like a real coupon code: 4–20 chars, mostly alphanumeric,
# at least one letter and one digit OR all-caps. Reduces misfires on plain words.
_CODE_TOKEN = re.compile(r"\b(?=[A-Z0-9]*\d)[A-Z][A-Z0-9]{3,19}\b|\b[A-Z]{4,20}\d{1,4}\b")


def is_likely_discount_post(caption: str) -> bool:
    if not caption:
        return False
    if _KEYWORDS.search(caption):
        return True
    # Fall back: a token that *looks* like a code in a short caption is also worth checking.
    if len(caption) < 600 and _CODE_TOKEN.search(caption):
        return True
    return False
