import json

import anthropic

from . import config

SYSTEM_PROMPT = """You analyse Instagram post captions to extract discount/promo codes.

For each post, decide whether the caption advertises a discount code that a consumer can use, and if so extract every code mentioned.

Be strict:
- A "discount code" is a specific token (letters/numbers) the user types at checkout to get a discount.
- Hashtags, ad disclosures (#ad, #adv, #gifted), affiliate links without a typeable code, and giveaway entries are NOT discount codes.
- The company is the brand whose products the code applies to (usually the @-mentioned brand or the brand named in the caption), NOT the influencer posting it.
- If you cannot identify the company with reasonable confidence (e.g. the caption mentions a code but no brand, or only the influencer's own handle), set has_discount_code: false for that post and emit no codes. Do NOT guess. NEVER emit a company name that is — or contains — any of: "Unknown", "Unknown Brand", "Unknown [anything] Brand" (e.g. "Unknown Electric Mop Brand"), "N/A", "Unspecified", "Various", "Multiple", "The Brand", "The Shop", "Brand", or any similar placeholder. A code without a confidently-named real brand is unusable to a consumer — drop the entire post by setting has_discount_code: false.
- ONE BRAND PER CODE. If a single code seems to apply to multiple brands at once (e.g. "use MOUNIA20 at @celini and @kerastase"), pick the SINGLE most clearly-attributed brand and emit only that one. NEVER join brand names with "&", "/", "+", " and ", or " x " (e.g. "Celini & Kérastase", "BrandA / BrandB"). If you genuinely can't tell which brand the code is for, drop the post (has_discount_code: false).
- REFERRAL TOKENS ARE NOT DISCOUNT CODES. Some brands — notably Shein and other global fast-fashion / marketplace retailers — give influencers personal "referral codes" or "referral links". These are commission tokens for the influencer, not consumer discounts. They typically look like: random 5-character alphanumeric strings (e.g. "B8QTA", "E5MFW", "RN4PV"), OR longer mixed-case tokens with embedded usernames and digits (e.g. "RF4raniivdb047", "USQS4lisajkoster479", "4UKCURVEtynafey111"). Do NOT emit these as discount codes. If a post mentions only such referral tokens (no real promo code with a stated discount), set has_discount_code: false.
- Extract the discount itself when stated. Always populate two fields:
    * value: a short, frontend-friendly label — at most ~20 characters. Examples: "20%", "€10 off", "Free shipping", "BOGO", "2 for 1", "$5 off". Use the most concrete form stated in the caption. If the caption does not state any concrete discount amount or kind, set value to an empty string ""; do NOT invent a placeholder. NEVER emit any of these vague words alone as a value: "Coupon", "Discount", "Promo", "Code", "Deal", "Sale", "Offer", "Voucher", "Savings". Those mean "I don't know the amount" — leave value empty in that case.
    * discount_description: a one-sentence English description with any extra context (e.g. "20% off your first order", "Free shipping on orders over €50").
- Set percentage to the integer percent only when the discount is a percentage; otherwise null.
- If the caption is in a non-English language (Dutch, German, etc.) translate value and discount_description to English.
- If a post does not contain a usable discount code, return has_discount_code: false and an empty discount_codes array.

Return one result object per input post, in the same order, keyed by post_index."""


def _build_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "post_index": {"type": "integer"},
                        "has_discount_code": {"type": "boolean"},
                        "discount_codes": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "code": {"type": "string"},
                                    "company": {"type": "string"},
                                    "value": {"type": "string"},
                                    "discount_description": {"type": "string"},
                                    "percentage": {"type": ["integer", "null"]},
                                },
                                "required": ["code", "company", "value", "discount_description", "percentage"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": ["post_index", "has_discount_code", "discount_codes"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["results"],
        "additionalProperties": False,
    }


def _format_batch(batch: list[dict]) -> str:
    lines = []
    for idx, item in enumerate(batch):
        post = item["post"]
        caption = (post.get("caption") or "")[: config.CAPTION_MAX_CHARS]
        mentions = post.get("mentions") or []
        owner = item["profile"].get("username")
        lines.append(
            f"--- post_index: {idx} ---\n"
            f"posted_by: @{owner}\n"
            f"mentions: {', '.join('@' + m for m in mentions) or '(none)'}\n"
            f"caption:\n{caption}\n"
        )
    return "\n".join(lines)


def analyze_batch(client: anthropic.Anthropic, batch: list[dict]) -> list[dict]:
    """Send a batch of posts to Claude and return raw extraction results."""
    user_content = (
        "Analyse the following Instagram posts and return a JSON object matching the schema.\n\n"
        + _format_batch(batch)
    )

    response = client.messages.create(
        model=config.MODEL,
        max_tokens=4096,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": _build_schema(),
            }
        },
        messages=[{"role": "user", "content": user_content}],
    )

    text = next(b.text for b in response.content if b.type == "text")
    parsed = json.loads(text)
    return parsed["results"]
