import re
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


BASE_NEGATIVE_THRESHOLD = 4.0
PARTY_NEGATIVE_THRESHOLD = 5.5
UNITED_RUSSIA_NEGATIVE_THRESHOLD = 6.0
DEPUTY_STRICT_THRESHOLD = 5.5

NEGATIVE_SIGNAL_RULES: List[Tuple[str, List[Tuple[object, float]]]] = [
    (
        "corruption_fraud",
        [
            ("коррупц", 5.0),
            ("взятк", 5.0),
            ("откат", 4.5),
            ("хищен", 4.5),
            ("растрат", 4.5),
            ("мошеннич", 5.0),
            ("распил", 4.5),
            ("присвоен", 4.0),
            ("обман", 4.0),
            ("афер", 4.0),
            ("злоупотреблен", 4.5),
            ("превышен полномоч", 4.5),
            ("конфликт интересов", 5.0),
            ("аффилирован", 4.5),
            ("скрыт", 3.0),
            ("не задекларир", 5.0),
        ],
    ),
    (
        "election_manipulation",
        [
            ("фальсификац", 5.0),
            ("вброс", 5.0),
            ("карусел", 5.0),
            ("подкуп избират", 5.0),
            ("админресурс", 4.5),
            ("накрут", 4.5),
            ("переписан протокол", 5.0),
            ("нарушен на выбор", 5.0),
            ("принужд к голосован", 5.0),
            ("давлен на избират", 4.5),
            ("сняли кандидата", 4.0),
            ("недопуск", 4.0),
        ],
    ),
    (
        "censorship_blocking",
        [
            ("заблокир", 5.0),
            ("блокиров", 5.0),
            ("замедл", 4.5),
            ("цензур", 4.5),
            ("роскомнадзор", 4.5),
            (re.compile(r"\bркн\b"), 4.5),
            ("реестр запрещен", 5.0),
            ("запрет vpn", 5.0),
            ("запрет прокси", 4.5),
            ("удален контент", 4.0),
            ("иноагент", 4.5),
            ("экстремистск", 4.0),
        ],
    ),
    (
        "repression_courts",
        [
            ("арест", 5.0),
            ("задерж", 5.0),
            ("обыск", 5.0),
            ("приговор", 4.5),
            ("уголовн", 4.0),
            ("политзаключ", 5.0),
            ("репресс", 5.0),
            ("преследован", 4.5),
            ("пытк", 5.0),
            ("сизо", 4.0),
            ("колони", 3.5),
            ("штраф", 3.0),
            ("запрет митинг", 5.0),
            ("разгон митинг", 5.0),
            ("массовые задержан", 5.0),
        ],
    ),
    (
        "war_mobilization_harm",
        [
            ("мобилиз", 5.0),
            ("повестк в военкомат", 5.0),
            ("военн повестк", 5.0),
            ("мобилизационн повестк", 5.0),
            ("военкомат", 4.5),
            ("призыв в армию", 4.5),
            ("призывник", 4.5),
            ("призывной", 4.0),
            ("осенний призыв", 4.0),
            ("весенний призыв", 4.0),
            ("негодн", 4.5),
            ("срочник", 4.0),
            ("погиб", 4.5),
            ("ранен", 4.0),
            ("без вести", 4.5),
            ("принуд", 4.5),
            ("отправили на фронт", 5.0),
            ("уклонен", 3.5),
            ("электронн повестк", 5.0),
            ("реестр повесток", 5.0),
        ],
    ),
    (
        "economic_harm",
        [
            ("санкц", 4.0),
            ("обвал", 5.0),
            ("инфляц", 4.5),
            ("подорож", 4.5),
            ("рост цен", 4.5),
            ("ключева ставка", 4.5),
            ("налог", 3.5),
            ("тариф", 3.5),
            ("банкрот", 4.5),
            ("дефицит", 4.0),
            ("сокращен", 3.0),
            ("увольнен", 3.5),
            ("паден доход", 4.5),
            ("заморозк актив", 5.0),
            ("блокировк счет", 4.5),
        ],
    ),
    (
        "state_coercion_surveillance",
        [
            ("слежк", 5.0),
            ("прослушк", 5.0),
            ("сорм", 5.0),
            ("распознаван лиц", 5.0),
            ("утечк персональн", 4.5),
            ("принуд установ", 5.0),
            ("принудительн установ", 5.0),
            ("обязали установ", 5.0),
            ("обязательн установ", 5.0),
            ("установить max", 5.0),
            ("установить макс", 5.0),
            ("мессенджер max", 4.0),
            ("мессенджер макс", 4.0),
            ("цифровой профиль", 3.5),
            ("биометр", 4.0),
            ("электронн реестр", 3.5),
        ],
    ),
    (
        "covid_restrictions",
        [
            ("ковид", 3.0),
            ("коронавирус", 3.0),
            ("qr-код", 4.5),
            ("куар-код", 4.5),
            ("карантин", 3.5),
            ("локдаун", 4.0),
            ("принудительн вакцинац", 5.0),
            ("обязательн вакцинац", 4.5),
            ("отстранили от работ", 4.5),
        ],
    ),
    (
        "social_harm",
        [
            ("забой скота", 5.0),
            ("уничтожен скот", 5.0),
            ("аварийн жиль", 4.5),
            ("обманутые дольщик", 5.0),
            ("коммунальн авар", 4.5),
            ("прорыв труб", 4.5),
            ("нет отоплен", 4.0),
            ("нет лекарств", 4.5),
            ("закрыли больниц", 4.5),
            ("сократили выплат", 4.0),
        ],
    ),
]

SELF_PROMO_RULES: List[Tuple[object, float]] = [
    ("побед", 3.0),
    ("уверенн", 2.0),
    ("бесспорн", 2.0),
    ("добил", 2.5),
    ("помог", 2.0),
    ("поддержк", 2.0),
    ("развит", 2.0),
    ("открыли", 2.0),
    ("запустили", 2.0),
    ("народная программа", 3.0),
    ("волонтер", 2.0),
    ("гуманитарн", 2.0),
    ("рейтинг", 2.0),
    ("большинство россиян", 3.0),
]

PARTY_PATTERNS = [
    "единая россия",
    "единоросс",
    "ер ",
    "ер.",
    "кпрф",
    "лдпр",
    "справедливая россия",
    "новые люди",
    "яблоко",
    "родина",
    "партия",
    "фракция",
]

UNITED_RUSSIA_PATTERNS = [
    "единая россия",
    "единой россии",
    "единую россию",
    "единоросс",
    "er_molnia",
    "er_ru",
]

DEPUTY_CONTEXT_PATTERNS = [
    "депутат",
    "госдум",
    "государственн дум",
    "фракц",
    "комитет гд",
    "председатель гд",
    "законопроект",
    "володин",
    "яровая",
    "хинштейн",
    "пискарев",
    "пискарёв",
    "турчак",
    "медведев",
]


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def _match(text: str, pattern: object) -> bool:
    if isinstance(pattern, re.Pattern):
        return pattern.search(text) is not None
    normalized_pattern = normalize(str(pattern))
    if re.fullmatch(r"[а-яёa-z0-9-]+", normalized_pattern) and len(normalized_pattern) <= 12:
        return re.search(
            rf"(?<![а-яёa-z0-9]){re.escape(normalized_pattern)}[а-яёa-z0-9-]*",
            text,
        ) is not None
    return normalized_pattern in text


def _score_flat_patterns(text: str, patterns: Sequence[Tuple[object, float]]) -> Tuple[float, List[str]]:
    norm = normalize(text)
    score = 0.0
    reasons: List[str] = []
    for pattern, points in patterns:
        if _match(norm, pattern):
            score += points
            reasons.append(pattern.pattern if isinstance(pattern, re.Pattern) else str(pattern))
    return score, reasons


def classify_negative_signal(text: str) -> Dict[str, object]:
    norm = normalize(text)
    categories: List[str] = []
    reasons: List[str] = []
    total = 0.0
    category_scores: Dict[str, float] = {}
    for category, patterns in NEGATIVE_SIGNAL_RULES:
        score, matched = _score_flat_patterns(norm, patterns)
        if score > 0:
            category_scores[category] = round(score, 2)
            reasons.extend(f"{category}:{item}" for item in matched)
            if score >= 3.5:
                categories.append(category)
            total += score
    return {
        "negative_score": round(total, 2),
        "negative_categories": categories,
        "negative_reasons": reasons[:20],
        "category_scores": category_scores,
    }


def classify_promo_signal(text: str) -> Dict[str, object]:
    score, reasons = _score_flat_patterns(text, SELF_PROMO_RULES)
    return {"promo_score": round(score, 2), "promo_reasons": reasons[:12]}


def _source_value(source: Optional[Mapping], key: str) -> str:
    if not source:
        return ""
    try:
        return str(source[key] or "")
    except Exception:
        get = getattr(source, "get", None)
        return str(get(key, "") if get else "")


def classify_source_context(source: Optional[Mapping], text: str = "") -> Dict[str, object]:
    source_text = normalize(
        " ".join(
            [
                _source_value(source, "name"),
                _source_value(source, "url"),
                _source_value(source, "subcategory"),
                _source_value(source, "political_alignment"),
                _source_value(source, "notes"),
            ]
        )
    )
    body_text = normalize(text)
    combined = f"{source_text} {body_text}"
    subcategory = normalize(_source_value(source, "subcategory"))
    alignment = normalize(_source_value(source, "political_alignment"))
    is_party_source = subcategory == "party" or any(pattern in source_text for pattern in PARTY_PATTERNS)
    is_united_russia = any(pattern in combined for pattern in UNITED_RUSSIA_PATTERNS)
    is_pro_government = alignment in {"pro_government", "state", "state_aligned"} or "pro_government" in source_text
    is_deputy_related = any(pattern in body_text for pattern in DEPUTY_CONTEXT_PATTERNS)
    is_strict_context = is_party_source or is_united_russia or (is_deputy_related and is_pro_government)
    return {
        "is_party_source": is_party_source,
        "is_united_russia": is_united_russia,
        "is_pro_government": is_pro_government,
        "is_deputy_related": is_deputy_related,
        "is_strict_context": is_strict_context,
        "source_name": _source_value(source, "name"),
        "source_url": _source_value(source, "url"),
        "political_alignment": _source_value(source, "political_alignment"),
        "subcategory": _source_value(source, "subcategory"),
    }


def strict_negative_threshold(source_context: Mapping[str, object]) -> float:
    threshold = BASE_NEGATIVE_THRESHOLD
    if source_context.get("is_party_source"):
        threshold = max(threshold, PARTY_NEGATIVE_THRESHOLD)
    if source_context.get("is_deputy_related"):
        threshold = max(threshold, DEPUTY_STRICT_THRESHOLD)
    if source_context.get("is_united_russia"):
        threshold = max(threshold, UNITED_RUSSIA_NEGATIVE_THRESHOLD)
    return threshold


def classify_negative_profile(
    text: str,
    source: Optional[Mapping] = None,
    tag_names: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    negative = classify_negative_signal(text)
    promo = classify_promo_signal(text)
    source_context = classify_source_context(source, text=text)
    threshold = strict_negative_threshold(source_context)
    negative_score = float(negative["negative_score"])
    is_negative_public_interest = negative_score >= threshold and bool(negative["negative_categories"])
    tag_set = {str(tag) for tag in (tag_names or [])}
    risk_tags: List[str] = []
    if is_negative_public_interest:
        risk_tags.append("filter:negative_public_interest")
    for category in negative["negative_categories"]:
        risk_tags.append(f"negative:{category}")
    if source_context["is_party_source"]:
        risk_tags.append("review:party_source_strict")
    if source_context["is_united_russia"]:
        risk_tags.append("review:united_russia_strict")
    if source_context["is_deputy_related"]:
        risk_tags.append("review:deputy_claim_strict")
    if promo["promo_score"] >= 4.0:
        risk_tags.append("promo_or_self_praise_risk")
    if "possible_corruption" in tag_set:
        risk_tags.append("review:corruption_claim")
    party_self_promo_without_negative = (
        bool(source_context["is_party_source"] or source_context["is_united_russia"])
        and float(promo["promo_score"]) >= 4.0
        and not is_negative_public_interest
    )
    return {
        **negative,
        **promo,
        "threshold": threshold,
        "is_negative_public_interest": is_negative_public_interest,
        "party_self_promo_without_negative": party_self_promo_without_negative,
        "source_context": source_context,
        "risk_tags": sorted(set(risk_tags)),
    }
