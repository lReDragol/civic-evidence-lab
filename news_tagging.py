import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Sequence, Tuple


TAG_RULES: List[Tuple[str, List[Tuple[str, float]], float]] = [
    (
        "Война / Украина",
        [
            ("украин", 4.0),
            ("всу", 4.0),
            ("обстрел", 4.0),
            ("ракет", 3.5),
            ("дрон", 3.5),
            ("бпла", 3.5),
            ("фронт", 3.5),
            ("минобороны", 2.5),
            ("харьков", 3.0),
            ("сумы", 3.0),
            ("донец", 3.0),
            ("луган", 3.0),
            ("крым", 2.5),
            ("запорож", 2.5),
            ("белгород", 2.5),
            ("курск", 2.5),
            ("удар", 2.0),
            ("армия", 2.0),
            ("пво", 2.0),
        ],
        4.0,
    ),
    (
        "Выборы / кампании",
        [
            ("выбор", 4.0),
            ("кандидат", 2.0),
            ("кандидат в президенты", 5.0),
            ("кандидата в президенты", 5.0),
            ("кандидате в президенты", 5.0),
            ("бюллетен", 3.5),
            ("голосован", 3.5),
            ("избир", 3.5),
            ("агитац", 3.0),
            ("кампан", 2.0),
            ("президентск", 3.0),
            ("выдвижен", 3.0),
            ("подпись за", 3.0),
            ("подписей", 3.0),
            ("сбор подпис", 4.0),
            ("точек сборов", 3.0),
            ("инициативной группы", 4.0),
            ("избирательного счета", 4.0),
            ("президентской гонке", 4.0),
            ("re:\\bцик\\b", 5.0),
        ],
        3.0,
    ),
    (
        "Оппозиция / Навальный",
        [
            ("навальн", 5.0),
            ("фбк", 5.0),
            ("певчих", 4.0),
            ("волков", 3.0),
            ("популярн политика", 4.0),
            ("политзаключ", 3.0),
        ],
        4.0,
    ),
    (
        "Репрессии / силовики",
        [
            ("арест", 4.0),
            ("задерж", 4.0),
            ("обыск", 4.0),
            ("розыск", 3.5),
            ("силовик", 3.0),
            ("колони", 3.0),
            ("сизо", 3.0),
            ("тюр", 3.0),
            ("мвд", 2.5),
            ("фсб", 3.0),
            ("пытк", 3.0),
        ],
        3.0,
    ),
    (
        "Суды / уголовные дела",
        [
            ("суд", 4.0),
            ("иск", 3.0),
            ("приговор", 4.0),
            ("уголовн", 4.0),
            ("обвин", 3.0),
            ("прокуратур", 3.0),
            ("адвокат", 2.0),
            ("дело", 1.5),
        ],
        3.0,
    ),
    (
        "Законы / регулирование",
        [
            ("закон", 4.0),
            ("законопроект", 4.0),
            ("регулир", 3.0),
            ("штраф", 3.0),
            ("фас", 3.0),
            ("требован", 3.0),
            ("обязал", 3.0),
            ("обязаны", 2.0),
            ("норма", 2.0),
            ("правила", 2.0),
            ("госдум", 2.0),
            ("иноагент", 3.0),
            ("маркировк", 3.0),
            ("минюст", 3.0),
            ("юстици", 2.5),
        ],
        3.0,
    ),
    (
        "Цензура / блокировки",
        [
            ("заблок", 5.0),
            ("блокиров", 5.0),
            ("замедл", 4.0),
            ("роскомнадзор", 5.0),
            ("re:\\bркн\\b", 5.0),
            ("огранич", 1.5),
            ("запрет", 2.0),
            ("цензур", 4.0),
        ],
        4.0,
    ),
    (
        "Санкции",
        [
            ("санкц", 5.0),
            ("эмбарго", 4.0),
        ],
        4.0,
    ),
    (
        "Экономика / цены",
        [
            ("инфляц", 4.0),
            ("подорож", 4.0),
            ("цена", 3.0),
            ("тариф", 3.0),
            ("рубл", 2.0),
            ("доллар", 2.0),
            ("евро", 2.0),
            ("экономик", 3.0),
            ("налог", 3.0),
            ("бюджет", 2.0),
            ("стоимост", 2.0),
        ],
        3.0,
    ),
    (
        "Рынки / акции",
        [
            ("котиров", 4.0),
            ("бирж", 4.0),
            ("обвал", 4.0),
            ("индекс", 3.5),
            ("торги", 3.0),
            ("капитализац", 3.0),
            ("фьючерс", 3.0),
            ("nasdaq", 4.0),
            ("s&p", 4.0),
            ("moex", 4.0),
        ],
        4.0,
    ),
    (
        "Банки / платежи",
        [
            ("банк", 4.0),
            ("swift", 5.0),
            ("свифт", 5.0),
            ("карта", 3.0),
            ("visa", 3.0),
            ("mastercard", 3.0),
            ("платеж", 4.0),
            ("перевод", 3.0),
            ("счет", 2.0),
            ("комисси", 2.0),
            ("тиньк", 4.0),
            ("сбер", 4.0),
            ("центробанк", 4.0),
            ("re:\\bцб\\b", 3.0),
        ],
        4.0,
    ),
    (
        "Крипта",
        [
            ("крипт", 5.0),
            ("биткоин", 5.0),
            ("re:\\bbtc\\b", 5.0),
            ("ethereum", 4.0),
            ("эфириум", 4.0),
            ("re:\\beth\\b", 4.0),
            ("токен", 3.0),
            ("майнинг", 4.0),
            ("notcoin", 5.0),
            ("блокчейн", 4.0),
        ],
        4.0,
    ),
    (
        "Мобилизация / призыв",
        [
            ("мобилиз", 5.0),
            ("мобилизац", 5.0),
            ("призыв", 4.0),
            ("повестк", 4.0),
            ("военкомат", 4.0),
            ("срочн", 3.0),
            ("контрактник", 3.0),
            ("в армию", 3.0),
        ],
        4.0,
    ),
    (
        "Реклама / монетизация",
        [
            ("реклам", 5.0),
            ("монетизац", 5.0),
            ("таргет", 4.0),
            ("донат", 3.0),
            ("спонсор", 3.0),
            ("рекламодатель", 4.0),
        ],
        4.0,
    ),
    (
        "Telegram",
        [
            ("telegram", 5.0),
            ("телеграм", 5.0),
            ("telegram desktop", 5.0),
        ],
        4.0,
    ),
    (
        "YouTube / видео",
        [
            ("youtube", 5.0),
            ("ютуб", 5.0),
        ],
        4.0,
    ),
    (
        "Twitch / стриминг",
        [
            ("twitch", 5.0),
            ("твич", 5.0),
            ("стрим", 3.0),
            ("стример", 3.0),
        ],
        4.0,
    ),
    (
        "Discord",
        [
            ("discord", 5.0),
            ("дискорд", 5.0),
        ],
        4.0,
    ),
    (
        "WhatsApp / Meta",
        [
            ("whatsapp", 5.0),
            ("ватсап", 5.0),
            ("meta", 4.0),
            ("instagram", 4.0),
            ("инстаграм", 4.0),
            ("facebook", 4.0),
            ("фейсбук", 4.0),
        ],
        4.0,
    ),
    (
        "Технологии / безопасность",
        [
            ("взлом", 5.0),
            ("уязвим", 4.0),
            ("кибератак", 4.0),
            ("ddos", 4.0),
            ("вирус", 3.0),
            ("хакер", 4.0),
            ("эксплойт", 4.0),
            ("malware", 4.0),
            ("обнова", 2.0),
            ("апдейт", 2.0),
            ("безопасност", 2.0),
        ],
        4.0,
    ),
    (
        "Происшествия / катастрофы",
        [
            ("пожар", 5.0),
            ("взрыв", 5.0),
            ("самолет", 4.0),
            ("крушен", 4.0),
            ("авари", 4.0),
            ("теракт", 5.0),
            ("погиб", 4.0),
            ("умер", 4.0),
            ("убит", 4.0),
            ("затоп", 3.0),
            ("землетряс", 4.0),
            ("отоплен", 3.0),
            ("горячей воды", 3.0),
            ("прорвало трубы", 4.0),
            ("коммунальн", 3.0),
        ],
        3.0,
    ),
    (
        "Протесты / митинги",
        [
            ("митинг", 5.0),
            ("протест", 5.0),
            ("демонстрац", 4.0),
            ("шестви", 3.0),
            ("полдень против", 5.0),
            ("массовые задерж", 4.0),
            ("акция протеста", 4.0),
            ("акции памяти", 4.0),
            ("антивоенн", 3.0),
        ],
        3.0,
    ),
    (
        "Российская политика",
        [
            ("путин", 4.0),
            ("кремл", 4.0),
            ("правительств", 3.0),
            ("госдум", 3.0),
            ("совфед", 3.0),
            ("кадыров", 4.0),
            ("собянин", 3.0),
            ("депутат", 2.0),
            ("губернатор", 2.0),
            ("re:\\bмэр\\b", 2.0),
        ],
        4.0,
    ),
    (
        "Международная политика",
        [
            ("лукашенко", 4.0),
            ("переговор", 3.0),
            ("саммит", 3.0),
            ("посольств", 3.0),
            ("нато", 4.0),
            ("евросоюз", 4.0),
            ("еврокомис", 3.0),
            ("израил", 4.0),
            ("герман", 3.0),
            ("китай", 3.0),
            ("франц", 3.0),
            ("малави", 3.0),
            ("re:\\bсша\\b", 3.0),
            ("re:\\bес\\b", 3.0),
        ],
        4.0,
    ),
]

LOW_SIGNAL_PATTERNS = [
    r"^новость в изображении",
    r"^(ссылка на оригинальный пост:\s*)?https?://\S+$",
]

LOW_SIGNAL_EXACT = {
    "позорище",
    "с новым годом",
    "с новым годом!",
}


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def compose_message_text(headline: str, text: str) -> str:
    head = (headline or "").strip()
    body = (text or "").strip()
    if head and body and normalize(head) == normalize(body):
        return head
    parts = [part for part in [head, body] if part]
    return "\n".join(parts).strip()


def split_tags(raw: str) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    for part in re.split(r"[;,|]", raw):
        tag = part.strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def is_low_signal_text(text: str) -> bool:
    norm = normalize(text)
    if not norm:
        return True
    if norm in LOW_SIGNAL_EXACT:
        return True
    if any(re.match(pattern, norm) for pattern in LOW_SIGNAL_PATTERNS):
        return True
    if len(norm) <= 20 and len(norm.split()) <= 3 and not any(ch.isdigit() for ch in norm):
        return True
    return False


def _pattern_hit(text: str, pattern: str) -> bool:
    if pattern.startswith("re:"):
        return re.search(pattern[3:], text) is not None
    return pattern in text


def _score_text(text: str, weight: float) -> Dict[str, float]:
    norm = f" {normalize(text)} "
    scores: Dict[str, float] = defaultdict(float)
    if not norm.strip():
        return scores

    for tag, patterns, threshold in TAG_RULES:
        matched = False
        tag_score = 0.0
        for pattern, points in patterns:
            if _pattern_hit(norm, pattern):
                matched = True
                tag_score += points
        if matched and tag_score >= threshold:
            scores[tag] += tag_score * weight
    return scores


def infer_tags(primary_text: str, extra_text: str = "", max_tags: int = 4) -> List[str]:
    scores = _score_text(primary_text, 1.0)
    primary_low_signal = is_low_signal_text(primary_text)
    if extra_text and (primary_low_signal or not scores):
        extra_weight = 0.75 if primary_low_signal else 0.5
        extra_scores = _score_text(extra_text, extra_weight)
        for tag, value in extra_scores.items():
            scores[tag] += value

    ordered = [tag for tag, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0])) if scores[tag] >= 3.0]
    norm_full = f" {normalize(f'{primary_text} {extra_text}')} "

    if "Рынки / акции" in ordered and "Экономика / цены" in ordered:
        if ordered.index("Рынки / акции") < ordered.index("Экономика / цены"):
            pass

    if "Цензура / блокировки" in ordered and ("Санкции" in ordered or "Банки / платежи" in ordered):
        internet_markers = [
            "роскомнадзор",
            " ркн ",
            "telegram",
            "телеграм",
            "youtube",
            "ютуб",
            "discord",
            "дискорд",
            "twitch",
            "твич",
            "whatsapp",
            "ватсап",
            "meta",
            "instagram",
            "facebook",
            "roblox",
            "google",
            "vk ",
            "сми",
            "вещан",
            "интернет",
            "контент",
            "мессенджер",
            "платформ",
        ]
        if not any(marker in norm_full for marker in internet_markers):
            ordered = [tag for tag in ordered if tag != "Цензура / блокировки"]

    if not ordered:
        norm = normalize(primary_text)
        if primary_low_signal and any(re.match(pattern, norm) for pattern in LOW_SIGNAL_PATTERNS):
            return ["Новость на изображении"]
        return ["Прочее"]
    return ordered[:max_tags]


def _parse_time(raw: str):
    try:
        return datetime.strptime(raw or "", "%H:%M:%S")
    except Exception:
        return None


def _neighbor_context(rows: Sequence[dict], idx: int) -> str:
    current = rows[idx]
    current_time = _parse_time(current.get("time", ""))
    context_parts: List[str] = []

    for offset in [1, -1, 2, -2, 3, -3]:
        pos = idx + offset
        if pos < 0 or pos >= len(rows):
            continue
        other = rows[pos]
        other_text = compose_message_text(other.get("headline", ""), other.get("text", ""))
        if is_low_signal_text(other_text):
            continue

        other_time = _parse_time(other.get("time", ""))
        if current_time and other_time:
            diff = abs((current_time - other_time).total_seconds())
            if diff > 3600:
                continue
        context_parts.append(other_text)
        if len(context_parts) >= 3:
            break
    return "\n".join(context_parts)


def retag_rows(rows: Sequence[dict], max_tags: int = 4) -> List[Tuple[int, str]]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.get("date", "")].append(row)

    updates: List[Tuple[int, str]] = []
    for _, day_rows in grouped.items():
        day_rows.sort(key=lambda item: (item.get("time", ""), item.get("message_id", ""), item.get("id", 0)))
        for idx, row in enumerate(day_rows):
            primary_text = compose_message_text(row.get("headline", ""), row.get("text", ""))
            extra_text = _neighbor_context(day_rows, idx)
            tags = infer_tags(primary_text, extra_text=extra_text, max_tags=max_tags)
            updates.append((row["id"], ", ".join(tags)))
    return updates
