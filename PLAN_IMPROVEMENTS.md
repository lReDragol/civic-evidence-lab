# ПЛАН: Прокачка парсинга, анализатора подлинности, классификатора тегов,
# трекинга голосований и должностных лиц
# Дата: 2026-04-23
# Статус: ЧЕРНОВИК — согласовать с пользователем перед реализацией

================================================================================
БЛОК 1: СХЕМА БД — новые таблицы для законов, голосований, должностей
================================================================================

1.1. Таблица `bills` — законопроекты
--------------------------------------
CREATE TABLE IF NOT EXISTS bills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    number          TEXT NOT NULL,           -- "№ 538926-8"
    title           TEXT NOT NULL,
    bill_type       TEXT,                    -- федеральный закон / постановление / указ / КоАП / УК
    status          TEXT,                    -- внесён / 1 чтение / 2 чтение / 3 чтение / подписан / отклонён / в архиве
    registration_date TEXT,
    duma_url        TEXT,
    committee       TEXT,                    -- профильный комитет
    keywords        TEXT,                    -- JSON массив тегов-ключевых слов
    annotation      TEXT,                    -- аннотация законопроекта
    raw_data        TEXT,                    -- полный JSON ответ от API/скрейпера
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(number)
);

1.2. Таблица `bill_sponsors` — кто внёс законопроект
------------------------------------------------------
CREATE TABLE IF NOT EXISTS bill_sponsors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER NOT NULL,
    entity_id       INTEGER,                -- ссылка на entities (если нашли)
    sponsor_name    TEXT NOT NULL,           -- "Иванов И.И."
    sponsor_role    TEXT,                    -- депутат / сенатор / президент / правительство / группа депутатов
    faction         TEXT,                    -- ЕР / КПРФ / ЛДПР / СР / независимый
    is_collective   INTEGER DEFAULT 0,       -- 1 если "группа депутатов" / "Правительство РФ"
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

1.3. Таблица `bill_votes` — поимённые голосования
---------------------------------------------------
CREATE TABLE IF NOT EXISTS bill_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER NOT NULL,
    vote_date       TEXT NOT NULL,           -- дата голосования
    vote_stage      TEXT,                    -- 1_чтение / 2_чтение / 3_чтение / в_целом / поправки
    entity_id       INTEGER,                -- кто голосовал (ссылка на entities)
    deputy_name     TEXT NOT NULL,           -- ФИО (для быстрого поиска без JOIN)
    faction         TEXT,                    -- фракция на момент голосования
    vote_result     TEXT NOT NULL,           -- за / против / воздержался / не голосовал / отсутствовал
    duma_session    TEXT,                    -- "Весенняя сессия 2025"
    raw_data        TEXT,
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    UNIQUE(bill_id, vote_date, entity_id, vote_stage)
);

1.4. Таблица `official_positions` — должности чиновников (история)
--------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS official_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    position_title  TEXT NOT NULL,           -- "Губернатор", "Министр финансов", "Депутат ГД"
    organization    TEXT NOT NULL,           -- "Правительство РФ", "Госдума", "Росреестр"
    region          TEXT,                    -- регион (если применимо)
    faction         TEXT,                    -- партия (если применимо)
    started_at      TEXT,                    -- дата вступления в должность
    ended_at        TEXT,                    -- дата ухода (NULL = действующий)
    source_url      TEXT,
    source_type     TEXT,                    -- decree / election / appointment / duma_profile
    is_active       INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

1.5. Таблица `party_memberships` — история партийной принадлежности
---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_memberships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    party_name      TEXT NOT NULL,           -- "Единая Россия", "КПРФ", "Справедливая Россия"
    role            TEXT,                    -- член / руководитель фракции / председатель
    started_at      TEXT,
    ended_at        TEXT,                    -- NULL = текущее членство
    source_url      TEXT,
    is_current      INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

1.6. Таблица `investigative_materials` — следственные/журналистские материалы
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS investigative_materials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER,                -- ссылка на content_items (если есть)
    material_type   TEXT NOT NULL,           -- investigation / court_filing / search_warrant / indictment / journalistic / leak
    title           TEXT NOT NULL,
    summary         TEXT,
    involved_entities TEXT,                  -- JSON: [entity_id, ...]
    referenced_laws TEXT,                    -- JSON: ["ст.159 УК РФ", "ФЗ-115", ...]
    referenced_cases TEXT,                   -- JSON: [case_number, ...]
    publication_date TEXT,
    source_org      TEXT,                    -- "СК РФ", "Прокуратура", "Проект", "iStories"
    source_credibility TEXT,                 -- A / B / C / D
    verification_status TEXT DEFAULT 'unverified', -- verified / partially / unverified / disputed
    url             TEXT,
    raw_data        TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

1.7. Таблица `tag_explanations` — почему тег был назначен
----------------------------------------------------------
CREATE TABLE IF NOT EXISTS tag_explanations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_tag_id  INTEGER NOT NULL,
    trigger_text    TEXT NOT NULL,           -- фрагмент текста-триггера
    trigger_rule    TEXT NOT NULL,           -- имя правила / LLM reasoning
    matched_pattern TEXT,                    -- regex pattern который сработал
    confidence_raw  REAL,                    -- сырое число до нормализации
    FOREIGN KEY (content_tag_id) REFERENCES content_tags(id) ON DELETE CASCADE
);

1.8. Таблица `law_references` — ссылки на законы в контенте
-------------------------------------------------------------
CREATE TABLE IF NOT EXISTS law_references (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    law_type        TEXT NOT NULL,           -- ФЗ / УК / КоАП / постановление / указ / приказ
    law_number      TEXT,                    -- "115-ФЗ", "ст.159", "№587"
    article         TEXT,                    -- "ч.2 ст.159"
    context         TEXT,                    -- предложение где упомянуто
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

ЗАДАЧА: Написать db/migrate_v2.py с CREATE TABLE IF NOT EXISTS для всех новых таблиц + индексы.

================================================================================
БЛОК 2: ПАРСЕРЫ — новые скрейперы для законов, голосований, депутатов
================================================================================

2.1. Парсер законопроектов Думы (sozd.duma.gov.ru) — ДЕТАЛИЗАЦИЯ
-----------------------------------------------------------------
Сейчас: duma_bills_collect() делает только поиск по ключевым словам и
извлекает номер+название+статус+СПЗИ со списка. НЕ ходит внутрь карточки.

НУЖНО:
  а) После получения списка законопроектов — проходить по каждой карточке
     (https://sozd.duma.gov.ru/bill/{number}) и извлекать:
     - Полная аннотация (annotation)
     - Комитет-соисполнитель (committee)
     - Полный список соавторов/спонсоров (с разбивкой пофамильно)
     - Все этапы прохождения (1 чтение, 2 чтение, 3 чтение, подписание)
     - Даты каждого этапа
     - Ссылки на текст закона (исходный + с поправками + окончательный)
  б) Парсить спонсоров в bill_sponsors с определением фракции через
     deputy_profiles или api.duma.gov.ru
  в) Записывать в таблицу bills, а не в content_items

ФАЙЛ: collectors/duma_bills_scraper.py (новый, заменить текущий в official_scraper)

2.2. Парсер голосований Думы (api.duma.gov.ru / sozd.duma.gov.ru)
-------------------------------------------------------------------
Сейчас: НЕТ ВООБЩЕ. claim_type='vote_record' — только regex по тексту.

НУЖНО:
  а) Использовать api.duma.gov.ru (если есть токен) ИЛИ парсить страницы
     результатов голосования на sozd.duma.gov.ru
  б) Для каждого голосования по законопроекту извлекать:
     - bill_id (связка с таблицей bills)
     - Дата голосования
     - Стадия (1/2/3 чтение)
     - Пофамильный результат: за/против/воздержался/не голосовал
     - Фракция каждого голосовавшего
  в) Записывать в bill_votes
  г) При обновлении accountability_index — votes_tracked_count будет
     реально считаться

ИСТОЧНИКИ:
  - https://sozd.duma.gov.ru/bill/{number} — карточка со списком голосований
  - https://api.duma.gov.ru/api/{token}/vote.xml — нужен токен (в settings.duma_api_token)
  - Если нет API-токена — парсить HTML-страницы голосований

ФАЙЛ: collectors/duma_votes_scraper.py (новый)

2.3. Парсер профилей депутатов (duma.gov.ru/deputies/)
-------------------------------------------------------
Сейчас: deputy_profiles заполняется только через deputy_importer.py
(который парсит CSV/ручной ввод).

НУЖНО:
  а) Скрейпить https://duma.gov.ru/deputies/ — список всех депутатов
     с фракциями, округами, комитетами
  б) Для каждого — зайти в карточку и извлечь:
     - full_name, faction, region, committee, duma_id
     - date_elected, income_latest (из декларации если доступна)
     - biography_url, photo_url
     - Список законопроектов где депутат — соавтор (для перекрёстной связи)
  в) Создавать entities (entity_type='person') + deputy_profiles
  г) Создавать party_memberships (текущая фракция)
  д) Создавать official_positions (депутат ГД, с датой)

ФАЙЛ: collectors/deputy_profiles_scraper.py (новый)

2.4. Парсер Совета Федерации (council.gov.ru)
----------------------------------------------
Сейчас: перечислен в sources_seed.json, но скрейпера нет.

НУЖНО:
  а) Скрейпить https://council.gov.ru/structure/senators/ — список сенаторов
  б) Извлечь: ФИО, регион, комитет, должность, партия
  в) Записывать в entities + official_positions + party_memberships
  г) Сенаторы голосуют по законам тоже — нужна связь с bill_votes

ФАЙЛ: collectors/senators_scraper.py (новый)

2.5. Парсер указов/постановлений Президента и Правительства
-------------------------------------------------------------
Сейчас: kremlin_collect и government_collect — stubs (недоступны извне).

НУЖНО:
  а) На домашнем сервере (VPN) — использовать Playwright для:
     - kremlin.ru/acts/ — указы президента
     - government.ru/docs/ — постановления правительства
  б) Каждый документ — в bills (bill_type='указ' / 'постановление')
  в) bill_sponsors: президент/правительство как коллективный спонсор
  г) Извлекать law_references (статьи законов, на которые ссылается документ)

ФАЙЛ: collectors/executive_acts_scraper.py (новый, Playwright)

2.6. Парсер судебных дел — расширенный поиск
----------------------------------------------
Сейчас: kad_arbitr (только арбитраж) + sudrf (ненадёжный HTML).

НУЖНО:
  а) kad.arbitr.ru — улучшить: парсить карточку дела (участники, суммы,
     категории спора, решения, даты)
  б) sudrf.ru — попробовать REST API если есть, иначе Playwright
  в) НОВЫЙ: sudsrf.ru (суды общей юрисдикции) — возможен API
  г) НОВЫЙ: vsrf.ru (Верховный Суд) — есть API для судебных решений
     https://vsrf.ru/lk/practic — практика Верховного Суда
  д) Все дела — в content_items с content_type='court_record' +
     investigative_materials (для уголовных/следственных)

ФАЙЛ: collectors/court_cases_scraper.py (новый, объединить и расширить)

2.7. Парсер СК РФ / Прокуратуры (следственные материалы)
---------------------------------------------------------
Сейчас: genproc.gov.ru и sledcom.ru в sources_seed, скрейпера нет.

НУЖНО:
  а) sledcom.ru — пресс-релизы о возбуждённых уголовных делах
  б) genproc.gov.ru — пресс-центр прокуратуры
  в) Извлекать: ФИО подозреваемых, статьи УК, суммы, даты
  г) Связывать с entities (если лицо уже в БД)
  д) Записывать в investigative_materials + content_items

ФАЙЛ: collectors/investigative_scraper.py (новый)

2.8. Парсер Минюста — экстремистские материалы
------------------------------------------------
Сейчас: только иноагенты (полноценный API). Нет скрейпера для реестра
экстремистских материалов.

НУЖНО:
  а) https://minjust.gov.ru/ru/pages/reestr-ekstremistskih-materialov/
  б) Извлекать: наименование материала, дата включения, решение суда
  в) Записывать в content_items + investigative_materials

ФАЙЛ: расширить minjust в official_scraper.py

2.9. Парсер ФАС (фас.gov.ru) — антимонопольные дела
----------------------------------------------------
Сейчас: есть в sources_seed, скрейпера нет.

НУЖНО:
  а) https://fas.gov.ru/pages/press-center/news/ — пресс-релизы
  б) https://fas.gov.ru/appeals/ — решения по нарушениям
  в) Извлекать: нарушитель (ИНН/название), тип нарушения, сумма штрафа
  г) Связывать с entities (организации) + bills (если нарушение по закону)

ФАЙЛ: collectors/fas_scraper.py (новый)

2.10. Парсер Счётной Палаты (ach.gov.ru) — аудиторские заключения
-------------------------------------------------------------------
Сейчас: есть в sources_seed, скрейпера нет.

НУЖНО:
  а) https://ach.gov.ru/ru/activity/ — заключения и проверки
  б) Извлекать: объект проверки, выявленные нарушения, сумма, ответственные
  в) Связывать с entities + investigative_materials

ФАЙЛ: collectors/audit_chamber_scraper.py (новый)

================================================================================
БЛОК 3: АНАЛИЗАТОР ПОДЛИННОСТИ — улучшение верификации
================================================================================

3.1. Многофакторная модель достоверности
-----------------------------------------
Сейчас: compute_claim_confidence() — простая формула:
  source_score + doc_score + corroboration (3 слагаемых)
  Результат: confirmed / partially_confirmed / unverified / raw_signal

НУЖНО:
  а) Разложить на 7 факторов:
     1. source_credibility (0-1) — credibility_tier источника
     2. document_evidence (0-1) — есть ли подтверждающий официальный документ
     3. cross_source_corroboration (0-1) — сколько независимых источников
        подтверждают (2+ источника = 0.7, 3+ = 0.9)
     4. temporal_consistency (0-1) — даты в заявлении совпадают с датами
        в документах (дело возбуждено ДО заявления о нём = хорошо)
     5. entity_verification (0-1) — упомянутые лица/ИНН/номера дел
        подтверждены в реестрах
     6. rhetoric_analysis (0-1) — манипуляционный риск (из LLM + negative_filter)
     7. contradiction_detection (0-1) — есть ли противоположные заявления
        того же лица (по quotes + rhetoric_class)

  б) Весовая формула:
     authenticity = w1*F1 + w2*F2 + w3*F3 + w4*F4 + w5*F5 - w6*F6 - w7*F7
     где w1=0.15, w2=0.25, w3=0.20, w4=0.10, w5=0.15, w6=0.10, w7=0.05

  в) Новые статусы:
     confirmed (>=0.8), likely_true (0.6-0.8), partially_confirmed (0.4-0.6),
     unverified (0.2-0.4), likely_false (0.1-0.2), disproved (<0.1),
     manipulation (rhetoric>0.7 AND corroboration<0.2)

  г) Каждый расчёт — писать в verifications (аудит-след)

ФАЙЛ: verification/authenticity_model.py (новый)

3.2. Кросс-источниковая корроборация
-------------------------------------
Сейчас: evidence_linker ищет совпадения по entity_mentions. external_corpus
ищет во второй БД. НЕТ: проверки "сколько РАЗНЫХ источников говорят то же".

НУЖНО:
  а) Для каждого claim — найти все content_items с похожими claims
     (через FTS5 + entity overlap)
  б) Группировать по source_id — считать сколько НЕЗАВИСИМЫХ источников
  в) Если 2+ источника с РАЗНЫМИ category — corroboration += 0.3
  г) Если источники с РАЗНЫМИ political_alignment — corroboration += 0.2
     (оппозиция + гос. СМИ подтверждают одно = сильная корроборация)
  д) Если только один источник с bias — corroboration -= 0.1

ФАЙЛ: verification/cross_source_corroboration.py (новый)

3.3. Временная консистентность
-------------------------------
Сейчас: нет совсем. Заявление от 2025 про дело 2020 — не проверяется.

НУЖНО:
  а) Для claim с датой T_claim:
     - Найти все evidence с датой T_evidence
     - Если T_evidence < T_claim (документ СТАРШЕ заявления) = +0.5
     - Если T_evidence > T_claim (документ НОВЕЕ) = +0.2 (может быть реакция)
     - Если T_evidence >> T_claim (разница > 1 год) = 0 (не релевантно)
  б) Для vote_record: проверить что голосование было ДО заявления о нём
  в) Для detention: проверить что дата задержки совпадает с датой в ФССП/суде

ФАЙЛ: verification/temporal_consistency.py (новый)

3.4. Детекция противоречий
---------------------------
Сейчас: contradiction risk в risk_detector — но только по quotes rhetoric_class.

НУЖНО:
  а) Для каждого entity_id — собрать все claims и quotes
  б) Найти пары где:
     - quote A: "мы снизили налоги" + quote B: "налоги выросли" = противоречие
     - claim A: "голосовал ЗА" + bill_votes: "голосовал ПРОТИВ" = противоречие
     - claim A: "доход 500т" + deputy_profile: "доход 50м" = противоречие
  в) Использовать LLM для семантической оценки противоречивости пар
  г) Результат — в entity_relations (relation_type='contradicts') + risk_patterns

ФАЙЛ: verification/contradiction_detector.py (новый)

3.5. Запись в verifications (аудит-след)
-----------------------------------------
Сейчас: таблица verifications существует, но никто в неё не пишет.

НУЖНО:
  а) Каждый раз когда claim меняет статус — писать в verifications:
     claim_id, verifier_type, old_status, new_status, notes, evidence_added
  б) verifier_type = 'local_evidence' / 'external_registry' / 'site_search' /
     'cross_source' / 'temporal' / 'contradiction' / 'llm' / 'editor'
  в) Добавить вызовы во все существующие verification функции

ФАЙЛ: расширить verification/engine.py + все новые модули

3.6. Re-verification при поступлении новых данных
---------------------------------------------------
Сейчас: верификация одноразовая. Новые данные не триггерят повторную проверку.

НУЖНО:
  а) При добавлении нового content_item от official_registry/official_site:
     - Найти все unverified claims упоминающие те же entities
     - Запустить verify_claim_with_site_search()
  б) При добавлении bill_votes:
     - Найти все claims с claim_type='vote_record' для этого bill
     - Обновить статус если данные голосования подтверждают/опровергают
  в) Scheduler job: nightly re-verification of all "unverified" claims
     with new evidence that arrived since last verification

ФАЙЛ: verification/re_verifier.py (новый)

================================================================================
БЛОК 4: КЛАССИФИКАТОР ТЕГОВ — улучшение качества и покрытие
================================================================================

4.1. Тегирование с объяснениями (tag_explanations)
---------------------------------------------------
Сейчас: тег назначается, но НЕ записывается ЧТО его вызвало.

НУЖНО:
  а) В tagger_v2.py — при каждом срабатывании паттерна записывать:
     - trigger_text: подстрока текста которая совпала
     - trigger_rule: имя правила (название тега)
     - matched_pattern: regex pattern
     - confidence_raw: score до нормализации
  б) В tagger_granular.py — аналогично для keyword/region/deputy тегов
  в) В llm_classifier.py — записывать reasoning из LLM ответа
  г) Все — в таблицу tag_explanations

ФАЙЛ: расширить classifier/tagger_v2.py + tagger_granular.py + llm_classifier.py

4.2. Негация и контекст (отрицание утверждений)
-------------------------------------------------
Сейчас: "НЕ арестован" = тег "арест" (нет обработки отрицания).
TAGGING_STRATEGY.md признаёт это как проблему.

НУЖНО:
  а) Добавить NEGATION_PATTERNS: ["не ", "нет ", "ни ", "никогда не ",
     "отрицает", "опроверг", "нельзя", "запрещено не"]
  б) При обнаружении negation перед ключевым словом:
     - Снизить confidence тега на 50%
     - Добавить тег с суффиксом ":negated" (например "detention:negated")
     - Записать в tag_explanations что сработала негация
  в) Отдельный класс для "опровержений": claim_type='rebuttal'

ФАЙЛ: classifier/negation_handler.py (новый), интегрировать в tagger_v2

4.3. Извлечение ссылок на законы (law_references)
---------------------------------------------------
Сейчас: tagger_granular.py частично ловит "статья 159", "ФЗ-115" — но
только как теги, не как структурированные данные.

НУЖНО:
  а) Выделить в отдельный модуль-экстрактор:
     - ФЗ: /(\d+)[-\s]*ФЗ/i → law_type='ФЗ', law_number
     - УК РФ: /ст\.?\s*(\d[\d.]*)\s*(?:ч\.?\s*(\d))?\s*УК\s*РФ/i
     - КоАП: /ст\.?\s*(\d[\d.]*)\s*(?:ч\.?\s*(\d))?\s*КоАП/i
     - Постановления: /постановлен.*?№\s*(\S+)/i
     - Указы: /указ.*?№\s*(\S+)/i
     - Приказы: /приказ.*?№\s*(\S+)/i
  б) Записывать в law_references (content_item_id, law_type, law_number,
     article, context)
  в) Использовать при верификации: если claim упоминает "ст.159 УК РФ" —
     искать в БД bills/law_references другие материалы по той же статье

ФАЙЛ: classifier/law_reference_extractor.py (новый)

4.4. Улучшение LLM-классификатора
-----------------------------------
Сейчас: qwen2.5:14b с простым промптом, хрупкий JSON-парсинг, нет
выравнивания тегов с rule-based теггером.

НУЖНО:
  а) Новый промпт с:
     - Списком ВСЕХ допустимых тегов L1/L2/L3 (чтобы LLM не придумывала свои)
     - Запросом объяснения (reasoning) для каждого тега
     - Запросом извлечения law_references
     - Запросом извлечения named entities (ФИО, организации, ИНН)
     - Запросом detection манипуляционных приёмов (whataboutism, appeal to
       fear, false dichotomy, etc.)
  б) Надёжный JSON-парсинг:
     - Использовать structured output / JSON mode Ollama
     - Fallback: несколько regex для извлечения полей по одному
     - Валидация каждого поля перед записью
  в) Согласование с rule-based:
     - Если rule и LLM дают один тег — confidence = max(rule, llm)
     - Если расходятся — писать оба, confidence = rule.confidence * 0.5
     - Записывать расхождения в tag_explanations для ревью

ФАЙЛ: переработать classifier/llm_classifier.py

4.5. Депутатские теги с привязкой к entities
---------------------------------------------
Сейчас: tagger_granular.py делает substring match по фамилии из списка ~200.
Ложные срабатывания ("Иванов" = не депутат Иванов). Нет связи с entities.

НУЖНО:
  а) Вместо статического списка — читать deputy_profiles из БД
  б) Для каждой найденной фамилии — проверять контекст:
     - Есть ли слова "депутат", "Госдума", "фракция", "законопроект"?
     - Есть ли упоминание фракции рядом (ЕР, КПРФ)?
     - Является ли источник партийным каналом?
  в) Если контекст подтверждает — тег + entity_mention (entity_id из
     deputy_profiles.entity_id)
  г) Если контекст НЕ подтверждает — confidence=0.3, needs_review=1
  д) Поддержка алиасов: "Мишустин" = "Мишустин М.В." через entity_aliases

ФАЙЛ: переработать класс deputy в classifier/tagger_granular.py

4.6. Теги уровня L4: аналитические кластеры
---------------------------------------------
Сейчас: L0-L3 (гранулярный → событие → домен → риск). Нет аналитики.

НУЖНО:
  а) L4 теги — автоматически выводимые из комбинаций L0-L3:
     - "коррупционная_схема" = L1:procurement + L1:corruption + L3:possible_corruption
     - "репрессии_оппозиции" = L1:detention + L2:human_rights + L3:needs_verification + source=opposition
     - "цензура_интернет" = L1:censorship + L0:keywords:ркн + L0:keywords:блокиров
     - "аффилированность_чиновник" = L1:ownership + L1:procurement + L3:possible_conflict_of_interest
     - "вотум_доверия" = L1:vote_record + L2:duma + (bill_votes result)
  б) L4 теги записываются в content_tags с tag_level=4, tag_source='derived'

ФАЙЛ: classifier/analytical_tags.py (новый)

================================================================================
БЛОК 5: СВЯЗЫВАНИЕ ЗАКОНОВ ↔ ДЕПУТАТОВ ↔ СИТУАЦИЙ
================================================================================

5.1. Приёмник данных голосований → связь с entities
----------------------------------------------------
Когда bill_votes заполнен:
  а) Для каждой записи — найти или создать entity (person) по deputy_name
  б) Обновить party_memberships (текущая фракция = faction из голосования)
  в) Обновить official_positions (депутат ГД, если нет записи)
  г) Пересчитать accountability_index.votes_tracked_count

5.2. Связь законопроект ↔ ситуативный кейс
--------------------------------------------
Когда новый bill + bill_votes:
  а) Найти все claims упоминающие номер закона или тему
  б) Найти все content_items с law_references ссылающимися на этот ФЗ
  в) Создать case если:
     - Закон + 3+ claims о коррупции/ущербе = case_type='legislative_corruption'
     - Закон + 3+ negative claims от населения = case_type='public_opposition'
     - Закон + голосование против оппозиции = case_type='partisan_legislation'
  г) case_events: добавить даты чтений и подписания как timeline events

5.3. Карта причастности (involvement map)
------------------------------------------
Для каждого чиновника/депутата показать:
  а) Какие законы он внёс (bill_sponsors WHERE entity_id=X)
  б) Как голосовал по ключевым законам (bill_votes WHERE entity_id=X)
  в) Какие заявления делал (claims через entity_mentions)
  г) Какие кейсы с ним связаны (cases через case_claims → claims → entity_mentions)
  д) Какие риски (risk_patterns WHERE entity_ids содержит X)
  ё) Партийная история (party_memberships WHERE entity_id=X)
  ж) Должности (official_positions WHERE entity_id=X)

ФАЙЛ: cases/involvement_map.py (новый) — генерация карты для UI

5.4. Авто-популяция entity_relations
------------------------------------
Сейчас: таблица пуста, graph/ директория пуста.

НУЖНО:
  а) Извлекать отношения из структурных данных:
     - bill_sponsors: депутат → закон (relation_type='sponsored_bill')
     - bill_votes: депутат → закон (relation_type='voted_for'/'voted_against')
     - deputy_profiles: депутат → фракция (relation_type='member_of')
     - official_positions: чиновник → организация (relation_type='works_at')
     - party_memberships: лицо → партия (relation_type='party_member')
  б) Извлекать из текста (NER + LLM):
     - "Иванов и Петров создали компанию" → 'co_founded'
     - "Сидоров — подчинённый Иванова" → 'reports_to'
     - "компания X получила контракт от ведомства Y" → 'contracted_by'
  в) Все — в entity_relations с evidence_item_id

ФАЙЛ: analysis/entity_relation_builder.py (новый)

================================================================================
БЛОК 6: UI — новые вкладки и визуализации
================================================================================

6.1. Вкладка "Законы и голосования"
------------------------------------
  а) Таблица bills с фильтрами по статусу/типу/дате
  б) Карточка закона: аннотация + спонсоры + результаты голосований
  в) Визуализация: как голосовали фракции (столбчатая диаграмма)
  г) Кнопка "Найти связанные кейсы"

6.2. Вкладка "Карта причастности чиновника"
--------------------------------------------
  а) Выбор лица (entity) → полная карта:
     - Законы внесённые / поддержанные / отклонённые
     - Заявления и их верификация
     - Риск-паттерны
     - Кейсы
     - Партийная история
     - Сеть связей (entity_relations как граф)
  б) Accountability score с трендом по периодам

6.3. Вкладка "Следственные материалы"
--------------------------------------
  а) Таблица investigative_materials с фильтрами по типу/статусу/органу
  б) Карточка: связанные лица, законы, кейсы
  в) Кнопка "Верифицировать против официальных данных"

6.4. Улучшение вкладки "Кейсы"
-------------------------------
  а) Timeline (case_events) — визуализация хронологии
  б) Связанные законы/голосования
  в) Карта участников кейса

================================================================================
БЛОК 7: ОЧЕРЁДНОСТЬ РЕАЛИЗАЦИИ
================================================================================

ПРИОРИТЕТ 1 (фундамент — без этого ничего не работает):
  ☐ 1.1-1.8  Новые таблицы БД + migrate_v2.py
  ☐ 2.3      Парсер профилей депутатов (основа для всех связок)
  ☐ 2.1      Парсер законопроектов Думы — детализация карточек
  ☐ 2.2      Парсер голосований Думы
  ☐ 4.3      Извлечение law_references
  ☐ 4.1      Tag explanations

ПРИОРИТЕТ 2 (аналитика — главное增值):
  ☐ 3.1      Многофакторная модель достоверности
  ☐ 3.2      Кросс-источниковая корроборация
  ☐ 5.2      Связь законопроект ↔ ситуативный кейс
  ☐ 5.1      Приёмник данных голосований → entities
  ☐ 5.4      Авто-популяция entity_relations

ПРИОРИТЕТ 3 (улучшение качества):
  ☐ 4.2      Негация и контекст
  ☐ 4.4      Улучшение LLM-классификатора
  ☐ 4.5      Депутатские теги с привязкой к entities
  ☐ 3.3      Временная консистентность
  ☐ 3.5      Запись в verifications

ПРИОРИТЕТ 4 (расширение источников):
  ☐ 2.4      Парсер Совета Федерации
  ☐ 2.5      Парсер указов/постановлений (Playwright)
  ☐ 2.6      Расширенный парсер судебных дел
  ☐ 2.7      Парсер СК РФ / Прокуратуры
  ☐ 2.8      Парсер экстремистских материалов Минюста
  ☐ 2.9      Парсер ФАС
  ☐ 2.10     Парсер Счётной Палаты

ПРИОРИТЕТ 5 (продвинутая аналитика):
  ☐ 3.4      Детекция противоречий
  ☐ 3.6      Re-verification при новых данных
  ☐ 4.6      Аналитические теги L4
  ☐ 5.3      Карта причастности

ПРИОРИТЕТ 6 (UI):
  ☐ 6.1-6.4  Новые вкладки и визуализации

================================================================================
БЛОК 8: ЗАВИСИМОСТИ МЕЖДУ ЗАДАЧАМИ
================================================================================

Граф зависимостей:

  1.1-1.8 (схема БД)
    → 2.1 (bills нужна таблица bills)
    → 2.2 (bill_votes нужна таблица bill_votes)
    → 2.3 (deputy_profiles расширение + party_memberships + official_positions)
    → 4.3 (law_references нужна таблица)
    → 4.1 (tag_explanations нужна таблица)

  2.3 (депутаты)
    → 2.2 (нужны entity_id депутатов для bill_votes)
    → 4.5 (нужны deputy_profiles для контекстного тегирования)
    → 5.1 (нужны entities для связи голосований)

  2.1 (законопроекты детализация)
    → 2.2 (нужны bill_id для голосований)
    → 5.2 (нужны bills для связи с кейсами)

  2.2 (голосования)
    → 5.1 (голосования → entities)
    → 5.2 (голосования → кейсы)
    → 5.4 (голосования → entity_relations)

  4.3 (law_references)
    → 3.1 (law_refs → фактор достоверности)
    → 5.2 (law_refs → связь закона с ситуацией)

  3.1 (модель достоверности)
    → 3.2 (корроборация = фактор)
    → 3.3 (временная = фактор)
    → 3.5 (verifications = аудит)

================================================================================
БЛОК 9: ОЦЕНКА ТРУДОЗАТРАТ
================================================================================

| Задача                     | Строк кода | Часы | Сложность |
|----------------------------|-----------|------|-----------|
| 1.1-1.8 migrate_v2.py      | 200       | 2    | низкая    |
| 2.1 duma_bills_detail      | 300       | 4    | средняя   |
| 2.2 duma_votes_scraper     | 400       | 6    | высокая   |
| 2.3 deputy_profiles        | 350       | 4    | средняя   |
| 2.4 senators_scraper       | 250       | 3    | средняя   |
| 2.5 executive_acts         | 300       | 5    | высокая   |
| 2.6 court_cases_extended   | 350       | 5    | высокая   |
| 2.7 investigative_scraper  | 250       | 3    | средняя   |
| 2.8 minjust_extremist      | 150       | 2    | низкая    |
| 2.9 fas_scraper            | 200       | 3    | средняя   |
| 2.10 audit_chamber         | 200       | 3    | средняя   |
| 3.1 authenticity_model     | 400       | 6    | высокая   |
| 3.2 cross_source_corr      | 300       | 4    | средняя   |
| 3.3 temporal_consistency   | 200       | 3    | средняя   |
| 3.4 contradiction_detector | 350       | 5    | высокая   |
| 3.5 verifications_audit    | 100       | 2    | низкая    |
| 3.6 re_verifier            | 250       | 4    | средняя   |
| 4.1 tag_explanations       | 150       | 2    | низкая    |
| 4.2 negation_handler       | 200       | 3    | средняя   |
| 4.3 law_reference_extract  | 250       | 3    | средняя   |
| 4.4 llm_classifier_v2      | 350       | 5    | высокая   |
| 4.5 deputy_tags_entity     | 200       | 3    | средняя   |
| 4.6 analytical_tags_L4     | 300       | 4    | средняя   |
| 5.1 votes_to_entities      | 150       | 2    | низкая    |
| 5.2 bill_to_case_link      | 250       | 4    | средняя   |
| 5.3 involvement_map        | 300       | 4    | средняя   |
| 5.4 entity_relation_build  | 350       | 5    | высокая   |
| 6.1-6.4 UI tabs            | 600       | 8    | средняя   |
| ИТОГО                      | ~7000     | ~100 |           |

================================================================================
БЛОК 10: КРИТЕРИИ ГОТОВНОСТИ ДЛЯ КАЖДОГО ПРИОРИТЕТА
================================================================================

P1 готов когда:
  - Все новые таблицы созданы и проиндексированы
  - Депутаты Госдумы текущего созыва в БД (entities + deputy_profiles + party_memberships)
  - Законопроекты с карточками (bills + bill_sponsors)
  - Хотя бы одно голосование загружено (bill_votes > 0 записей)
  - law_references извлекается из нового контента
  - tag_explanations пишется для новых тегов

P2 готов когда:
  - authenticity_model считает 7-факторную оценку
  - cross_source_corroboration находит 2+ независимых источника
  - bill ↔ case связь работает
  - entity_relations populated из структурных данных

P3 готов когда:
  - "НЕ арестован" не даёт тег "detention"
  - LLM-ответы валидированы и выровнены с rule-based
  - Депутатские теги связаны с entity_id
  - verifications таблица заполняется

P4 готов когда:
  - Сенаторы в БД
  - Указы/постановления собираются через Playwright
  - Суды общей юрисдикции парсятся
  - СК/Прокуратура пресс-релизы собираются

P5 готов когда:
  - Противоречия детектируются автоматически
  - Re-verification триггерится при новых данных
  - Аналитические L4 теги кластеризуют контент

P6 готов когда:
  - Все новые вкладки работают в PySide6 UI
