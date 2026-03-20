import html
import json
import os
import re
import unicodedata
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR))).expanduser()
STATE_FILE = Path(os.getenv("STATE_FILE", str(DATA_DIR / "state.json"))).expanduser()
SOURCES_FILE = Path(os.getenv("SOURCES_FILE", str(BASE_DIR / "sources.json"))).expanduser()
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30
MAX_POSTS_PER_RUN = min(int(os.getenv("MAX_POSTS_PER_RUN", "2")), 2)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

DIRECT_RELEVANCE_KEYWORDS = [
    "extranjeria",
    "residencia",
    "nie",
    "tie",
    "asilo",
    "proteccion internacional",
    "permiso de residencia",
    "permiso de trabajo",
    "autorizacion de residencia",
    "autorizacion de trabajo",
    "empadronamiento",
    "cita previa",
    "arraigo",
    "visado",
    "tarjeta roja",
    "refugiado",
    "refugiados",
    "acogida",
]

AID_BENEFIT_KEYWORDS = [
    "ingreso minimo vital",
    "bono social",
    "subvencion",
    "subvenciones",
    "prestacion",
    "prestaciones",
    "ayuda alquiler",
    "ayudas al alquiler",
    "ayuda vivienda",
    "ayudas vivienda",
    "ayuda al transporte",
    "beca comedor",
]

INDIRECT_RELEVANCE_KEYWORDS = {
    "transport": [
        "transporte",
        "movilidad",
        "renfe",
        "metro",
        "tram",
        "tranvia",
        "tranvia",
        "autobus",
        "autobuses",
        "cercanias",
        "cercanias",
        "abono transporte",
        "bono transporte",
        "trafico",
        "circulacion",
    ],
    "safety": [
        "policia",
        "guardia civil",
        "seguridad",
        "emergencia",
        "emergencias",
        "112",
        "evacuacion",
        "incendio",
        "cierre",
        "cierres",
        "suspension",
        "suspensiones",
        "alerta",
        "herido",
        "heridos",
    ],
    "health": [
        "sanidad",
        "salud",
        "hospital",
        "centro de salud",
        "medico",
        "medica",
        "urgencias",
        "vacuna",
        "vacunacion",
        "medicamento",
        "farmacia",
    ],
    "education": [
        "educacion",
        "colegio",
        "escuela",
        "instituto",
        "beca",
        "becas",
        "comedor escolar",
        "matricula",
    ],
    "housing": [
        "vivienda",
        "alquiler",
        "hipoteca",
        "desahucio",
        "alojamiento",
        "alquileres",
    ],
    "work": [
        "trabajo",
        "empleo",
        "laboral",
        "paro",
        "contrato",
        "salario",
    ],
    "taxes": [
        "impuesto",
        "impuestos",
        "hacienda",
        "declaracion de la renta",
        "renta",
    ],
    "laws": [
        "ley",
        "decreto",
        "reglamento",
        "normativa",
        "entra en vigor",
    ],
}

PRACTICAL_ACTION_KEYWORDS = [
    "aprueba",
    "aprobado",
    "aprobada",
    "cambia",
    "cambio",
    "cambios",
    "nuevo",
    "nueva",
    "nuevas",
    "nuevos",
    "obliga",
    "obligatorio",
    "obligatoria",
    "prorroga",
    "prorrog",
    "entra en vigor",
    "abre plazo",
    "plazo",
    "activa",
    "amplia",
    "reduce",
    "sube",
    "baja",
    "mejora",
    "colapsa",
    "colapso",
    "servicio",
    "servicios",
    "avisa",
    "alerta",
    "recomienda",
    "recomendaciones",
    "cierre",
    "cierres",
    "suspension",
    "suspensiones",
    "huelga",
    "viajes al extranjero",
]

PUBLIC_ALERT_KEYWORDS = [
    "alerta",
    "aviso",
    "avisa",
    "recomendaciones",
    "emergencia",
    "emergencias",
    "112",
    "cierre",
    "cierres",
    "suspension",
    "suspensiones",
    "interrupcion",
    "interrupciones",
    "retrasos",
    "colapso",
    "huelga",
    "aemet",
]

POLITICAL_NOISE_KEYWORDS = [
    "pp",
    "psoe",
    "vox",
    "sumar",
    "podemos",
    "senado",
    "congreso",
    "diputado",
    "diputada",
    "alcalde",
    "alcaldesa",
    "europarlamento",
    "partido",
    "cumbre",
    "moncloa",
]

CRIME_NOISE_KEYWORDS = [
    "cadaver",
    "homicidio",
    "asesinato",
    "apuñal",
    "agredir",
    "agresion",
    "amenaza",
    "amenazas",
    "emboscada",
    "detenido",
    "detenida",
    "tribunal supremo",
    "supremo",
    "juzgado",
    "juicio",
]

TITLE_BOOST_KEYWORDS = [
    "extranjeria",
    "residencia",
    "nie",
    "ayuda",
    "ayudas",
    "subvencion",
    "subvenciones",
    "vivienda",
    "alquiler",
    "sanidad",
    "transporte",
    "policia",
    "educacion",
    "trabajo",
    "impuestos",
]

LOW_VALUE_BLACKLIST = [
    "galeria",
    "galeria de imagenes",
    "album",
    "foto",
    "fotos",
    "photocall",
    "video resumen",
]

LOW_VALUE_KEYWORDS = [
    "cultural",
    "cultura",
    "concierto",
    "teatro",
    "danza",
    "zarzuela",
    "festival",
    "agenda",
    "orquesta",
    "cantata",
    "gala",
    "campeonato",
    "premio",
    "premios",
    "exposicion",
    "inauguracion",
    "opinion",
    "editorial",
    "columna",
    "videoanalisis",
    "videoanalisi",
    "podcast",
    "fallas",
    "mascleta",
    "crema",
    "ofrenda",
    "cabalgata",
    "virgen",
    "fallera",
    "falleras",
    "toros",
]

HARD_REJECT_TITLE_KEYWORDS = [
    "videoanalisis",
    "videoanalisi",
    "opinion",
    "editorial",
    "columna",
    "podcast",
    "fallas",
    "mascleta",
    "crema",
    "ofrenda",
    "toros",
    "fallera",
    "falleras",
    "festival",
    "concierto",
    "teatro",
    "gala",
]

LOW_VALUE_ALLOWLIST = [
    "emergencia",
    "emergencias",
    "policia",
    "guardia civil",
    "herido",
    "heridos",
    "sanidad",
    "hospital",
    "cierre",
    "cierres",
    "suspension",
    "ayuda",
    "subvencion",
    "vivienda",
    "alquiler",
    "trabajo",
    "impuesto",
    "ley",
    "decreto",
    "educacion",
    "colegio",
    "escuela",
    "transporte",
    "residencia",
    "extranjeria",
]

SOURCE_PRIORITY_SCORES = {
    "local": 12,
    "regional": 8,
    "national": 4,
    "ukraine": 2,
    "eu": 1,
}

ABSOLUTE_PRIORITY_KEYWORDS = [
    "documento",
    "documentos",
    "documentacion",
    "residencia",
    "nie",
    "extranjeria",
    "asilo",
    "refugiado",
    "refugiados",
    "proteccion temporal",
    "ingreso minimo vital",
    "ayuda social",
    "ayuda economica",
    "ayuda al alquiler",
    "ayudas al alquiler",
    "bono social",
    "subvencion",
    "subvenciones",
    "estatus legal",
    "situacion legal",
    "empadronamiento",
]

CRIME_KEYWORDS = [
    "tiroteo",
    "disparos",
    "policia",
    "guardia civil",
    "detenido",
    "detenidos",
    "crimen",
    "asesinato",
    "robo",
    "asalto",
    "drogas",
    "narco",
    "operacion policial",
    "incautado",
    "heridos",
    "muertos",
    "incendio",
    "explosion",
    "emergencia",
]

LOCAL_CRIME_AREAS = [
    "torrevieja",
    "orihuela",
    "guardamar",
    "alicante",
    "costa blanca",
    "vega baja",
    "pilar de la horadada",
    "santa pola",
    "elche",
]

UKRAINE_PRIORITY_KEYWORDS = [
    "ucrania",
    "ucranianos",
    "ucranianas",
    "ucraniano",
    "ucraniana",
    "refugiados",
    "proteccion temporal",
]

GEO_PRIORITY_KEYWORDS = {
    "high": ["torrevieja", "costa blanca", "alicante", "orihuela", "guardamar"],
    "medium": ["valencia", "comunidad valenciana", "castellon", "gandia", "elche"],
    "low": ["espana", "gobierno", "madrid"],
}

PRACTICAL_VALUE_KEYWORDS = [
    "transporte",
    "trafico",
    "salud",
    "sanidad",
    "policia",
    "seguridad",
    "educacion",
    "vivienda",
    "alquiler",
    "trabajo",
    "impuestos",
    "cita previa",
    "itv",
    "radar",
    "carretera",
    "pension",
    "pensiones",
    "salario minimo",
    "medicina",
    "ayuda",
    "ayudas",
    "documento",
    "documentos",
    "gasolina",
    "diesel",
    "luz",
    "electricidad",
    "butano",
    "paro",
    "sepe",
    "iva",
]

UKRAINE_KEYWORDS = [
    "ucrania",
    "ucranianos",
    "ucranianas",
    "ucraniano",
    "ucraniana",
]

EU_KEYWORDS = [
    "ue",
    "union europea",
    "bruselas",
    "comision europea",
    "comision",
    "parlamento europeo",
]

SPAIN_RELEVANCE_KEYWORDS = [
    "espana",
    "gobierno",
    "madrid",
    "torrevieja",
    "costa blanca",
    "alicante",
    "orihuela",
    "guardamar",
    "valencia",
    "comunidad valenciana",
    "castellon",
    "gandia",
    "elche",
]

UKRAINE_SPAIN_RELEVANCE_KEYWORDS = [
    "consulado",
    "consular",
    "migracion",
    "retorno",
    "frontera",
    "documento",
    "documentos",
    "residencia",
    "proteccion temporal",
    "asilo",
    "refugio",
    "ayuda",
    "subvencion",
    "trabajo",
    "vivienda",
]

EU_PRACTICAL_KEYWORDS = [
    "migracion",
    "proteccion temporal",
    "asilo",
    "refugio",
    "frontera",
    "trabajo",
    "vivienda",
    "alquiler",
    "educacion",
    "sanidad",
    "salud",
    "ayuda",
    "subvencion",
    "sanciones",
]

GENERIC_POLITICS_KEYWORDS = [
    "psoe",
    "pp",
    "vox",
    "sumar",
    "podemos",
    "senado",
    "congreso",
    "diputado",
    "diputada",
    "elecciones",
    "debate",
    "partido",
]

MIGRATION_LIFE_KEYWORDS = [
    "nie",
    "residencia",
    "extranjeria",
    "empadronamiento",
    "asilo",
    "refugio",
    "proteccion temporal",
    "trabajo",
    "vivienda",
    "alquiler",
    "sanidad",
    "impuestos",
    "educacion",
]

COMMERCIAL_PROMO_KEYWORDS = [
    "clinica",
    "hotel",
    "restaurante",
    "descubre",
    "lujo",
    "libro",
    "novela",
    "novelon",
    "escritora",
    "catalogo de rutas",
]

GENERIC_CULTURAL_KEYWORDS = [
    "cultural",
    "cultura",
    "concierto",
    "teatro",
    "festival",
    "gala",
    "agenda",
    "fiesta",
    "fiestas",
    "fallas",
    "mascleta",
    "ofrenda",
    "crema",
    "fallera",
    "falleras",
    "premio",
    "premios",
    "orquesta",
    "musica",
    "cabalgata",
]

EXHIBITION_GALLERY_KEYWORDS = [
    "exposicion",
    "galeria",
    "galeria de imagenes",
    "album",
    "foto",
    "fotos",
]

LOW_IMPACT_KEYWORDS = [
    "basket",
    "futbol",
    "futbol sala",
    "valencia cf",
    "levante ud",
    "villarreal",
    "deportivo",
    "deporte",
    "deportes",
    "jugador",
    "jugadores",
    "entrenador",
    "liga",
    "campeon",
    "campeonato",
    "toreo",
    "torero",
    "toros",
    "turismo",
    "hotel",
    "hoteles",
    "lujo",
    "playa",
    "restaurante",
    "gastronomia",
    "horoscopo",
    "loteria",
    "bonoloto",
    "euromillones",
    "primitiva",
    "once",
    "famoso",
    "famosa",
    "cantante",
    "actor",
    "actriz",
]

LOW_SIGNAL_KEYWORDS = [
    "alcalde",
    "ayuntamiento",
    "concejalia",
    "concejal",
    "junta directiva",
    "auditorio",
    "centro cultural",
    "agenda cultural",
    "show",
    "online",
    "ceramica",
    "mobiliario",
    "dependencias municipales",
    "comercio local",
    "parques y jardines",
]

CRIME_ROUTINE_KEYWORDS = [
    "detenido",
    "detenida",
    "fiscal",
    "juicio",
    "yihadista",
    "yihadistas",
    "carterista",
    "carteristas",
    "agredir",
    "agresion",
    "apalea",
    "destornillador",
    "robo",
    "robos",
    "desvalijo",
    "alunicero",
    "aluniceros",
    "ladron",
    "ladrones",
]

LISTING_SELECTORS = {
    "torrevieja": [
        "article.node--type-noticia h2 a[href*='/es/noticias/']",
        "h2 a[href*='/es/noticias/']",
    ],
    "valencia": [
        "a[href*='/-/asset_publisher/']",
        "a[href*='/cas/actualidad/']",
        "a[href*='/cas/general/-/asset_publisher/']",
    ],
    "levante": [
        "a.ft-link.ft-link--secondary[href]",
        "a[href]",
    ],
    "lasprovincias": [
        "a[href$='-nt.html']",
        "a[href]",
    ],
    "elpais": [
        "h2 a[href*='/espana/']",
        "h2 a[href*='elpais.com/espana/']",
        "a[href*='elpais.com/espana/']",
    ],
}

ARTICLE_BODY_SELECTORS = {
    "torrevieja": [
        "article.node--type-noticia .field--name-body p",
    ],
    "valencia": [
        ".asset-full-content p",
        ".journal-content-article p",
        "article p",
    ],
    "levante": [
        "article p",
        "[class*='article'] p",
        "[class*='body'] p",
    ],
    "lasprovincias": [
        "article p",
        ".voc-article-content p",
        "[class*='cuerpo'] p",
    ],
    "elpais": [
        "article p",
        ".a_c p",
        "[data-dtm-region='articulo_cuerpo'] p",
    ],
}

SOURCE_URL_PATTERNS = {
    "torrevieja": re.compile(r"https?://torrevieja\.es/es/noticias/"),
    "valencia": re.compile(r"https?://www\.valencia\.es/(?:cas/[^?#]+|.*/asset_publisher/[^?#]+)"),
    "levante": re.compile(
        r"https?://www\.levante-emv\.com/"
        r"(?!autores/|videos/|fotos/|podcast/|tags/|firmas/)"
        r"[^?#]+/\d{4}/\d{2}/\d{2}/.+\.html$"
    ),
    "lasprovincias": re.compile(
        r"https?://www\.lasprovincias\.es/"
        r"(?!opinion/|videos/|galerias/|fotos/|autor/)"
        r"[^?#]+-\d{14}-nt\.html$"
    ),
    "elpais": re.compile(r"https?://elpais\.com/espana/\d{4}-\d{2}-\d{2}/.+\.html$"),
    "rss": re.compile(r"https?://.+"),
}

LATIN_TEXT_RE = re.compile(r"[A-Za-z]")


class PipelineError(RuntimeError):
    pass


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.lower())
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"\s+", " ", ascii_text)
    return ascii_text.strip()


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def require_env() -> None:
    missing = [
        name
        for name, value in {
            "BOT_TOKEN": BOT_TOKEN,
            "CHAT_ID": CHAT_ID,
            "OPENAI_API_KEY": OPENAI_API_KEY,
        }.items()
        if not value
    ]

    if missing:
        raise PipelineError(f"Missing required environment variables: {', '.join(missing)}")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_html(session: requests.Session, url: str, extra_headers: Optional[Dict[str, str]] = None) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT, headers=extra_headers)
    response.raise_for_status()
    return response.text


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_sources() -> List[Dict[str, Any]]:
    data = load_json(SOURCES_FILE, default={"sources": []})
    sources = data.get("sources", [])

    if not sources:
        raise PipelineError("sources.json does not contain any sources")

    return [source for source in sources if source.get("enabled", True)]


def load_state() -> Dict[str, Any]:
    state = load_json(STATE_FILE, default={})
    state.setdefault("posted_urls", [])
    return state


def save_state(state: Dict[str, Any]) -> None:
    posted_urls = list(dict.fromkeys(state.get("posted_urls", [])))
    state["posted_urls"] = posted_urls[-500:]
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_json(STATE_FILE, state)


def get_source_headers(source: Dict[str, Any]) -> Optional[Dict[str, str]]:
    headers = dict(DEFAULT_HEADERS)
    headers.update(source.get("headers", {}))
    return headers


def get_source_type(source: Dict[str, Any]) -> str:
    return source.get("source_type", "generic")


def matches_source_pattern(url: str, source: Dict[str, Any]) -> bool:
    if get_source_type(source) == "rss":
        allowed_domains = source.get("allowed_domains", [])
        if not allowed_domains:
            return url.startswith("http://") or url.startswith("https://")

        hostname = (urlparse(url).hostname or "").lower()
        return any(hostname == domain or hostname.endswith(f".{domain}") for domain in allowed_domains)

    pattern = SOURCE_URL_PATTERNS.get(get_source_type(source))
    return bool(pattern and pattern.match(url))


def extract_link_title(link: Any) -> str:
    title = (link.get("title") or "").strip()
    if title:
        return title
    return link.get_text(" ", strip=True)


def build_listing_item(source: Dict[str, Any], title: str, url: str, index: int) -> Dict[str, Any]:
    return {
        "source_name": source["name"],
        "source_type": get_source_type(source),
        "source_priority": source.get("priority", "local"),
        "title": title,
        "url": url,
        "listing_index": index,
    }


def parse_rss_listing(html: str, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, Any]] = []
    seen_urls = set()

    entries = soup.find_all(["item", "entry"])
    for index, entry in enumerate(entries):
        title = ""
        title_tag = entry.find("title")
        if title_tag:
            title = title_tag.get_text(" ", strip=True)

        href = ""
        link_tag = entry.find("link")
        if link_tag:
            href = (link_tag.get("href") or link_tag.get_text(" ", strip=True)).strip()

        if not href or len(title) < 12:
            continue

        url = urljoin(source["base_url"], href)
        if url in seen_urls or not matches_source_pattern(url, source):
            continue

        seen_urls.add(url)
        items.append(build_listing_item(source, title, url, index))

    return items


def parse_listing(html: str, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    if get_source_type(source) == "rss":
        return parse_rss_listing(html, source)

    soup = BeautifulSoup(html, "html.parser")
    selectors = LISTING_SELECTORS.get(get_source_type(source), ["a[href]"])
    items: List[Dict[str, Any]] = []
    seen_urls = set()
    index = 0

    for selector in selectors:
        for link in soup.select(selector):
            href = link.get("href")
            title = extract_link_title(link)
            if not href or len(title) < 20:
                continue

            url = urljoin(source["base_url"], href)
            if url in seen_urls or not matches_source_pattern(url, source):
                continue

            seen_urls.add(url)
            items.append(build_listing_item(source, title, url, index))
            index += 1

        if items:
            break

    return items


def extract_article_details(
    session: requests.Session,
    item: Dict[str, Any],
    source: Dict[str, Any],
) -> Dict[str, Any]:
    html = fetch_html(session, item["url"], extra_headers=get_source_headers(source))
    soup = BeautifulSoup(html, "html.parser")

    description_tag = (
        soup.find("meta", attrs={"property": "og:description"})
        or soup.find("meta", attrs={"name": "description"})
    )
    og_image_tag = soup.find("meta", attrs={"property": "og:image"})
    og_title_tag = soup.find("meta", attrs={"property": "og:title"})

    paragraphs: List[str] = []
    for selector in ARTICLE_BODY_SELECTORS.get(get_source_type(source), ["article p", ".article p", "p"]):
        paragraphs = [
            paragraph.get_text(" ", strip=True)
            for paragraph in soup.select(selector)
            if paragraph.get_text(" ", strip=True)
        ]
        if paragraphs:
            break

    summary = ""
    if description_tag and description_tag.get("content"):
        summary = description_tag["content"].strip()

    if not summary and paragraphs:
        summary = " ".join(paragraphs[:2]).strip()

    og_image = None
    if og_image_tag and og_image_tag.get("content"):
        og_image = urljoin(item["url"], og_image_tag["content"].strip())

    title = item["title"]
    if og_title_tag and og_title_tag.get("content"):
        title = og_title_tag["content"].strip()

    details = dict(item)
    details["title"] = re.sub(r"\s*[|\-]\s*[^|\-]+$", "", title).strip()
    details["summary"] = summary
    details["body"] = " ".join(paragraphs[:4]).strip()
    details["image"] = og_image
    return details


def match_keywords(text: str, keywords: Iterable[str]) -> List[str]:
    matches: List[str] = []
    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        pattern = rf"(?<!\w){re.escape(normalized_keyword)}(?!\w)"
        if re.search(pattern, text):
            matches.append(normalized_keyword)
    return matches


def is_local_crime(title: str) -> bool:
    normalized_title = normalize_text(title)
    has_crime_keyword = bool(match_keywords(normalized_title, CRIME_KEYWORDS))
    has_local_area = bool(match_keywords(normalized_title, LOCAL_CRIME_AREAS))
    return has_crime_keyword and has_local_area


def compute_priority_tier(scoring: Dict[str, Any]) -> int:
    if scoring["is_local_crime"]:
        return 0
    if scoring["absolute_matches"]:
        return 1
    if scoring["ukraine_spain_matches"]:
        return 2
    if scoring["geo_tier"] == "high":
        return 3
    if scoring["geo_tier"] == "medium":
        return 4
    if scoring["eu_practical_matches"]:
        return 5
    if scoring["geo_tier"] == "low":
        return 6
    return 7


def score_item(item: Dict[str, Any]) -> Dict[str, Any]:
    haystack = normalize_text(" ".join(filter(None, [item["title"], item.get("summary", "")])))
    title_text = normalize_text(item["title"])
    crime_matches = unique_preserve_order(match_keywords(title_text, CRIME_KEYWORDS))
    local_crime_area_matches = unique_preserve_order(match_keywords(title_text, LOCAL_CRIME_AREAS))
    is_local_crime_item = is_local_crime(item["title"])
    is_non_local_crime_item = bool(crime_matches) and not is_local_crime_item

    absolute_matches = unique_preserve_order(match_keywords(haystack, ABSOLUTE_PRIORITY_KEYWORDS))
    usefulness_matches = unique_preserve_order(match_keywords(haystack, PRACTICAL_VALUE_KEYWORDS))
    cultural_matches = unique_preserve_order(match_keywords(haystack, GENERIC_CULTURAL_KEYWORDS))
    gallery_matches = unique_preserve_order(match_keywords(haystack, EXHIBITION_GALLERY_KEYWORDS))
    low_impact_matches = unique_preserve_order(match_keywords(haystack, LOW_IMPACT_KEYWORDS))
    political_matches = unique_preserve_order(match_keywords(haystack, GENERIC_POLITICS_KEYWORDS))
    commercial_matches = unique_preserve_order(match_keywords(haystack, COMMERCIAL_PROMO_KEYWORDS))
    low_signal_matches = unique_preserve_order(match_keywords(haystack, LOW_SIGNAL_KEYWORDS))
    crime_routine_matches = unique_preserve_order(match_keywords(haystack, CRIME_ROUTINE_KEYWORDS))
    ukraine_matches = unique_preserve_order(match_keywords(haystack, UKRAINE_KEYWORDS))
    eu_matches = unique_preserve_order(match_keywords(haystack, EU_KEYWORDS))
    spain_matches = unique_preserve_order(match_keywords(haystack, SPAIN_RELEVANCE_KEYWORDS))
    migration_life_matches = unique_preserve_order(match_keywords(haystack, MIGRATION_LIFE_KEYWORDS))
    ukraine_context_matches = unique_preserve_order(match_keywords(haystack, UKRAINE_SPAIN_RELEVANCE_KEYWORDS))
    eu_context_matches = unique_preserve_order(match_keywords(haystack, EU_PRACTICAL_KEYWORDS))

    geo_matches: Dict[str, List[str]] = {}
    geo_tier = "none"
    geo_score = 0
    for label, score_value in [("high", 10), ("medium", 6), ("low", 3)]:
        matches = unique_preserve_order(match_keywords(haystack, GEO_PRIORITY_KEYWORDS[label]))
        if matches:
            geo_matches[label] = matches
            geo_tier = label
            geo_score = score_value
            break

    source_priority_score = SOURCE_PRIORITY_SCORES.get(item.get("source_priority", "national"), 0)
    usefulness_score = 5 if usefulness_matches else 0

    ukraine_spain_matches = ukraine_matches if ukraine_matches and (spain_matches or ukraine_context_matches) else []
    eu_practical_matches = eu_matches if eu_matches and eu_context_matches else []
    ukraine_spain_score = 8 if ukraine_spain_matches else 0
    eu_practical_score = 6 if eu_practical_matches else 0

    absolute_allow = bool(absolute_matches)
    if ukraine_matches and not (spain_matches or ukraine_context_matches):
        absolute_allow = False
    if eu_matches and not eu_context_matches:
        absolute_allow = False

    penalty = 0
    reject_reason: Optional[str] = None

    if is_non_local_crime_item:
        reject_reason = "non-local crime"
    elif any(fragment in item["url"] for fragment in ["/opinion/", "/videos/", "/fotos/", "/podcast/", "/deportes/", "/valencia-bc/"]):
        reject_reason = "non-practical section"
    elif ukraine_matches and not ukraine_spain_matches:
        penalty -= 6
        reject_reason = "generic Ukraine news without Spain relevance"
    elif eu_matches and not eu_practical_matches:
        penalty -= 6
        reject_reason = "generic EU news without practical Spain relevance"
    elif gallery_matches:
        penalty -= 5
        reject_reason = "gallery or exhibition content"
    elif cultural_matches and not (absolute_allow or usefulness_matches or migration_life_matches):
        penalty -= 3
        reject_reason = "generic cultural filler"
    elif political_matches and not (usefulness_matches or migration_life_matches or absolute_allow):
        penalty -= 5
        reject_reason = "generic politics without practical impact"
    elif commercial_matches and not (absolute_allow or migration_life_matches):
        reject_reason = "commercial or promotional content"
    elif not is_local_crime_item and (low_impact_matches or crime_routine_matches or low_signal_matches):
        reject_reason = "low practical value"

    score = source_priority_score + geo_score + usefulness_score + ukraine_spain_score + eu_practical_score + penalty
    if is_local_crime_item:
        score = max(score, 100)

    title_is_obvious_high = (
        is_local_crime_item
        or absolute_allow
        or (geo_score >= 10 and usefulness_score > 0)
        or bool(ukraine_spain_matches)
        or bool(eu_practical_matches)
    )
    title_is_obvious_low = reject_reason is not None or score <= 0
    needs_ai = (
        not is_local_crime_item
        and not title_is_obvious_high
        and not title_is_obvious_low
        and not usefulness_matches
        and not migration_life_matches
    )

    if is_local_crime_item:
        relevance_label = "local-crime"
    elif absolute_allow:
        relevance_label = "absolute"
    elif ukraine_spain_matches:
        relevance_label = "ukraine-spain"
    elif eu_practical_matches:
        relevance_label = "eu-practical"
    elif geo_score:
        relevance_label = "geo"
    elif usefulness_matches:
        relevance_label = "practical"
    else:
        relevance_label = "borderline"

    return {
        "score": score,
        "relevance_label": relevance_label,
        "absolute_matches": absolute_matches,
        "ukraine_matches": ukraine_matches,
        "eu_matches": eu_matches,
        "ukraine_spain_matches": ukraine_spain_matches,
        "eu_practical_matches": eu_practical_matches,
        "crime_matches": crime_matches,
        "local_crime_area_matches": local_crime_area_matches,
        "is_local_crime": is_local_crime_item,
        "is_non_local_crime": is_non_local_crime_item,
        "geo_matches": geo_matches,
        "geo_tier": geo_tier,
        "geo_score": geo_score,
        "usefulness_matches": usefulness_matches,
        "source_priority_score": source_priority_score,
        "title_is_obvious_high": title_is_obvious_high,
        "title_is_obvious_low": title_is_obvious_low,
        "needs_ai": needs_ai,
        "reject_reason": reject_reason,
        "priority_tier": compute_priority_tier(
            {
                "is_local_crime": is_local_crime_item,
                "absolute_matches": absolute_matches,
                "geo_tier": geo_tier,
                "ukraine_spain_matches": ukraine_spain_matches,
                "eu_practical_matches": eu_practical_matches,
            }
        ),
        "migration_life_matches": migration_life_matches,
        "commercial_matches": commercial_matches,
    }


def should_skip_item(item: Dict[str, Any], scoring: Dict[str, Any]) -> Optional[str]:
    if any(
        fragment in item["url"]
        for fragment in ["/opinion/", "/videos/", "/fotos/", "/podcast/", "/deportes/", "/valencia-bc/"]
    ):
        return "non-practical section"

    if scoring["is_local_crime"]:
        return None

    if scoring["is_non_local_crime"]:
        return "non-local crime"

    if scoring["absolute_matches"]:
        return None

    if scoring["ukraine_matches"] and not scoring["ukraine_spain_matches"]:
        return "generic Ukraine news without Spain relevance"

    if scoring["eu_matches"] and not scoring["eu_practical_matches"]:
        return "generic EU news without practical Spain relevance"

    if scoring["reject_reason"]:
        return scoring["reject_reason"]

    if scoring["score"] < 5 and not scoring["usefulness_matches"]:
        return "score below practical threshold"

    return None


def build_generation_payload(item: Dict[str, Any]) -> str:
    payload = {
        "title": item["title"],
        "summary": item.get("summary", ""),
        "priority_score": item["score"],
        "relevance_label": item["relevance_label"],
        "absolute_priority_topics": item["absolute_matches"],
        "local_crime": item["is_local_crime"],
        "ukraine_spain_topics": item["ukraine_spain_matches"],
        "eu_practical_topics": item["eu_practical_matches"],
        "geo_priority": item["geo_matches"],
        "usefulness_topics": item["usefulness_matches"],
        "ai_impact_reason_ua": item.get("ai_reason_ua", ""),
        "source_name": item["source_name"],
        "source_priority": item["source_priority"],
    }
    return json.dumps(payload, ensure_ascii=False)


def contains_latin_text(value: str) -> bool:
    return bool(LATIN_TEXT_RE.search(value))


def evaluate_impact_for_ukrainians(client: OpenAI, title: str) -> Dict[str, Any]:
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "Оціни лише практичну користь заголовка для українців, які живуть в Іспанії. "
                    "Поверни тільки JSON: {\"score\":0,\"reason_ua\":\"\"}. "
                    "score від 0 до 10. reason_ua максимум 12 слів. "
                    "Став 5 або більше лише якщо заголовок прямо впливає на документи, статус, "
                    "житло, роботу, виплати, освіту, доступ до медицини, транспорт, безпеку "
                    "або базові витрати життя в Іспанії."
                ),
            },
            {
                "role": "user",
                "content": f"Заголовок: {title}",
            },
        ],
    )
    raw_content = response.choices[0].message.content or "{}"
    parsed = json.loads(raw_content)
    score = int(parsed.get("score", 0) or 0)
    score = max(0, min(10, score))
    reason_ua = str(parsed.get("reason_ua", "")).strip()
    return {"score": score, "reason_ua": reason_ua}


def generate_ukrainian_post(client: OpenAI, item: Dict[str, Any]) -> Dict[str, str]:
    last_error: Optional[str] = None

    for attempt in range(2):
        extra_rule = ""
        if attempt == 1:
            extra_rule = (
                "\n- ще суворіше: кожен рядок має бути дуже коротким, без міток і без пояснювальних блоків"
            )

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ти пишеш дуже короткі пости для телеграм-каналу українців в Іспанії. "
                        "Пиши тільки українською. "
                        "Текст має бути природним, простим і коротким. "
                        "Без міток, без секцій, без офіційного тону. "
                        "Не використовуй латиницю або іспанські слова в текстових полях. "
                        "Поверни тільки JSON."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "Створи максимально лаконічний телеграм-допис.\n"
                        "Вимоги:\n"
                        "- Поверни JSON з полями: title, line1, line2\n"
                        "- title: 1 короткий заголовок українською\n"
                        "- line1: 1 коротке речення, що сталося\n"
                        "- line2: 1 коротке речення, практичний вплив\n"
                        "- максимум 8-12 слів у кожному рядку\n"
                        "- максимум 3 рядки до посилання\n"
                        "- без міток, без 'Чому', без вступів, без пояснювальних блоків\n"
                        "- жодних іспанських слів, жодної латиниці, жодної рекламності\n"
                        "- для локальних небезпечних або кримінальних подій пиши ще коротше, спокійно і фактологічно\n"
                        "- якщо це критична подія, не пиши емодзі тут, його додасть шаблон\n"
                        "- практичний вплив має бути продовженням, а не окремим блоком"
                        f"{extra_rule}\n\n"
                        f"Дані новини:\n{build_generation_payload(item)}"
                    ),
                },
            ],
        )

        raw_content = response.choices[0].message.content or "{}"
        parsed = json.loads(raw_content)

        title = parsed.get("title", "").strip()
        line1 = parsed.get("line1", "").strip()
        line2 = parsed.get("line2", "").strip()

        if not title or not line1 or not line2:
            last_error = f"OpenAI returned incomplete content for {item['url']}"
            continue

        if any("чому" in normalize_text(value) for value in [title, line1, line2]):
            last_error = f"OpenAI returned labeled content for {item['url']}"
            continue

        if any(len(value.split()) > 12 for value in [title, line1, line2]):
            last_error = f"OpenAI returned overly long lines for {item['url']}"
            continue

        if any(contains_latin_text(value) for value in [title, line1, line2]):
            last_error = f"OpenAI returned latin text for {item['url']}"
            continue

        return {
            "title": title,
            "line1": line1,
            "line2": line2,
            "is_local_crime": item.get("is_local_crime", False),
        }

    raise PipelineError(last_error or f"OpenAI failed to generate content for {item['url']}")


def format_source_label(source_name: str, source_url: str) -> str:
    if source_name.strip():
        return source_name.strip()

    hostname = urlparse(source_url).hostname or source_url
    return hostname.replace("www.", "")


def format_post(generated: Dict[str, str], source_name: str, source_url: str) -> str:
    prefix = "🚨 " if generated.get("is_local_crime") else ""
    title = html.escape(f"{prefix}{generated['title']}")
    line1 = html.escape(generated["line1"])
    line2 = html.escape(generated["line2"])
    source_label = html.escape(format_source_label(source_name, source_url))
    source_link = html.escape(source_url, quote=True)

    return (
        f"<b>{title}</b>\n\n"
        f"{line1}\n"
        f"{line2}\n\n"
        f"<i>Джерело:</i> <a href=\"{source_link}\">{source_label}</a>"
    )


def send_to_telegram(session: requests.Session, text: str, image_url: Optional[str]) -> Dict[str, Any]:
    endpoint = "sendPhoto" if image_url else "sendMessage"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}"

    data: Dict[str, Any] = {
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if image_url:
        data["photo"] = image_url
        data["caption"] = text
    else:
        data["text"] = text

    response = session.post(url, data=data, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not payload.get("ok"):
        raise PipelineError(f"Telegram API error: {payload}")

    return payload


def collect_candidates(session: requests.Session, posted_urls: set) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    client = OpenAI(api_key=OPENAI_API_KEY)

    for source in load_sources():
        try:
            listing_html = fetch_html(session, source["url"], extra_headers=get_source_headers(source))
        except Exception as exc:
            print(f"Skipping source {source['name']} due to listing error: {exc}")
            continue

        listed_items = parse_listing(listing_html, source)
        if not listed_items:
            print(f"No article links found for source {source['name']}")
            continue

        for listed_item in listed_items:
            if listed_item["url"] in posted_urls:
                continue

            title_scoring = score_item(listed_item)
            title_skip_reason = should_skip_item(listed_item, title_scoring)
            if title_skip_reason:
                print(f"Skipping {listed_item['url']} ({title_skip_reason})")
                continue

            if title_scoring["needs_ai"]:
                try:
                    ai_impact = evaluate_impact_for_ukrainians(client, listed_item["title"])
                except Exception as exc:
                    print(f"Skipping {listed_item['url']} due to AI evaluation error: {exc}")
                    continue

                if ai_impact["score"] < 5:
                    print(f"Skipping {listed_item['url']} (AI impact {ai_impact['score']}/10: {ai_impact['reason_ua']})")
                    continue

                listed_item["ai_reason_ua"] = ai_impact["reason_ua"]
                title_scoring["score"] += ai_impact["score"]
                title_scoring["relevance_label"] = "ai-approved"

            try:
                detailed_item = extract_article_details(session, listed_item, source)
            except Exception as exc:
                print(f"Skipping {listed_item['url']} due to extraction error: {exc}")
                continue

            scoring = score_item(detailed_item)
            if listed_item.get("ai_reason_ua"):
                detailed_item["ai_reason_ua"] = listed_item["ai_reason_ua"]
                scoring["score"] += title_scoring["score"] - score_item(listed_item)["score"]
                scoring["relevance_label"] = "ai-approved"
            skip_reason = should_skip_item(detailed_item, scoring)
            if skip_reason:
                print(f"Skipping {detailed_item['url']} ({skip_reason})")
                continue

            detailed_item.update(scoring)
            candidates.append(detailed_item)
            print(
                "Candidate accepted:",
                detailed_item["title"],
                f"| relevance={detailed_item['relevance_label']}",
                f"| score={detailed_item['score']}",
                f"| source={detailed_item['source_name']}",
            )

    candidates.sort(
        key=lambda item: (
            item["priority_tier"],
            -int(bool(item["absolute_matches"])),
            -item["score"],
            -item["source_priority_score"],
            item["listing_index"],
        )
    )
    return candidates


def main() -> None:
    require_env()

    state = load_state()
    posted_urls = set(state.get("posted_urls", []))
    session = build_session()
    client = OpenAI(api_key=OPENAI_API_KEY)

    candidates = collect_candidates(session, posted_urls)
    print(f"Found {len(candidates)} candidate items")

    sent_count = 0

    for item in candidates:
        if sent_count >= MAX_POSTS_PER_RUN:
            break

        try:
            generated = generate_ukrainian_post(client, item)
            text = format_post(generated, item["source_name"], item["url"])
            print("\n--- Telegram preview ---")
            print(text)
            print("--- End preview ---\n")
            send_to_telegram(session, text, item.get("image"))
        except Exception as exc:
            print(f"Failed to process {item['url']}: {exc}")
            continue

        posted_urls.add(item["url"])
        state["posted_urls"] = list(posted_urls)
        save_state(state)
        sent_count += 1
        print(f"Posted: {item['url']}")

    if sent_count == 0:
        print("No new posts were sent")
    else:
        print(f"Sent {sent_count} post(s)")

    save_state(state)
    print("READY")


if __name__ == "__main__":
    main()
