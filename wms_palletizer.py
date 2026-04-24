"""
WMS Palletizer — Автоматизация формирования паллетов
Desktop GUI приложение (CustomTkinter + threading)
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, messagebox
import requests
import urllib3
import threading
import time
import json
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
import re
import platform
import subprocess
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ В ФАЙЛ 
# ══════════════════════════════════════════════════════════════

_APP_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
_LOG_FILE = os.path.join(_APP_DIR, "api_debug_log.txt")

logger = logging.getLogger("wms_palletizer")
logger.setLevel(logging.DEBUG)
_log_handler = RotatingFileHandler(
    _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(_log_handler)
logger.info("=" * 60)
logger.info("Приложение запущено. Лог-файл: %s", _LOG_FILE)

# ══════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ (загрузка из config.ini)
# ══════════════════════════════════════════════════════════════

_CONFIG_FILE = os.path.join(_APP_DIR, "config.ini")

def _load_config() -> configparser.ConfigParser:
    """Загружает конфиг из config.ini. Создаёт дефолтный, если файл отсутствует."""
    cfg = configparser.ConfigParser()
    if os.path.isfile(_CONFIG_FILE):
        cfg.read(_CONFIG_FILE, encoding="utf-8")
    else:
        cfg["server"] = {
            "production_url": "http://10.203.0.10",
            "custom_key": "6f7b513a-0a29-4511-bc84-1ea2790a8751",
        }
        cfg["network"] = {
            "max_workers": "8",
            "retry_attempts": "3",
            "retry_backoff": "1.0",
        }
        with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
            cfg.write(f)
        logger.info("Создан config.ini с настройками по умолчанию")
    return cfg

_cfg = _load_config()

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ ПРИЛОЖЕНИЯ
# ══════════════════════════════════════════════════════════════

APP_TITLE = "LES — Формирование паллетов"
APP_VERSION = "1.1"
WINDOW_SIZE_LOGIN = "460x450"
WINDOW_SIZE_MAIN = "900x700"

PRODUCTION_URL = _cfg.get("server", "production_url", fallback="http://10.203.0.10")
_CUSTOM_KEY = _cfg.get("server", "custom_key", fallback="")
MAX_WORKERS = _cfg.getint("network", "max_workers", fallback=8)
RETRY_ATTEMPTS = _cfg.getint("network", "retry_attempts", fallback=3)
RETRY_BACKOFF = _cfg.getfloat("network", "retry_backoff", fallback=1.0)

HEADERS_TEMPLATE = {
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'customkey': _CUSTOM_KEY,
    'lang': 'ru_RU',
    'Content-Type': 'application/json; charset=UTF-8',
}


def _open_file(path: str):
    """Кроссплатформенное открытие файла."""
    system = platform.system()
    if system == "Windows":
        os.startfile(path)  # type: ignore[attr-defined]
    elif system == "Darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])

# Тема
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

# Цвета
C_BG = "#F4F6F8"
C_CARD = "#FFFFFF"
C_PRIMARY = "#2563EB"
C_PRIMARY_HOVER = "#1D4ED8"
C_SUCCESS = "#16A34A"
C_DANGER = "#DC2626"
C_TEXT = "#1E293B"
C_TEXT_SEC = "#64748B"
C_BORDER = "#E2E8F0"
C_LOG_BG = "#0F172A"
C_LOG_FG = "#E2E8F0"


# ══════════════════════════════════════════════════════════════
#  BACKEND
# ══════════════════════════════════════════════════════════════

class WMSBackend:
    """Инкапсулирует все API-вызовы к WMS."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(HEADERS_TEMPLATE)
        self.token = None
        self._on_auth_expired = None  # callback для UI при 401

    # ---------- retry-обёртка ----------
    @staticmethod
    def _sanitize_log(text: str, max_len: int = 400) -> str:
        """Маскирует токены/пароли в логах."""
        sanitized = re.sub(r'(access_token|password|Authorization)["\s:=]+["\']?[\w\-\.]+', r'\1=***', text)
        return sanitized[:max_len]

    def _req(self, method: str, path: str, timeout: int = 15, **kwargs):
        url = f"{self.base_url}{path}"
        logger.debug("→ %s %s  params=%s", method, url, kwargs.get("params"))

        last_exc = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                resp = self.session.request(method, url, verify=False, timeout=timeout, **kwargs)
                logger.debug("← %s %s  body=%s", resp.status_code, url,
                             self._sanitize_log(resp.text))

                # Обработка просроченного токена
                if resp.status_code == 401:
                    logger.warning("Токен истёк или невалиден (401)")
                    if self._on_auth_expired:
                        self._on_auth_expired()
                    raise requests.exceptions.HTTPError("Сессия истекла. Войдите заново.")

                return resp
            except requests.exceptions.HTTPError:
                raise
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ReadTimeout) as e:
                last_exc = e
                if attempt < RETRY_ATTEMPTS:
                    wait = RETRY_BACKOFF * attempt
                    logger.warning("Попытка %d/%d не удалась: %s. Повтор через %.1f с.",
                                   attempt, RETRY_ATTEMPTS, type(e).__name__, wait)
                    time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    # ---------- авторизация ----------
    def login(self, login: str, password: str) -> tuple[bool, str]:
        try:
            res = self._req("POST", "/gateway/system/api/user/login",
                            json={"loginName": login, "password": password,
                                  "operateTerminal": 1, "browser": "Chrome"})
            try:
                body = res.json()
            except ValueError:
                logger.error("login: сервер вернул не JSON (status=%d)", res.status_code)
                return False, "Ошибка сервера: получен не JSON ответ"
            if res.status_code == 200 and body.get("success"):
                self.token = body["data"]["access_token"]
                self.session.headers["Authorization"] = f"Bearer {self.token}"
                logger.info("Авторизация успешна для пользователя %s", login)
                return True, "Авторизация успешна"
            logger.warning("Ошибка авторизации: %s", body.get("msg"))
            return False, body.get("msg", "Неизвестная ошибка авторизации")
        except requests.exceptions.ConnectionError:
            logger.error("Нет связи с сервером %s", self.base_url)
            return False, "Нет связи с сервером. Проверьте IP/VPN."
        except Exception as e:
            logger.exception("Исключение при логине")
            return False, f"Ошибка: {e}"

    # ---------- поиск контейнера (с проверкой статуса) ----------
    def search_container(self, container_number: str) -> tuple[bool, str, list, str]:
        try:
            res = self._req("GET", "/gateway/wms/api/tvOverseasContainerHeader/page",
                            params={"containerNumbers": container_number, "current": 1, "pageSize": 100})
            try:
                headers_list = res.json().get("data", {}).get("list", [])
            except ValueError:
                logger.error("search_container: сервер вернул не JSON (status=%d)", res.status_code)
                return False, "Ошибка сервера: получен не JSON ответ", [], ""
            if not headers_list:
                return False, "Контейнер не найден в системе.", [], ""

            header = headers_list[0]
            status = header.get("itemStatus", "")
            batch = header.get("shipmentBatch", "")

            # Защита: контейнер не принят складом
            if status == "Key_Container_Status_1":
                return False, "❌ Контейнер НЕ ПРИНЯТ складом (待收货). Сначала выполните приемку!", [], batch

            return True, "OK", headers_list, batch
        except Exception as e:
            logger.exception("Ошибка поиска контейнера %s", container_number)
            return False, str(e), [], ""

    # ---------- получить коробки (с авто-пагинацией) ----------
    def get_boxes(self, header_id: str) -> list:
        MAX_PAGES = 100
        page_size = 500
        all_items = []
        current = 1
        while current <= MAX_PAGES:
            try:
                res = self._req("GET", "/gateway/wms/api/tvOverseasContainerDetail/page",
                                params={"current": current, "pageSize": page_size,
                                        "containerHeaderId": header_id, "types": "page"})
                data = res.json().get("data", {})
            except (ValueError, AttributeError):
                logger.error("get_boxes: невалидный JSON от сервера")
                break
            items = data.get("list", [])
            all_items.extend(items)
            total = data.get("pagination", {}).get("total", len(items))
            if len(all_items) >= total or not items:
                break
            current += 1
        return all_items

    # ---------- поиск SKU для коробки ----------
    def find_sku(self, item: dict) -> str | None:
        carton_no = item.get("cartonHandNo", "")
        wooden_no = item.get("woodenHandNo", "")
        shipment_batch = item.get("shipmentBatch", "")
        customer_no = item.get("customerNo", "")
        
        target_part_no = item.get("partNo", "") 

        scan_attempts = list(dict.fromkeys([s for s in [
            carton_no,
            wooden_no,
            f"{shipment_batch}{carton_no}",
            f"{shipment_batch}{wooden_no}",
            f"{customer_no}-{shipment_batch}{carton_no}",
            f"{customer_no}-{shipment_batch}{wooden_no}",
        ] if s and s != "-"]))

        for attempt in scan_attempts:
            try:
                res = self._req("GET", "/gateway/wms/api/containerUnPack/unPackListByBoxNo",
                                params={"scanNo": attempt, "scanType": 1, "ignoreSkuIdList": ""},
                                timeout=10)
                data = res.json()
                
                if data.get("success") and data.get("data"):
                    for d in data["data"]:
                        if d.get("partNo") == target_part_no:
                            return str(d["id"])
                    return str(data["data"][0]["id"])
            except Exception:
                pass
        return None

    # ---------- формировать паллет ----------
    def create_pallet(self, box_list: list[dict]) -> tuple[bool, str]:
        try:
            res = self._req("POST", "/gateway/wms/api/containerUnPack/groupSupportConfirm",
                            json={"paramQueryList": box_list}, timeout=20)
            try:
                body = res.json()
            except ValueError:
                logger.error("create_pallet: сервер вернул не JSON (status=%d)", res.status_code)
                return False, "Ошибка сервера: получен не JSON ответ"
            if res.status_code == 200 and body.get("success"):
                logger.info("Паллет создан: %s (%d коробок)", body.get("data"), len(box_list))
                return True, str(body.get("data", ""))
            logger.warning("Ошибка создания паллета: %s", body.get("msg"))
            return False, body.get("msg", "Неизвестная ошибка")
        except Exception as e:
            logger.exception("Исключение при создании паллета")
            return False, str(e)

    # ---------- верификация (с авто-пагинацией) ----------
    def _fetch_all_rack_details(self, batch: str) -> list:
        """Получает все записи twPartOnRackDetail для партии с пагинацией."""
        MAX_PAGES = 100
        page_size = 500
        all_items = []
        current = 1
        while current <= MAX_PAGES:
            try:
                res = self._req("GET", "/gateway/wms/api/twPartOnRackDetail/page",
                                params={"saleBatch": batch, "current": current, "pageSize": page_size})
                data = res.json().get("data", {})
            except (ValueError, AttributeError):
                break
            items = data.get("list", [])
            all_items.extend(items)
            total = data.get("pagination", {}).get("total", len(items))
            if len(all_items) >= total or not items:
                break
            current += 1
        return all_items

    def verify_pallets(self, batches: set, pallet_ids: list) -> int:
        verified = 0
        for batch in batches:
            if not batch:
                continue
            try:
                items = self._fetch_all_rack_details(batch)
                for vi in items:
                    if vi.get("newPackLake") in pallet_ids:
                        verified += 1
            except Exception:
                pass
        return verified

    # ---------- поиск уже сформированных паллетов ----------
    def get_formed_pallets(self, batch: str, container_wooden_nos: set[str] | None = None) -> list[dict]:
        """
        Получает сформированные паллеты для партии.
        container_wooden_nos — множество woodenHandNo из контейнера для фильтрации.
        Если передано, паллеты фильтруются: woodenNo должен заканчиваться на один из них.
        """
        try:
            items = self._fetch_all_rack_details(batch)
            pallets: dict[str, dict] = {}
            for item in items:
                lake = item.get("newPackLake")
                if not lake or not lake.startswith("NH"):
                    continue
                # Фильтрация по woodenNo контейнера
                if container_wooden_nos:
                    wooden_no = item.get("woodenNo", "")
                    matched = any(wooden_no.endswith(wn) for wn in container_wooden_nos)
                    if not matched:
                        continue
                if lake not in pallets:
                    pallets[lake] = {"pallet_id": lake, "part_no": item.get("partNo", "—"), "count": 0}
                pallets[lake]["count"] += 1
            return list(pallets.values())
        except Exception:
            return []

    # ---------- отозвать паллеты ----------
    def revoke_pallets(self, pallet_ids: list[str]) -> list[dict]:
        results = []
        for pid in pallet_ids:
            try:
                res = self._req("POST", "/gateway/wms/api/twPartOnRackDetail/revoke",
                                json={"newPackLake": pid}, timeout=15)
                try:
                    body = res.json()
                except ValueError:
                    results.append({"pallet_id": pid, "status": "err", "msg": "Ошибка сервера: получен не JSON ответ"})
                    continue
                if res.status_code == 200 and body.get("success"):
                    results.append({"pallet_id": pid, "status": "ok", "msg": "Успешно отозван"})
                else:
                    results.append({"pallet_id": pid, "status": "err", "msg": body.get("msg", "Ошибка")})
            except Exception as e:
                results.append({"pallet_id": pid, "status": "err", "msg": str(e)})
        return results

    # ---------- генерация файла печати ----------
    def generate_print_file(self, pallet_id: str, print_type: int) -> str | None:
        try:
            res = self._req("POST", "/gateway/wms/api/twPartOnRackDetail/preview",
                            json={"newPackLakes": pallet_id, "printType": print_type})
            body = res.json()
            if res.status_code == 200 and body.get("success"):
                remote_path = body.get("data", "")
                logger.info("Файл печати сгенерирован: %s (type=%d)", remote_path, print_type)
                return remote_path
            logger.warning("Ошибка генерации печати: %s", body.get("msg"))
            return None
        except Exception:
            logger.exception("Исключение при генерации файла печати")
            return None

    # ---------- скачивание PDF ----------
    def download_pdf(self, remote_path: str, save_path: str) -> bool:
        try:
            res = self._req("GET", "/gateway/middle/api/file/download",
                            params={"fileName": "Файл_печати", "remoteFilePath": remote_path},
                            timeout=30)
            if res.status_code == 200 and len(res.content) > 0:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, "wb") as f:
                    f.write(res.content)
                logger.info("PDF сохранён: %s (%d байт)", save_path, len(res.content))
                return True
            logger.warning("Ошибка скачивания PDF: status=%d, size=%d", res.status_code, len(res.content))
            return False
        except Exception:
            logger.exception("Исключение при скачивании PDF")
            return False
            # ---------- ПОДГОТОВКА К СПИСАНИЮ ----------
    def fetch_writeoff_info(self, barcode: str) -> tuple[bool, str, list[dict]]:
        try:
            # 1. Ищем коробки по отсканированному штрихкоду
            res1 = self._req("GET", "/gateway/wms/api/rfSkuout/getSkuList", 
                             params={"scanNo": barcode, "scanType": 1, "availableQuantity": "", "trBasPartSupplId": "", "tmBasStorageNo": ""})
            data1 = res1.json().get("data", [])
            
            if not data1:
                return False, "Ничего не найдено по этому штрихкоду или коробка уже списана.", []

            results = []
            # Штрихкод может содержать несколько коробок, проверяем все
            for item in data1:
                lake = item.get("smallPackLake")
                if not lake:
                    continue
                
                # 2. Получаем технические ID для каждой коробки
                res2 = self._req("GET", "/gateway/wms/api/rfSkuout/getSkuInfoByHu", 
                                 params={"smallPackLake": lake, "availableQuantity": "", "trBasPartSupplId": "", "tmBasStorageNo": ""})
                data2 = res2.json().get("data", {})
                
                if data2:
                    results.append({
                        "smallPackLake": lake,
                        "partNo": data2.get("partNo", item.get("partNo", "—")),
                        "outQty": data2.get("outQty", 0),
                        "trBasPartSupplId": data2.get("trBasPartSupplId"),
                        "tmBasStorageNo": data2.get("tmBasStorageNo", "AL300"), # Обычно AL300 по умолчанию
                        "barcode": barcode # Сохраняем исходный штрихкод для логов
                    })
            
            if results:
                return True, "OK", results
            else:
                return False, "Не удалось получить детальную информацию по коробкам.", []
                
        except Exception as e:
            logger.exception("Ошибка при поиске данных для списания: %s", barcode)
            return False, f"Ошибка сети/API: {e}", []

    # ---------- ВЫПОЛНЕНИЕ СПИСАНИЯ ----------
    def commit_writeoff(self, payload: dict) -> tuple[bool, str]:
        try:
            res = self._req("POST", "/gateway/wms/api/rfSkuout/skuOut", json=payload)
            try:
                body = res.json()
            except ValueError:
                logger.error("commit_writeoff: сервер вернул не JSON (status=%d)", res.status_code)
                return False, "Ошибка сервера: получен не JSON ответ"
            if res.status_code == 200 and body.get("success"):
                return True, "Успешно списано"
            return False, body.get("msg", "Неизвестная ошибка сервера")
        except Exception as e:
            return False, str(e)
            # ---------- ПРОВЕРКА ЛОКАЦИИ (ПОСЛЕ СПИСАНИЯ) ----------
    def verify_location(self, barcode: str) -> tuple[bool, str, int, int]:
        """
        Проверяет, переместились ли коробки по данному штрихкоду 
        на склады AGMA300 / AZ300 / AL300.
        Возвращает: (success, message, total_boxes, verified_boxes)
        """
        try:
            MAX_PAGES = 100
            page_size = 500
            all_boxes = []
            current = 1
            total = 0
            while current <= MAX_PAGES:
                res = self._req("GET", "/gateway/wms/api/locationStock/rfStockPage", 
                                params={"woodenNo": barcode, "pageSize": page_size, "current": current})
                data = res.json().get("data", {})
                items = data.get("list", [])
                all_boxes.extend(items)
                total = data.get("pagination", {}).get("total", len(items))
                if len(all_boxes) >= total or not items:
                    break
                current += 1
            
            if not all_boxes and total == 0:
                return False, "Коробки не найдены в базе.", 0, 0
                
            verified_count = 0
            target_warehouses = ["AGMA300"]
            target_areas = ["AZ300"]
            target_locations = ["AL300"]

            for box in all_boxes:
                w_house = box.get("storageWareHouseNo", "")
                w_area = box.get("storageWareAreaNo", "")
                w_loc = box.get("storageLocationNo", "")
                
                # Проверяем, совпадает ли текущая локация коробки с нужной
                if w_house in target_warehouses and w_area in target_areas and w_loc in target_locations:
                    verified_count += 1
            
            if verified_count == total and total > 0:
                 return True, "Все коробки успешно перемещены", total, verified_count
            elif verified_count > 0:
                 return True, "Часть коробок перемещена", total, verified_count
            else:
                 return False, "Коробки не переместились на нужный склад", total, verified_count

        except Exception as e:
            logger.exception("Ошибка при проверке локации для %s", barcode)
            return False, f"Ошибка проверки: {e}", 0, 0


# ══════════════════════════════════════════════════════════════
#  GUI — ОКНО АВТОРИЗАЦИИ
# ══════════════════════════════════════════════════════════════

class LoginWindow(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(f"Авторизация — {APP_TITLE}")
        self.geometry(WINDOW_SIZE_LOGIN)
        self.resizable(False, False)
        self.configure(fg_color=C_BG)
        
        try:
            self.iconbitmap("app_icon.ico")
        except Exception:
            pass

        self._center()
        self._build_ui()

    def _center(self):
        self.update_idletasks()
        w, h = 460, 450
        x = (self.winfo_screenwidth() - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _build_ui(self):
        card = ctk.CTkFrame(self, fg_color=C_CARD, corner_radius=16, border_width=1, border_color=C_BORDER)
        card.place(relx=0.5, rely=0.5, anchor="center", relwidth=0.85, relheight=0.92)

        ctk.CTkLabel(card, text="Авторизация LES", font=("Segoe UI", 22, "bold"), text_color=C_TEXT
                     ).pack(pady=(25, 6))
        ctk.CTkLabel(card, text="Формирование паллетов", font=("Segoe UI", 13), text_color=C_TEXT_SEC
                     ).pack(pady=(0, 20))

        # Логин
        ctk.CTkLabel(card, text="Логин", font=("Segoe UI", 12), text_color=C_TEXT_SEC, anchor="w"
                     ).pack(fill="x", padx=32)
        self.login_entry = ctk.CTkEntry(card, height=38, font=("Segoe UI", 14), placeholder_text="u000000")
        self.login_entry.pack(fill="x", padx=32, pady=(2, 12))

        # Пароль
        ctk.CTkLabel(card, text="Пароль", font=("Segoe UI", 12), text_color=C_TEXT_SEC, anchor="w"
                     ).pack(fill="x", padx=32)
        self.pass_entry = ctk.CTkEntry(card, height=38, font=("Segoe UI", 14), show="●", placeholder_text="••••••")
        self.pass_entry.pack(fill="x", padx=32, pady=(2, 15))
        self.pass_entry.bind("<Return>", lambda e: self._do_login())

        # Кнопка
        self.login_btn = ctk.CTkButton(
            card, text="Войти", height=42, font=("Segoe UI", 15, "bold"),
            fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, corner_radius=10,
            command=self._do_login,
        )
        self.login_btn.pack(fill="x", padx=32, pady=(0, 10))

        # Статус
        self.status_label = ctk.CTkLabel(card, text="", font=("Segoe UI", 12), text_color=C_DANGER, wraplength=320)
        self.status_label.pack(pady=(5, 10))

    def _do_login(self):
        # Теперь берем логин из поля ввода
        login = self.login_entry.get().strip()
        password = self.pass_entry.get().strip()
        
        if not login:
            self.status_label.configure(text="Введите логин", text_color=C_DANGER)
            return

        if not password:
            self.status_label.configure(text="Введите пароль", text_color=C_DANGER)
            return

        self.login_btn.configure(state="disabled", text="Подключение...")
        self.status_label.configure(text="", text_color=C_TEXT_SEC)
        
        base_url = PRODUCTION_URL
        server_label = "Продакшн"

        def worker():
            backend = WMSBackend(base_url)
            ok, msg = backend.login(login, password)
            self.after(0, lambda: self._on_login_result(ok, msg, backend, server_label))

        threading.Thread(target=worker, daemon=True).start()

    def _on_login_result(self, ok, msg, backend, server_label):
        if ok:
            self.status_label.configure(text="✓ Успешно!", text_color=C_SUCCESS)
            self.after(400, lambda: self._open_main(backend, server_label))
        else:
            self.status_label.configure(text=msg, text_color=C_DANGER)
            self.login_btn.configure(state="normal", text="Войти")

    def _open_main(self, backend, server_label):
        self.withdraw()
        MainWindow(self, backend, server_label)


# ══════════════════════════════════════════════════════════════
#  GUI — ГЛАВНОЕ ОКНО
# ══════════════════════════════════════════════════════════════

class MainWindow(ctk.CTkToplevel):
    def __init__(self, parent, backend: WMSBackend, server_label: str):
        super().__init__(parent)
        self.backend = backend
        self.parent = parent
        self.title(APP_TITLE)
        self.geometry(WINDOW_SIZE_MAIN)
        self.minsize(800, 600)
        self.configure(fg_color=C_BG)
        
        try:
            self.iconbitmap("app_icon.ico")
        except Exception:
            pass

        self._center()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self.found_pallets: list[dict] = []
        self.formed_pallets: list[dict] = []
        self.revoke_vars: list[ctk.BooleanVar] = []
        self._unique_batches: set[str] = set()
        self._current_batch: str = ""
        self._container_wooden_nos: set[str] = set()
        self._is_busy = False
        self._wo_lock = threading.Lock()  # Защита boxes_to_writeoff

        # Callback при истечении токена
        self.backend._on_auth_expired = lambda: self.after(0, self._handle_auth_expired)

        self._build_ui(server_label)
        self.log(f"Подключено к серверу: {server_label}")
        self.log("Готово к работе. Введите номер контейнера и нажмите «Найти».")

    def _center(self):
        self.update_idletasks()
        w, h = 900, 700
        x = (self.winfo_screenwidth() - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _on_close(self):
        self.parent.destroy()

    def _handle_auth_expired(self):
        """Вызывается при получении 401 от сервера."""
        messagebox.showerror("Сессия истекла", "Токен авторизации истёк.\nПриложение будет закрыто. Войдите заново.")
        self._on_close()

    def _build_ui(self, server_label: str):
        # === ВЕРХНЯЯ ПАНЕЛЬ ===
        top = ctk.CTkFrame(self, fg_color=C_CARD, corner_radius=12, border_width=1, border_color=C_BORDER, height=60)
        top.pack(fill="x", padx=16, pady=(12, 6))
        top.pack_propagate(False)

        ctk.CTkLabel(top, text="Номер поставки:", font=("Segoe UI", 13), text_color=C_TEXT).pack(side="left", padx=(16, 8), pady=12)

        self.search_entry = ctk.CTkEntry(top, height=36, width=340, font=("Segoe UI", 14), placeholder_text="Например: C029")
        self.search_entry.pack(side="left", padx=4, pady=12)
        self.search_entry.bind("<Return>", lambda e: self._do_search())

        self.search_btn = ctk.CTkButton(top, text="Найти", width=100, height=36, font=("Segoe UI", 13, "bold"),
                                        fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, corner_radius=8,
                                        command=self._do_search)
        self.search_btn.pack(side="left", padx=8, pady=12)

        ctk.CTkLabel(top, text=f"🟢  {server_label}", font=("Segoe UI", 11), text_color=C_SUCCESS).pack(side="right", padx=16)

        # === СРЕДНЯЯ ЧАСТЬ — TABVIEW ===
        self.tabview = ctk.CTkTabview(self, fg_color=C_CARD, corner_radius=12, border_width=1, border_color=C_BORDER,
                                      segmented_button_fg_color="#94A3B8", segmented_button_unselected_color="#94A3B8", 
                                      segmented_button_selected_color=C_PRIMARY, segmented_button_selected_hover_color=C_PRIMARY_HOVER,
                                      text_color="#FFFFFF")
        self.tabview.pack(fill="both", expand=True, padx=16, pady=6)

        tab1 = self.tabview.add("📦  К формированию")
        tab2 = self.tabview.add("📋  Сформированные паллеты")
        tab3 = self.tabview.add("🗑️  Массовое списание")

        self._build_tab_form(tab1)
        self._build_tab_revoke(tab2)
        self._build_tab_writeoff(tab3)

        # === НИЖНЯЯ ПАНЕЛЬ ===
        self.bottom_bar = ctk.CTkFrame(self, fg_color=C_CARD, corner_radius=12, border_width=1, border_color=C_BORDER, height=60)
        self.bottom_bar.pack(fill="x", padx=16, pady=(6, 8))
        self.bottom_bar.pack_propagate(False)

        self.select_all_var = ctk.BooleanVar(value=False)
        self.select_all_cb = ctk.CTkCheckBox(self.bottom_bar, text="Выбрать всё", variable=self.select_all_var,
                                             font=("Segoe UI", 12), width=24, fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER,
                                             command=self._toggle_select_all)
        self.select_all_cb.pack(side="left", padx=(16, 8), pady=12)

        self.selected_label = ctk.CTkLabel(self.bottom_bar, text="Выбрано: 0 паллетов", font=("Segoe UI", 13), text_color=C_TEXT_SEC)
        self.selected_label.pack(side="left", padx=(0, 16), pady=12)

        self.revoke_btn = ctk.CTkButton(self.bottom_bar, text="Отозвать выделенные", height=38, font=("Segoe UI", 13, "bold"),
                                        fg_color=C_DANGER, hover_color="#B91C1C", corner_radius=8,
                                        command=self._do_revoke, state="disabled")

        self.form_btn = ctk.CTkButton(self.bottom_bar, text="Формировать выделенные", height=38, font=("Segoe UI", 13, "bold"),
                                      fg_color=C_SUCCESS, hover_color="#15803D", corner_radius=8,
                                      command=self._do_form_pallets, state="disabled")
        self.form_btn.pack(side="right", padx=(4, 16), pady=12)

       # === ЛОГ-ПАНЕЛЬ ===
        self.log_frame = ctk.CTkFrame(self, fg_color=C_CARD, corner_radius=12, border_width=1, border_color=C_BORDER, height=150)
        self.log_frame.pack(fill="x", padx=16, pady=(0, 10))
        self.log_frame.pack_propagate(False)

        log_header = ctk.CTkFrame(self.log_frame, fg_color="transparent", height=28)
        log_header.pack(fill="x")
        log_header.pack_propagate(False)
        ctk.CTkLabel(log_header, text="Журнал событий", font=("Segoe UI", 11, "bold"), text_color=C_TEXT_SEC).pack(side="left", padx=12, pady=4)

        self.log_text = ctk.CTkTextbox(self.log_frame, font=("Consolas", 11), fg_color=C_LOG_BG, text_color=C_LOG_FG,
                                       corner_radius=8, state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.tabview.configure(command=self._on_tab_changed)

    def _build_tab_form(self, parent):
        self.form_placeholder = ctk.CTkLabel(parent, text="Здесь появятся коробки к формированию", font=("Segoe UI", 14), text_color=C_TEXT_SEC)
        self.form_placeholder.place(relx=0.5, rely=0.45, anchor="center")

        self.form_header = ctk.CTkFrame(parent, fg_color="transparent", height=40)
        self.form_count_label = ctk.CTkLabel(self.form_header, text="", font=("Segoe UI", 13, "bold"), text_color=C_TEXT)
        self.form_count_label.pack(side="left", padx=16, pady=8)

        self.form_scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent", corner_radius=0)

    def _build_tab_revoke(self, parent):
        self.revoke_placeholder = ctk.CTkLabel(parent, text="Здесь появятся сформированные паллеты", font=("Segoe UI", 14), text_color=C_TEXT_SEC)
        self.revoke_placeholder.place(relx=0.5, rely=0.45, anchor="center")

        self.revoke_header = ctk.CTkFrame(parent, fg_color="transparent", height=40)
        self.revoke_count_label = ctk.CTkLabel(self.revoke_header, text="", font=("Segoe UI", 13, "bold"), text_color=C_TEXT)
        self.revoke_count_label.pack(side="left", padx=16, pady=8)

        self.revoke_scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent", corner_radius=0)

        # ══════════════════════════════════════════════════════════════
    #   ВКЛАДКА: МАССОВОЕ СПИСАНИЕ
    # ══════════════════════════════════════════════════════════════
    def _build_tab_writeoff(self, parent):
        self.boxes_to_writeoff = [] # Внутренний список готовых к списанию коробок

        # --- Верхняя панель со сканером ---
        scan_frame = ctk.CTkFrame(parent, fg_color="transparent")
        scan_frame.pack(fill="x", pady=(10, 5), padx=8)

        ctk.CTkLabel(scan_frame, text="Сканируйте штрихкод:", font=("Segoe UI", 13)).pack(side="left", padx=(0, 10))
        
        self.scan_entry = ctk.CTkEntry(scan_frame, height=36, width=300, font=("Segoe UI", 14), placeholder_text="Фокус здесь...")
        self.scan_entry.pack(side="left")
        self.scan_entry.bind("<Return>", self._on_box_scanned)

        self.clear_scan_btn = ctk.CTkButton(scan_frame, text="Очистить список", width=120, height=36, 
                                            fg_color=C_TEXT_SEC, hover_color="#475569", 
                                            command=self._clear_writeoff_list)
        self.clear_scan_btn.pack(side="right")

        # --- Таблица (Treeview) ---
        table_frame = ctk.CTkFrame(parent, fg_color=C_CARD, corner_radius=8, border_width=1, border_color=C_BORDER)
        table_frame.pack(fill="both", expand=True, padx=8, pady=5)

        cols = ("lake", "part", "qty", "status")
        self.wo_tree = ttk.Treeview(table_frame, columns=cols, show="headings")
        self.wo_tree.heading("lake", text="ID Коробки")
        self.wo_tree.heading("part", text="Деталь")
        self.wo_tree.heading("qty", text="Кол-во")
        self.wo_tree.heading("status", text="Статус")
        
        self.wo_tree.column("lake", width=160)
        self.wo_tree.column("part", width=120)
        self.wo_tree.column("qty", width=60, anchor="center")
        self.wo_tree.column("status", width=200)
        self.wo_tree.pack(fill="both", expand=True, padx=4, pady=4)

        # --- Нижняя панель действий ---
        btn_frame = ctk.CTkFrame(parent, fg_color="transparent")
        btn_frame.pack(fill="x", padx=8, pady=(5, 10))

        self.wo_count_label = ctk.CTkLabel(btn_frame, text="Готово к списанию: 0 коробок", font=("Segoe UI", 13, "bold"), text_color=C_TEXT)
        self.wo_count_label.pack(side="left")

        self.wo_commit_btn = ctk.CTkButton(btn_frame, text="⚠️ Подтвердить списание", height=38, font=("Segoe UI", 13, "bold"),
                                           fg_color=C_DANGER, hover_color="#B91C1C", corner_radius=8, state="disabled",
                                           command=self._do_mass_writeoff)
        self.wo_commit_btn.pack(side="right")

    def _on_box_scanned(self, event=None):
        barcode = self.scan_entry.get().strip()
        self.scan_entry.delete(0, 'end')
        
        if not barcode or self._is_busy:
            return

        # Проверяем, не сканировали ли мы этот штрихкод ранее
        if any(b.get("barcode") == barcode for b in self.boxes_to_writeoff):
            self.log(f"⚠️ Штрихкод {barcode} уже есть в списке.", "warn")
            return

        self._set_busy(True)
        self.log(f"🔎 Проверка штрихкода {barcode}...", "info")

        def worker():
            ok, msg, boxes = self.backend.fetch_writeoff_info(barcode)
            self.after(0, lambda: self._handle_scan_result(ok, msg, boxes, barcode))

        threading.Thread(target=worker, daemon=True).start()

    def _handle_scan_result(self, ok: bool, msg: str, boxes: list, barcode: str):
        self._set_busy(False)
        self.scan_entry.focus_set()

        if not ok:
            self.log(f"❌ Штрихкод {barcode}: {msg}", "err")
            return

        # Добавляем найденные коробки в список и таблицу
        with self._wo_lock:
            for box in boxes:
                lake = box["smallPackLake"]
                # Защита от дублей внутри мульти-штрихкода
                if any(b["smallPackLake"] == lake for b in self.boxes_to_writeoff):
                    continue
                    
                self.boxes_to_writeoff.append(box)
                # Вставляем строку и сохраняем её IID для будущего обновления статуса
                item_iid = self.wo_tree.insert("", "end", values=(lake, box["partNo"], box["outQty"], "Ожидает списания"))
                box["_tree_iid"] = item_iid
            
        self.log(f"✅ Штрихкод {barcode} успешно добавлен ({len(boxes)} кор.)", "ok")
        self._update_wo_ui()

    def _update_wo_ui(self):
        count = len(self.boxes_to_writeoff)
        self.wo_count_label.configure(text=f"Готово к списанию: {count} коробок")
        self.wo_commit_btn.configure(state="normal" if count > 0 else "disabled")

    def _clear_writeoff_list(self):
        self.boxes_to_writeoff.clear()
        for item in self.wo_tree.get_children():
            self.wo_tree.delete(item)
        self._update_wo_ui()
        self.scan_entry.focus_set()
        self.log("Очередь на списание очищена.", "info")

    def _do_mass_writeoff(self):
        if not self.boxes_to_writeoff: return
        
        n = len(self.boxes_to_writeoff)
        if not messagebox.askyesno("⚠️ Списание", f"Вы уверены, что хотите безвозвратно списать {n} коробок со склада?"):
            return

        self._set_busy(True)
        self.log(f"🗑️ Начинаю массовое списание ({n} шт.)...", "warn")

        def worker():
            success_count = 0
            # Копируем список, чтобы безопасно по нему итерироваться
            items_to_process = list(self.boxes_to_writeoff)
            
            # 1. Сначала выполняем само списание для каждой коробки
            for box in items_to_process:
                payload = {
                    "outQty": str(box["outQty"]),
                    "smallPackLake": box["smallPackLake"],
                    "trBasPartSupplId": box["trBasPartSupplId"],
                    "tmBasStorageNo": box["tmBasStorageNo"]
                }
                
                ok, msg = self.backend.commit_writeoff(payload)
                tree_iid = box["_tree_iid"]
                
                if ok:
                    success_count += 1
                    # Временно ставим статус "Проверка..."
                    self.after(0, lambda iid=tree_iid: self.wo_tree.set(iid, "status", "⏳ Проверка..."))
                    self.after(0, lambda l=box["smallPackLake"]: self.log(f" ✅ {l} — запрос на списание отправлен", "ok"))
                    with self._wo_lock:
                        if box in self.boxes_to_writeoff:
                            self.boxes_to_writeoff.remove(box)
                else:
                    self.after(0, lambda iid=tree_iid, m=msg: self.wo_tree.set(iid, "status", f"❌ Ошибка: {m}"))
                    self.after(0, lambda l=box["smallPackLake"], m=msg: self.log(f" ❌ {l} — ошибка: {m}", "err"))

            # 2. Теперь проверяем фактическое перемещение по уникальным штрихкодам
            self.after(0, lambda: self.log("🔎 Запускаю проверку фактического перемещения на склад AZ300...", "info"))
            
            # Собираем уникальные штрихкоды из тех коробок, которые мы только что успешно обработали
            with self._wo_lock:
                remaining_set = set(id(b) for b in self.boxes_to_writeoff)
            unique_barcodes = set(box["barcode"] for box in items_to_process if id(box) not in remaining_set)
            
            total_verified = 0
            for barcode in unique_barcodes:
                # Даем системе секунду на обработку транзакций перед проверкой
                time.sleep(1) 
                ok, msg, total_found, verified = self.backend.verify_location(barcode)
                total_verified += verified
                
                if ok and verified == total_found:
                     self.after(0, lambda b=barcode, v=verified, t=total_found: self.log(f" 🏁 Штрихкод {b}: Все {v}/{t} кор. на месте (AL300)", "ok"))
                else:
                     self.after(0, lambda b=barcode, v=verified, t=total_found: self.log(f" ⚠️ Штрихкод {b}: Переместилось только {v} из {t} кор.", "warn"))

            # Обновляем статусы в таблице на финальные
            with self._wo_lock:
                remaining_set2 = set(id(b) for b in self.boxes_to_writeoff)
            for box in items_to_process:
                 if id(box) not in remaining_set2:
                     self.after(0, lambda iid=box["_tree_iid"]: self.wo_tree.set(iid, "status", "✅ Списано и проверено"))

            self.after(0, lambda: self.log(f"Операция завершена. Успешно списано: {success_count} из {n}. Подтверждено складом: {total_verified}.", "ok" if success_count == n else "warn"))
            self.after(0, lambda: self._update_wo_ui())
            self.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def log(self, msg: str, tag: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = {"info": "ℹ", "ok": "✅", "err": "❌", "warn": "⚠️"}.get(tag, "•")
        line = f"[{ts}] {prefix}  {msg}\n"
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")
        logger.info("[%s] %s", tag.upper(), msg)

    def _do_search(self):
        query = self.search_entry.get().strip()
        if not query or self._is_busy:
            return
        self._set_busy(True)
        self.log(f"Поиск контейнера «{query}»...")

        def worker():
            ok, msg, headers, batch = self.backend.search_container(query)
            if not ok:
                self.after(0, lambda: (self.log(msg, "err"), self._set_busy(False)))
                return

            header_id = headers[0]["id"]
            self._current_batch = batch
            self.after(0, lambda: self.log(f"Контейнер найден (id={header_id}, партия={batch}). Загружаю данные...", "ok"))

            items = self.backend.get_boxes(header_id)

            # Собираем woodenHandNo для фильтрации паллетов именно этого контейнера
            container_wooden_nos = set()
            for it in items:
                wn = it.get("woodenHandNo", "")
                if wn:
                    container_wooden_nos.add(wn)
            self._container_wooden_nos = container_wooden_nos

            formed = self.backend.get_formed_pallets(batch, container_wooden_nos) if batch else []

            self.after(0, lambda: self.log(f"Коробок в накладной: {len(items)} | Уже сформировано паллетов: {len(formed)}"))

            if not items:
                self.after(0, lambda: (self.log("Коробки не найдены.", "warn"), self._populate_tabs({}, [], set(), formed), self._set_busy(False)))
                return

            total = len(items)
            grouped: dict[str, list[dict]] = {}
            missing: list[str] = []
            unique_batches: set[str] = set()
            lock = threading.Lock()

            def scan_one(idx_item: tuple[int, dict]) -> None:
                idx, item = idx_item
                part_no = item.get("partNo", "UNKNOWN")
                wooden_no = item.get("woodenHandNo", "")
                carton_no = item.get("cartonHandNo", "")
                shipment_batch = item.get("shipmentBatch", "")
                label = carton_no or wooden_no or "?"

                sku_id = self.backend.find_sku(item)

                with lock:
                    unique_batches.add(shipment_batch)
                    if sku_id:
                        grouped.setdefault(part_no, []).append({
                            "containerSkuId": sku_id, "packageCode": None,
                            "woodenHandNo": wooden_no, "cartonHandNo": carton_no,
                        })
                        self.after(0, lambda i=idx, lb=label, p=part_no: self.log(f"  [{i:02d}/{total}] Найден: {lb} (Деталь: {p})", "ok"))
                    else:
                        missing.append(label)
                        self.after(0, lambda i=idx, lb=label: self.log(f"  [{i:02d}/{total}] Ошибка связей: {lb}", "err"))

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = [pool.submit(scan_one, (idx, item)) for idx, item in enumerate(items, 1)]
                for f in as_completed(futures):
                    f.result()

            self.after(0, lambda: self._populate_tabs(grouped, missing, unique_batches, formed))

        threading.Thread(target=worker, daemon=True).start()

    def _populate_tabs(self, grouped: dict, missing: list, batches: set, formed: list):
        self._set_busy(False)
        self._unique_batches = batches
        self.select_all_var.set(False)
        self._populate_form_tab(grouped, missing)
        self._populate_revoke_tab(formed)
        self._update_selection_count()

    def _toggle_select_all(self):
        state = self.select_all_var.get()
        for p in self.found_pallets:
            p["var"].set(state)
        self._update_selection_count()

    def _populate_form_tab(self, grouped: dict, missing: list):
        self.found_pallets.clear()
        self.form_placeholder.place_forget()
        self.form_header.pack(fill="x", side="top")
        self.form_scroll.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        for w in self.form_scroll.winfo_children():
            w.destroy()

        total = len(grouped)
        total_boxes = sum(len(v) for v in grouped.values())
        self.form_count_label.configure(text=f"К формированию:  {total} деталей  •  {total_boxes} коробок")

        for part_no, box_list in grouped.items():
            var = ctk.BooleanVar(value=False)

            p_data: dict = {
                "part_no": part_no, "count": len(box_list),
                "box_list": box_list, "var": var,
            }
            self.found_pallets.append(p_data)

            # Собираем инфо по ящикам и коробкам
            wooden_groups: dict[str, list[str]] = {}
            for b in box_list:
                wn = b.get("woodenHandNo", "")
                cn = b.get("cartonHandNo", "")
                wooden_groups.setdefault(wn or "—", []).append(cn or "—")

            detail_parts = []
            for wn, cartons in wooden_groups.items():
                cartons_str = ", ".join(sorted(cartons))
                detail_parts.append(f"Ящик {wn} → {cartons_str}")
            detail_text = "  |  ".join(detail_parts)

            row = ctk.CTkFrame(self.form_scroll, fg_color="#F8FAFC", corner_radius=8, border_width=1, border_color=C_BORDER, height=64)
            row.pack(fill="x", pady=3, padx=4)
            row.pack_propagate(False)

            left_frame = ctk.CTkFrame(row, fg_color="transparent")
            left_frame.pack(side="left", fill="y", padx=(0, 4))

            ctk.CTkCheckBox(left_frame, text="", variable=var, width=24, command=self._update_selection_count,
                            fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER).pack(side="left", padx=(12, 4), pady=20)

            text_frame = ctk.CTkFrame(row, fg_color="transparent")
            text_frame.pack(side="left", fill="both", expand=True, pady=4)

            ctk.CTkLabel(text_frame, text=f"{part_no}  ({len(box_list)} кор.)", font=("Segoe UI", 13, "bold"), text_color=C_TEXT,
                         anchor="w").pack(fill="x", padx=(4, 0), pady=(4, 0))

            ctk.CTkLabel(text_frame, text=detail_text, font=("Segoe UI", 10), text_color=C_TEXT_SEC,
                         anchor="w", wraplength=500).pack(fill="x", padx=(4, 0), pady=(0, 2))

            split_label = ctk.CTkLabel(row, text="1 паллет (все шт)", font=("Segoe UI", 11), text_color=C_TEXT_SEC)
            split_label.pack(side="right", padx=(0, 12), pady=20)
            p_data["_split_label"] = split_label

            ctk.CTkButton(row, text="🔀 Разбить", width=90, height=28, font=("Segoe UI", 11), corner_radius=6,
                          fg_color="#6366F1", hover_color="#4F46E5", command=lambda _p=p_data: self._open_split_dialog(_p)
                          ).pack(side="right", padx=(0, 6), pady=20)

        if missing:
            self.log(f"⚠️  Коробки без маршрутизации ({len(missing)} шт.):", "warn")
            for m in missing:
                self.log(f"   — {m}", "warn")

    def _populate_revoke_tab(self, formed: list):
        self.formed_pallets.clear()
        self.revoke_vars.clear()
        self.revoke_placeholder.place_forget()
        self.revoke_header.pack(fill="x", side="top")
        self.revoke_scroll.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        for w in self.revoke_scroll.winfo_children():
            w.destroy()

        if not formed:
            self.revoke_count_label.configure(text="Сформированных паллетов не найдено")
            ctk.CTkLabel(self.revoke_scroll, text="Нет паллетов для отзыва", font=("Segoe UI", 13), text_color=C_TEXT_SEC).pack(pady=40)
            return

        total_boxes = sum(p["count"] for p in formed)
        self.revoke_count_label.configure(text=f"Сформировано:  {len(formed)} паллет(а)  •  {total_boxes} коробок")

        for p in formed:
            var = ctk.BooleanVar(value=False)
            self.revoke_vars.append(var)
            self.formed_pallets.append({**p, "var": var})

            row = ctk.CTkFrame(self.revoke_scroll, fg_color="#FEF2F2", corner_radius=8, border_width=1, border_color="#FECACA", height=48)
            row.pack(fill="x", pady=3, padx=4)
            row.pack_propagate(False)

            ctk.CTkLabel(row, text=f"{p['pallet_id']}  (Деталь: {p['part_no']}, {p['count']} кор.)",
                         font=("Segoe UI", 13), text_color=C_TEXT, anchor="w").pack(side="left", padx=16, pady=10)

            ctk.CTkCheckBox(row, text="", variable=var, width=24, command=self._update_selection_count,
                            fg_color=C_DANGER, hover_color="#B91C1C").pack(side="right", padx=(0, 12), pady=10)

            pid = p["pallet_id"]
            ctk.CTkButton(row, text="Бол.", width=38, height=26, font=("Segoe UI", 10), corner_radius=6,
                          fg_color="#6366F1", hover_color="#4F46E5", command=lambda _pid=pid: self._do_print(_pid, 2)
                          ).pack(side="right", padx=(0, 4), pady=10)
            ctk.CTkButton(row, text="Мал.", width=38, height=26, font=("Segoe UI", 10), corner_radius=6,
                          fg_color="#8B5CF6", hover_color="#7C3AED", command=lambda _pid=pid: self._do_print(_pid, 1)
                          ).pack(side="right", padx=(0, 4), pady=10)

    def _do_print(self, pallet_id: str, print_type: int):
        type_label = "маленькая" if print_type == 1 else "большая"
        self.log(f"🖨️  Печать ({type_label}) для паллета {pallet_id}...")

        def worker():
            remote_path = self.backend.generate_print_file(pallet_id, print_type)
            if not remote_path:
                self.after(0, lambda: self.log(f"❌ Не удалось сгенерировать файл печати для {pallet_id}", "err"))
                return
            pdf_dir = os.path.join(_APP_DIR, "PDF_Labels")
            save_path = os.path.join(pdf_dir, f"Pallet_{pallet_id}_Type_{print_type}.pdf")
            ok = self.backend.download_pdf(remote_path, save_path)
            if ok:
                self.after(0, lambda: self.log(f"✅ Этикетка сохранена: {save_path}", "ok"))
                try:
                    _open_file(save_path)
                except Exception as e:
                    self.after(0, lambda: self.log(f"⚠️  Не удалось открыть PDF: {e}", "warn"))
            else:
                self.after(0, lambda: self.log(f"❌ Ошибка скачивания PDF для {pallet_id}", "err"))

        threading.Thread(target=worker, daemon=True).start()

    def _open_split_dialog(self, p: dict):
        part_no = p["part_no"]
        total = p["count"]
        split_label: ctk.CTkLabel = p["_split_label"]

        dlg = ctk.CTkToplevel(self)
        dlg.title(f"Распределение — {part_no}")
        dlg.geometry("460x480")
        dlg.resizable(False, True)
        dlg.configure(fg_color=C_BG)
        dlg.grab_set()
        dlg.transient(self)

        dlg.update_idletasks()
        x = (dlg.winfo_screenwidth() - 460) // 2
        y = (dlg.winfo_screenheight() - 480) // 2
        dlg.geometry(f"460x480+{x}+{y}")

        ctk.CTkLabel(dlg, text=f"Деталь: {part_no}", font=("Segoe UI", 16, "bold"), text_color=C_TEXT).pack(pady=(16, 2))
        ctk.CTkLabel(dlg, text=f"Всего коробок: {total}", font=("Segoe UI", 13), text_color=C_TEXT_SEC).pack(pady=(0, 10))

        alloc_label = ctk.CTkLabel(dlg, text="Распределено: 0 из " + str(total), font=("Segoe UI", 13, "bold"), text_color=C_PRIMARY)
        alloc_label.pack(pady=(0, 6))

        scroll = ctk.CTkScrollableFrame(dlg, fg_color=C_CARD, corner_radius=10, border_width=1, border_color=C_BORDER)
        scroll.pack(fill="both", expand=True, padx=20, pady=(0, 8))

        rows: list[dict] = []

        def _recalc(*_args):
            used = 0
            for r in rows:
                try:
                    val = int(r["entry"].get())
                    if val < 0: val = 0
                except (ValueError, TypeError):
                    val = 0
                used += val
            color = C_SUCCESS if used <= total else C_DANGER
            alloc_label.configure(text=f"Распределено: {used} из {total}", text_color=color)

        def _add_row(initial_value: int = 0):
            idx = len(rows) + 1
            row_frame = ctk.CTkFrame(scroll, fg_color="#F8FAFC", corner_radius=8, border_width=1, border_color=C_BORDER, height=44)
            row_frame.pack(fill="x", pady=3, padx=4)
            row_frame.pack_propagate(False)

            entry = ctk.CTkEntry(row_frame, width=70, height=30, font=("Segoe UI", 13), justify="center", placeholder_text="шт")
            entry.pack(side="left", padx=4, pady=8)
            if initial_value > 0:
                entry.insert(0, str(initial_value))
            entry.bind("<KeyRelease>", _recalc)

            ctk.CTkLabel(row_frame, text="шт", font=("Segoe UI", 11), text_color=C_TEXT_SEC).pack(side="left", padx=(2, 8), pady=8)

            def _remove(rf=row_frame):
                for i, r in enumerate(rows):
                    if r["frame"] is rf:
                        rows.pop(i)
                        break
                rf.destroy()
                for i, r in enumerate(rows):
                    r["label"].configure(text=f"Паллет {i + 1}")
                _recalc()

            ctk.CTkButton(row_frame, text="❌", width=30, height=28, font=("Segoe UI", 12), corner_radius=6,
                          fg_color=C_DANGER, hover_color="#B91C1C", command=_remove).pack(side="right", padx=8, pady=8)

            lbl = ctk.CTkLabel(row_frame, text=f"Паллет {idx}", font=("Segoe UI", 12, "bold"), text_color=C_TEXT)
            lbl.pack(side="left", padx=(12, 8), pady=8)

            rows.append({"frame": row_frame, "entry": entry, "label": lbl})
            _recalc()

        ctk.CTkButton(dlg, text="＋  Добавить паллет", height=34, font=("Segoe UI", 13, "bold"), corner_radius=8,
                      fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, command=_add_row).pack(pady=(4, 6), padx=20, fill="x")

        btn_frame = ctk.CTkFrame(dlg, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=(0, 14))

        def _save():
            sizes = []
            used = 0
            for r in rows:
                try:
                    val = int(r["entry"].get())
                    if val <= 0: continue
                except (ValueError, TypeError):
                    continue
                used += val
                sizes.append(val)

            if used > total:
                messagebox.showwarning("Ошибка", f"Распределено {used} шт, но в наличии только {total}.\nУменьшите количество.")
                return

            if not sizes:
                p.pop("split_sizes", None)
                split_label.configure(text="1 паллет (все шт)")
            else:
                p["split_sizes"] = sizes
                sizes_str = ", ".join(str(s) for s in sizes)
                split_label.configure(text=f"Разбито: {len(sizes)} палл. ({sizes_str})")
            dlg.destroy()

        def _reset():
            p.pop("split_sizes", None)
            split_label.configure(text="1 паллет (все шт)")
            dlg.destroy()

        ctk.CTkButton(btn_frame, text="Сбросить", height=36, width=120, font=("Segoe UI", 13), corner_radius=8,
                      fg_color="#94A3B8", hover_color="#64748B", command=_reset).pack(side="left", padx=(0, 8))

        ctk.CTkButton(btn_frame, text="✅  Сохранить", height=36, font=("Segoe UI", 13, "bold"), corner_radius=8,
                      fg_color=C_SUCCESS, hover_color="#15803D", command=_save).pack(side="right", fill="x", expand=True)

        existing = p.get("split_sizes")
        if existing:
            for sz in existing: _add_row(sz)
        else:
            _add_row(total)

    def _on_tab_changed(self):
        self._update_selection_count()

    def _update_selection_count(self):
        current_tab = self.tabview.get()
        
        # 1. Если мы на вкладке "Массовое списание" — прячем нижнюю панель и уходим
        if "списание" in current_tab.lower():
            self.bottom_bar.pack_forget()
            return
        else:
            # Возвращаем панель на место, если мы на первых двух вкладках
            self.bottom_bar.pack(fill="x", padx=16, pady=(6, 8), before=self.log_frame)

        # 2. Логика для первых двух вкладок
        is_form_tab = "формированию" in current_tab

        if is_form_tab:
            n = sum(1 for p in self.found_pallets if p["var"].get())
            word = self._pallet_word(n)
            self.selected_label.configure(text=f"Выбрано к формированию: {n} {word}")
            self.form_btn.configure(state="normal" if n > 0 else "disabled")
            self.revoke_btn.configure(state="disabled")
            self.form_btn.pack(side="right", padx=(4, 16), pady=12)
            self.revoke_btn.pack_forget()
        else:
            n = sum(1 for p in self.formed_pallets if p["var"].get())
            word = self._pallet_word(n)
            self.selected_label.configure(text=f"Выбрано к отзыву: {n} {word}")
            self.revoke_btn.configure(state="normal" if n > 0 else "disabled")
            self.form_btn.configure(state="disabled")
            self.revoke_btn.pack(side="right", padx=(4, 16), pady=12)
            self.form_btn.pack_forget()

    @staticmethod
    def _pallet_word(n: int) -> str:
        if n % 10 == 1 and n % 100 != 11: return "паллет"
        if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14): return "паллета"
        return "паллетов"

    def _do_form_pallets(self):
        selected = [p for p in self.found_pallets if p["var"].get()]
        if not selected or self._is_busy: return

        desc_lines = []
        for p in selected:
            sizes = p.get("split_sizes")
            if sizes:
                sizes_str = " + ".join(str(s) for s in sizes)
                desc_lines.append(f"• {p['part_no']}:  {len(sizes)} палл. ({sizes_str}) = {sum(sizes)} кор.")
            else:
                desc_lines.append(f"• {p['part_no']}:  1 паллет ({p['count']} кор.)")

        if not messagebox.askyesno("Подтверждение", f"Сформировать паллеты?\n\n" + "\n".join(desc_lines)):
            return

        self._set_busy(True)
        self.log("Формирование паллетов...", "info")

        def worker():
            created, errors = [], []

            for p in selected:
                part = p["part_no"]
                box_list = p["box_list"]
                sizes = p.get("split_sizes")

                if not sizes:
                    chunks = [box_list]
                else:
                    total_needed = sum(sizes)
                    if total_needed > len(box_list):
                        err_msg = f"Запрошено {total_needed} шт, а в наличии только {len(box_list)}"
                        errors.append({"part_no": part, "error": err_msg})
                        self.after(0, lambda pt=part, m=err_msg: self.log(f"  ❌ {pt}: {m}", "err"))
                        continue

                    chunks = []
                    offset = 0
                    for sz in sizes:
                        chunks.append(box_list[offset : offset + sz])
                        offset += sz

                total_chunks = len(chunks)
                for ci, chunk in enumerate(chunks, 1):
                    self.after(0, lambda pt=part, idx=ci, tot=total_chunks, sz=len(chunk):
                               self.log(f"  Собираю паллет {idx}/{tot} для детали {pt} ({sz} шт.)..."))

                    ok, result = self.backend.create_pallet(chunk)
                    if ok:
                        created.append({"part_no": part, "pallet_id": result, "count": len(chunk)})
                        self.after(0, lambda pt=part, r=result, idx=ci: self.log(f"  ✅ {pt} [{idx}] → Паллет: {r}", "ok"))
                    else:
                        errors.append({"part_no": part, "error": result})
                        self.after(0, lambda pt=part, r=result, idx=ci: self.log(f"  ❌ {pt} [{idx}] → Ошибка: {r}", "err"))

            if created:
                self.after(0, lambda: self.log("Проверяю регистрацию в БД...", "info"))
                pallet_ids = [c["pallet_id"] for c in created]
                verified = self.backend.verify_pallets(self._unique_batches, pallet_ids)
                total_boxes = sum(c["count"] for c in created)
                self.after(0, lambda v=verified, t=total_boxes: self.log(f"БД подтверждает: {v} из {t} коробок привязаны.", "ok"))

            self.after(0, lambda: self._show_result_popup(created, errors))
            self.after(1500, lambda: (self._set_busy(False), self._do_search()))

        threading.Thread(target=worker, daemon=True).start()

    def _do_revoke(self):
        selected = [p for p in self.formed_pallets if p["var"].get()]
        if not selected or self._is_busy: return

        n = len(selected)
        ids = [p["pallet_id"] for p in selected]
        if not messagebox.askyesno("⚠️ Отзыв паллетов", f"Расформировать {n} {self._pallet_word(n)}?\n\n" + "\n".join(f"• {pid}" for pid in ids) + "\n\nЭто действие необратимо!"):
            return

        self._set_busy(True)
        self.log(f"Отзываю {n} паллетов...", "warn")

        def worker():
            results = self.backend.revoke_pallets(ids)
            ok_count = 0
            for r in results:
                if r["status"] == "ok":
                    ok_count += 1
                    self.after(0, lambda pid=r["pallet_id"]: self.log(f"  ✅ {pid} — отозван", "ok"))
                else:
                    self.after(0, lambda pid=r["pallet_id"], m=r["msg"]: self.log(f"  ❌ {pid} — {m}", "err"))

            self.after(0, lambda: self.log(f"Отзыв завершён: {ok_count}/{n} успешно.", "ok" if ok_count == n else "warn"))

            if self._current_batch:
                formed = self.backend.get_formed_pallets(self._current_batch, self._container_wooden_nos)
                self.after(0, lambda: self._populate_revoke_tab(formed))
                self.after(0, lambda: self._update_selection_count())

            self.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _show_result_popup(self, created: list[dict], errors: list[dict]):
        popup = ctk.CTkToplevel(self)
        popup.title("Результаты формирования")
        popup.geometry("560x440")
        popup.resizable(False, False)
        popup.configure(fg_color=C_BG)
        popup.grab_set()

        popup.update_idletasks()
        x = (popup.winfo_screenwidth() - 560) // 2
        y = (popup.winfo_screenheight() - 440) // 2
        popup.geometry(f"560x440+{x}+{y}")

        if errors:
            title_text = f"Завершено с ошибками ({len(errors)})"
            title_color = C_DANGER
        else:
            title_text = "Все паллеты успешно созданы!"
            title_color = C_SUCCESS

        ctk.CTkLabel(popup, text=title_text, font=("Segoe UI", 18, "bold"), text_color=title_color).pack(pady=(20, 12))

        if created:
            ctk.CTkLabel(popup, text=f"✅ Успешно создано: {len(created)}", font=("Segoe UI", 13, "bold"), text_color=C_SUCCESS, anchor="w").pack(fill="x", padx=24, pady=(8, 4))
            tree_frame = ctk.CTkFrame(popup, fg_color=C_CARD, corner_radius=8)
            tree_frame.pack(fill="x", padx=24, pady=(0, 10))

            cols = ("part", "pallet", "boxes")
            tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=min(len(created), 6))
            tree.heading("part", text="Деталь")
            tree.heading("pallet", text="ID Паллета")
            tree.heading("boxes", text="Коробок")
            tree.column("part", width=200)
            tree.column("pallet", width=200)
            tree.column("boxes", width=80, anchor="center")
            for c in created:
                tree.insert("", "end", values=(c["part_no"], c["pallet_id"], c["count"]))
            tree.pack(fill="x", padx=4, pady=4)

        if errors:
            ctk.CTkLabel(popup, text=f"❌ Ошибки: {len(errors)}", font=("Segoe UI", 13, "bold"), text_color=C_DANGER, anchor="w").pack(fill="x", padx=24, pady=(8, 4))
            err_frame = ctk.CTkFrame(popup, fg_color="#FEF2F2", corner_radius=8)
            err_frame.pack(fill="x", padx=24, pady=(0, 10))
            for e in errors:
                ctk.CTkLabel(err_frame, text=f"• {e['part_no']}: {e['error']}", font=("Segoe UI", 12), text_color=C_DANGER, anchor="w", wraplength=480).pack(fill="x", padx=12, pady=2)

        ctk.CTkButton(popup, text="Закрыть", width=140, height=38, font=("Segoe UI", 13),
                      fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, corner_radius=8,
                      command=popup.destroy).pack(pady=(10, 16))

    def _set_busy(self, busy: bool):
        self._is_busy = busy
        state = "disabled" if busy else "normal"
        self.search_btn.configure(state=state)
        self.search_btn.configure(text="⏳ Загрузка..." if busy else "Найти")


if __name__ == "__main__":
    app = LoginWindow()
    app.mainloop()
    app.mainloop()
