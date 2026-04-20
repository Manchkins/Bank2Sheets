#!/usr/bin/env python3
"""
Bank2Sheets — Фоновая синхронизация банковских операций с Google Sheets
"""

import os
import json
import sqlite3
import random
import hashlib
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ================= НАСТРОЙКИ =================
MODE = "demo"                              # demo или production
BANKS = ["tbank", "alfabank", "sber"]      # Т-Банк, Альфа-Банк, Сбер
SYNC_INTERVAL_MINUTES = 0.2                 # Интервал синхронизации

# Google Sheets
SHEET_ID = "Ваш URL таблицы"                 # ID таблицы
WORKSHEET_NAME = "Bank_Transactions"       # Постоянное имя листа
CREDS_FILE = "creds.json"

# Реальные ключи (для production режима)
TBANK_CLIENT_ID = os.getenv("TBANK_CLIENT_ID", "")
TBANK_API_KEY = os.getenv("TBANK_API_KEY", "")
TBANK_ACCOUNT = os.getenv("TBANK_ACCOUNT", "")

ALFA_TOKEN = os.getenv("ALFA_TOKEN", "")
ALFA_ACCOUNT_ID = os.getenv("ALFA_ACCOUNT_ID", "")

SBER_TOKEN = os.getenv("SBER_TOKEN", "")
SBER_ACCOUNT = os.getenv("SBER_ACCOUNT", "")
SBER_CERT_PATH = os.getenv("SBER_CERT", "")
SBER_KEY_PATH = os.getenv("SBER_KEY", "")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bank_sync.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

running = True

def signal_handler(signum, frame):
    global running
    logger.info("🛑 Остановка...")
    running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ========== БАЗА ДАННЫХ ==========
class Database:
    def __init__(self):
        self.db_path = "bank_sync.db"
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    bank TEXT PRIMARY KEY,
                    last_sync_date TEXT,
                    last_transaction_id TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    bank TEXT,
                    processed_at TEXT
                )
            """)
            conn.commit()
    
    def is_processed(self, tx_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT 1 FROM processed_transactions WHERE transaction_id = ?", (tx_id,))
            return cursor.fetchone() is not None
    
    def mark_processed(self, tx_id: str, bank: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO processed_transactions VALUES (?, ?, ?)",
                (tx_id, bank, datetime.now().isoformat())
            )
            conn.commit()
    
    def update_sync_state(self, bank: str, last_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO sync_state VALUES (?, ?, ?)",
                (bank, datetime.now().isoformat(), last_id)
            )
            conn.commit()
    
    def clear_all(self):
        """Очищает все данные (для тестирования)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM processed_transactions")
            conn.execute("DELETE FROM sync_state")
            conn.commit()

# ========== ГЕНЕРАТОР ТЕСТОВЫХ ДАННЫХ ==========
class MockBankAPI:
    def __init__(self):
        self.counter = 0
    
    def get_transactions(self, bank: str) -> List[Dict]:
        transactions = []
        self.counter += 1
        for i in range(random.randint(5, 15)):
            days_ago = random.randint(0, 30)
            tx_date = datetime.now() - timedelta(days=days_ago)
            amount = round(random.uniform(100, 100000), 2)
            tx_type = "credit" if random.random() > 0.55 else "debit"
            tx_id = hashlib.md5(f"{bank}_{tx_date}_{self.counter}_{i}_{amount}".encode()).hexdigest()[:16]
            
            # Генерация ИНН (10 цифр для юрлица)
            inn = str(random.randint(1000000000, 9999999999))
            
            # Генерация БИК (9 цифр, начинается с 04)
            bik = f"04{random.randint(1000000, 9999999)}"
            
            transactions.append({
                "id": tx_id,
                "bank": bank,
                "date": tx_date.strftime("%Y-%m-%d %H:%M:%S"),
                "amount": amount if tx_type == "credit" else -amount,
                "currency": "RUB",
                "type": tx_type,
                "purpose": random.choice(["Оплата по договору", "Аренда", "Зарплата", "Налоги", "Эквайринг"]),
                "counterparty": random.choice(["ООО Ромашка", "ИП Иванов", "АО ТехноСервис", "ПАО Сбер"]),
                "counterparty_inn": inn,
                "counterparty_bik": bik,
                "status": "COMPLETED"
            })
        return sorted(transactions, key=lambda x: x['date'], reverse=True)

# ========== API КЛИЕНТЫ БАНКОВ ==========
class TBankClient:
    def __init__(self, db: Database):
        self.db = db
        self.name = "tbank"
        self.mock = MockBankAPI()
    
    def get_transactions(self) -> List[Dict]:
        if MODE == "demo":
            return self.mock.get_transactions(self.name)
        
        url = "https://business.tbank.ru/openapi/api/v1/statement"
        headers = {
            "Authorization": f"Bearer {TBANK_API_KEY}",
            "x-IBM-Client-Id": TBANK_CLIENT_ID
        }
        params = {
            "accountNumber": TBANK_ACCOUNT,
            "from": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z"),
            "to": datetime.now().strftime("%Y-%m-%dT23:59:59Z")
        }
        response = requests.get(url, headers=headers, params=params)
        return response.json().get("transactions", [])

class AlfaBankClient:
    def __init__(self, db: Database):
        self.db = db
        self.name = "alfabank"
        self.mock = MockBankAPI()
    
    def get_transactions(self) -> List[Dict]:
        if MODE == "demo":
            return self.mock.get_transactions(self.name)
        
        url = "https://baas.alfabank.ru/api/pp/v1/reports/type/account/balance"
        headers = {"Authorization": f"Bearer {ALFA_TOKEN}"}
        response = requests.post(url, headers=headers, json={"accountIds": [ALFA_ACCOUNT_ID]})
        report_id = response.json().get("id")
        return []

class SberClient:
    def __init__(self, db: Database):
        self.db = db
        self.name = "sber"
        self.mock = MockBankAPI()
    
    def get_transactions(self) -> List[Dict]:
        if MODE == "demo":
            return self.mock.get_transactions(self.name)
        
        url = "https://api.sberbank.ru/fintech/api/v2/statement"
        headers = {"Authorization": f"Bearer {SBER_TOKEN}"}
        params = {"accountNumber": SBER_ACCOUNT}
        
        if SBER_CERT_PATH and SBER_KEY_PATH:
            response = requests.get(url, headers=headers, params=params, cert=(SBER_CERT_PATH, SBER_KEY_PATH))
        else:
            response = requests.get(url, headers=headers, params=params)
        
        return response.json().get("transactions", [])

# ========== GOOGLE SHEETS ==========
class GoogleSheetsWriter:
    def __init__(self, sheet_id: str, creds_file: str, worksheet_name: str):
        self.sheet_id = sheet_id
        self.creds_file = creds_file
        self.worksheet_name = worksheet_name
        self.service = None
    
    def authenticate(self) -> bool:
        if not os.path.exists(self.creds_file):
            logger.error(f"Файл {self.creds_file} не найден")
            return False
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.creds_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            self.service = build("sheets", "v4", credentials=creds)
            logger.info(f"✅ Google Sheets подключён, лист: {self.worksheet_name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return False
    
    def _get_sheet_id(self) -> Optional[int]:
        """Получает числовой ID листа по имени"""
        try:
            metadata = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
            for sheet in metadata.get("sheets", []):
                if sheet["properties"]["title"] == self.worksheet_name:
                    return sheet["properties"]["sheetId"]
            return None
        except Exception as e:
            logger.error(f"Ошибка получения ID листа: {e}")
            return None
    
    def write(self, transactions: List[Dict]) -> bool:
        if not self.service:
            return False
        
        if not transactions:
            logger.info("Нет данных для записи")
            return True
        
        # Заголовки
        headers = ["Банк", "ID", "Дата", "Сумма", "Валюта", "Тип", "Назначение", "Контрагент", "ИНН", "БИК", "Статус", "Время синхронизации"]
        
        # Формируем строки новых транзакций
        rows = []
        for tx in transactions:
            rows.append([
                tx.get('bank', ''),
                tx.get('id', tx.get('transactionId', '')),
                tx.get('date', tx.get('paymentDate', '')),
                tx.get('amount', 0),
                tx.get('currency', 'RUB'),
                "Пополнение" if tx.get('type') == 'credit' else "Списание",
                tx.get('purpose', ''),
                tx.get('counterparty', tx.get('counterParty', {}).get('name', '')),
                tx.get('counterparty_inn', tx.get('counterParty', {}).get('inn', '')),
                tx.get('counterparty_bik', tx.get('counterParty', {}).get('bankCode', '')),
                tx.get('status', 'COMPLETED'),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])
        
        try:
            # Проверяем существует ли лист
            sheet_id = self._get_sheet_id()
            
            if sheet_id is None:
                # Создаём лист с заголовками
                body = {
                    "requests": [{
                        "addSheet": {
                            "properties": {"title": self.worksheet_name}
                        }
                    }]
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.sheet_id, body=body
                ).execute()
                # Записываем заголовки
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.sheet_id,
                    range=f"{self.worksheet_name}!A1:L1",
                    valueInputOption="USER_ENTERED",
                    body={"values": [headers]}
                ).execute()
                logger.info(f"📄 Создан лист: {self.worksheet_name}")
                start_row = 2
            else:
                # Получаем текущее количество строк
                result = self.service.spreadsheets().values().get(
                    spreadsheetId=self.sheet_id,
                    range=f"{self.worksheet_name}!A:A"
                ).execute()
                existing_rows = result.get('values', [])
                start_row = len(existing_rows) + 1
                logger.info(f"📄 Существующий лист, текущих строк: {len(existing_rows)}")
            
            # Добавляем новые строки в конец
            range_name = f"{self.worksheet_name}!A{start_row}:L{start_row + len(rows) - 1}"
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body={"values": rows}
            ).execute()
            
            logger.info(f"✅ Добавлено {len(rows)} новых транзакций (строки {start_row}-{start_row + len(rows) - 1})")
            logger.info(f"🔗 https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            return True
            
        except HttpError as e:
            logger.error(f"Ошибка записи: {e}")
            return False

# ========== ОРКЕСТРАТОР ==========
class BankSyncOrchestrator:
    def __init__(self, db: Database, sheet_writer: GoogleSheetsWriter):
        self.db = db
        self.sheet_writer = sheet_writer
        self.clients = {
            "tbank": TBankClient(db),
            "alfabank": AlfaBankClient(db),
            "sber": SberClient(db)
        }
    
    def sync_all(self):
        all_new_transactions = []
        
        for bank_name in BANKS:
            if bank_name not in self.clients:
                logger.warning(f"Банк {bank_name} не поддерживается")
                continue
            
            client = self.clients[bank_name]
            logger.info(f"🔄 Синхронизация {bank_name}...")
            
            try:
                transactions = client.get_transactions()
                
                new_count = 0
                for tx in transactions:
                    tx_id = tx.get('id', tx.get('transactionId', ''))
                    if tx_id and not self.db.is_processed(tx_id):
                        self.db.mark_processed(tx_id, bank_name)
                        all_new_transactions.append(tx)
                        new_count += 1
                
                if transactions:
                    last_id = transactions[0].get('id', transactions[0].get('transactionId', ''))
                    if last_id:
                        self.db.update_sync_state(bank_name, last_id)
                
                logger.info(f"   ✅ {new_count} новых операций")
                
            except Exception as e:
                logger.error(f"   ❌ Ошибка: {e}")
        
        # Запись в Google Sheets
        if all_new_transactions:
            logger.info(f"📤 Запись {len(all_new_transactions)} операций в Google Sheets")
            self.sheet_writer.write(all_new_transactions)
        else:
            logger.info("📭 Новых операций нет")
        
        return all_new_transactions

# ========== ЗАПУСК ==========
def main():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║     Bank2Sheets — Сбор банковских операций в Google Sheets   ║
    ║                    Фоновая синхронизация                      ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    logger.info("🚀 Запуск Bank2Sheets (фоновая служба)")
    logger.info(f"📋 Банки: {', '.join(BANKS)}")
    logger.info(f"⏱️ Интервал: {SYNC_INTERVAL_MINUTES} минут")
    logger.info(f"📊 Лист Google Sheets: {WORKSHEET_NAME}")
    
    db = Database()
    sheet_writer = GoogleSheetsWriter(SHEET_ID, CREDS_FILE, WORKSHEET_NAME)
    
    if not sheet_writer.authenticate():
        logger.error("Не удалось подключиться к Google Sheets")
        logger.info("Проверьте:")
        logger.info("  1. Файл creds.json существует")
        logger.info("  2. SHEET_ID правильный")
        logger.info("  3. Email сервисного аккаунта добавлен в редакторы таблицы")
        return
    
    orchestrator = BankSyncOrchestrator(db, sheet_writer)
    
    # Первый запуск сразу
    logger.info("📡 Первый запуск синхронизации...")
    orchestrator.sync_all()
    
    # Настройка планировщика
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        orchestrator.sync_all,
        trigger=IntervalTrigger(minutes=SYNC_INTERVAL_MINUTES),
        id='bank_sync'
    )
    scheduler.start()
    
    logger.info(f"⏳ Фоновая служба запущена. Следующий запуск через {SYNC_INTERVAL_MINUTES} минут")
    logger.info("🛑 Для остановки нажмите Ctrl+C")
    
    # Бесконечный цикл
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("🛑 Остановлено пользователем")

if __name__ == "__main__":
    main()
