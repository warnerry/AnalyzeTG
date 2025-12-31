import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageReactions

load_dotenv()

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]

# ВАЖНО: сюда впиши username канала или ссылку (например: "profbyuro_channel" или "https://t.me/profbyuro_channel")
CHANNEL = os.environ.get("TG_CHANNEL", "").strip()  # можно положить в .env
if not CHANNEL:
    CHANNEL = input("Введи username/ссылку канала (например profbyuro_channel): ").strip()

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "posts.csv"

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text

def reactions_to_int(reactions: MessageReactions | None) -> int:
    """
    Telegram может возвращать несколько типов реакций.
    Суммируем все counts.
    """
    if not reactions or not getattr(reactions, "results", None):
        return 0
    total = 0
    for r in reactions.results:
        # у r есть count
        total += int(getattr(r, "count", 0) or 0)
    return total

async def main():
    # создаст файл сессии рядом: tg_session.session
    async with TelegramClient("tg_session", API_ID, API_HASH) as client:
        entity = await client.get_entity(CHANNEL)

        rows = []
        async for msg in client.iter_messages(entity, limit=None):
            # Берём только посты с текстом (можно расширить на медиа + подписи)
            text = msg.message or ""
            text = clean_text(text)

            if not text:
                continue

            views = int(msg.views or 0)
            reacts = reactions_to_int(msg.reactions)

            # msg.date обычно timezone-aware (UTC)
            dt = msg.date
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            rows.append(
                {
                    "message_id": msg.id,
                    "date_utc": dt.isoformat() if dt else None,
                    "text": text,
                    "text_len": len(text),
                    "views": views,
                    "reactions": reacts,
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            print("Не удалось собрать посты: проверь доступ к каналу и что там есть текстовые посты.")
            return

        # Базовая метрика вовлечённости (реакции на просмотры)
        df["engagement"] = (df["reactions"] / df["views"].replace(0, pd.NA)).fillna(0.0)

        # сортировка по времени
        df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce", utc=True)
        df = df.sort_values("date_utc").reset_index(drop=True)

        df.to_csv(OUT_PATH, index=False, encoding="utf-8")
        print(f"Готово! Сохранено: {OUT_PATH} | Постов: {len(df)}")
        print(df.tail(5)[["date_utc", "views", "reactions", "engagement", "text_len"]])

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
