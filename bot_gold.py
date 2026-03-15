import os
import sqlite3
import logging
from io import StringIO
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_USER_ID = os.getenv("OWNER_USER_ID", "").strip()  # có thể để trống lúc đầu

DB_NAME = "gold_bot.db"
SJC_URL = "https://sjc.com.vn/gia-vang-online"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ====== Mapping loại vàng trong bot -> tên gần đúng trên bảng giá SJC ======
GOLD_TYPE_MAP = {
    "sjc_mieng": [
        "Vàng SJC 1L, 10L, 1KG",
        "Vàng SJC 5 chỉ",
        "Vàng SJC 0.5 chỉ, 1 chỉ, 2 chỉ",
    ],
    "sjc_nhan_9999": [
        "Vàng nhẫn SJC 99,99% 1 chỉ, 2 chỉ, 5 chỉ",
        "Vàng nhẫn SJC 99,99% 0.5 chỉ",
    ],
}

GOLD_TYPE_LABELS = {
    "sjc_mieng": "SJC miếng",
    "sjc_nhan_9999": "Nhẫn SJC 9999",
}

# Conversation states
BUY_DATE, QUANTITY, BUY_PRICE, GOLD_TYPE = range(4)


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buy_date TEXT NOT NULL,
            quantity_chi REAL NOT NULL,
            buy_price_per_chi REAL NOT NULL,
            gold_type TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def get_conn():
    return sqlite3.connect(DB_NAME)


def normalize_number(value):
    """Chuyển '181,800,000' hoặc '181.800' thành float."""
    if value is None:
        return None
    s = str(value).strip()
    s = s.replace("₫", "").replace("đ", "").replace("VNĐ", "").strip()
    s = s.replace(" ", "")

    # Nếu có dạng 181.800 -> khả năng là ngàn đồng
    # Nếu có dạng 181,800,000 -> là đồng
    if s.count(".") > 0 and s.count(",") == 0:
        # có thể là format 181.800 (nghìn đồng)
        try:
            num = float(s.replace(".", ""))
            # giả định dữ liệu kiểu "181.800" nghìn đồng/lượng => 181,800,000 đồng/lượng
            return num * 1000
        except ValueError:
            return None

    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def fetch_sjc_prices():
    """
    Trả về dict:
    {
        "sjc_mieng": {"buy_per_chi": ..., "sell_per_chi": ..., "matched_name": ...},
        "sjc_nhan_9999": {...}
    }
    """
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    resp = requests.get(SJC_URL, headers=headers, timeout=20)
    resp.raise_for_status()

    html = resp.text
    tables = pd.read_html(StringIO(html))

    matched_rows = []

    for df in tables:
        if df.shape[1] < 3:
            continue

        # Chuẩn hóa tên cột
        df.columns = [str(c).strip() for c in df.columns]
        records = df.to_dict(orient="records")

        for row in records:
            vals = list(row.values())
            if len(vals) < 3:
                continue

            row_text = " | ".join(str(v) for v in vals if v is not None)
            matched_rows.append(row_text)

    results = {}

    for internal_type, possible_names in GOLD_TYPE_MAP.items():
        found = None

        for row_text in matched_rows:
            for name in possible_names:
                if name.lower() in row_text.lower():
                    parts = [p.strip() for p in row_text.split("|")]
                    # thường là [Tên, Mua, Bán]
                    if len(parts) >= 3:
                        buy_val = normalize_number(parts[-2])
                        sell_val = normalize_number(parts[-1])
                        if buy_val and sell_val:
                            # SJC niêm yết theo đồng/lượng => chia 10 ra đồng/chỉ
                            found = {
                                "buy_per_chi": buy_val / 10,
                                "sell_per_chi": sell_val / 10,
                                "matched_name": name,
                            }
                            break
            if found:
                break

        if not found:
            raise ValueError(f"Không tìm thấy giá hiện tại cho loại: {internal_type}")

        results[internal_type] = found

    return results


def format_vnd(x):
    return f"{x:,.0f} đ".replace(",", ".")


def format_date(date_str):
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return date_str


def is_authorized(update: Update) -> bool:
    if not OWNER_USER_ID:
        return True
    return str(update.effective_user.id) == OWNER_USER_ID


async def unauthorized(update: Update):
    await update.message.reply_text(
        "Bot này đang giới hạn cho chủ bot sử dụng."
    )


# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    text = (
        "Chào bạn. Đây là bot quản lý vàng.\n\n"
        "Các lệnh chính:\n"
        "/them - thêm giao dịch mua vàng\n"
        "/xem - xem danh sách giao dịch\n"
        "/gia - xem giá vàng hiện tại\n"
        "/taisan - tính tổng tài sản hiện tại\n"
        "/xoa <id> - xóa giao dịch\n"
        "/id - xem Telegram user id của bạn\n\n"
        "Đơn vị số lượng: chỉ\n"
        "Định giá hiện tại: dùng giá mua vào SJC."
    )
    await update.message.reply_text(text)


async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Telegram user id của bạn là: {update.effective_user.id}\n"
        "Hãy copy số này vào OWNER_USER_ID trong file .env nếu muốn khóa bot chỉ cho bạn dùng."
    )


async def gia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    try:
        prices = fetch_sjc_prices()
        msg = ["Giá vàng hiện tại (quy đổi theo chỉ):"]
        for k, v in prices.items():
            msg.append(
                f"- {GOLD_TYPE_LABELS[k]}: mua vào {format_vnd(v['buy_per_chi'])}/chỉ | "
                f"bán ra {format_vnd(v['sell_per_chi'])}/chỉ"
            )
        msg.append("\n*Tài sản hiện tại sẽ tính theo giá mua vào.*")
        await update.message.reply_text("\n".join(msg))
    except Exception as e:
        await update.message.reply_text(f"Lỗi lấy giá vàng: {e}")


async def xem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, buy_date, quantity_chi, buy_price_per_chi, gold_type
        FROM transactions
        ORDER BY id DESC
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return await update.message.reply_text("Chưa có giao dịch nào.")

    lines = ["Danh sách giao dịch:"]
    for row in rows:
        tx_id, buy_date, quantity, buy_price, gold_type = row
        lines.append(
            f"#{tx_id} | {format_date(buy_date)} | "
            f"{quantity:g} chỉ | {format_vnd(buy_price)}/chỉ | {GOLD_TYPE_LABELS.get(gold_type, gold_type)}"
        )

    await update.message.reply_text("\n".join(lines))


async def taisan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT gold_type,
               SUM(quantity_chi) AS total_qty,
               SUM(quantity_chi * buy_price_per_chi) AS total_cost
        FROM transactions
        GROUP BY gold_type
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return await update.message.reply_text("Chưa có giao dịch nào để tính tài sản.")

    try:
        prices = fetch_sjc_prices()
    except Exception as e:
        return await update.message.reply_text(f"Không lấy được giá hiện tại: {e}")

    total_cost_all = 0
    total_market_all = 0
    lines = ["Tổng tài sản hiện tại:"]

    for gold_type, total_qty, total_cost in rows:
        if gold_type not in prices:
            lines.append(f"- {gold_type}: chưa map được giá hiện tại")
            continue

        current_buy_price = prices[gold_type]["buy_per_chi"]
        market_value = total_qty * current_buy_price
        pnl = market_value - total_cost

        total_cost_all += total_cost
        total_market_all += market_value

        lines.append(
            f"\n{GOLD_TYPE_LABELS.get(gold_type, gold_type)}"
            f"\n  Số lượng: {total_qty:g} chỉ"
            f"\n  Giá vốn bình quân: {format_vnd(total_cost / total_qty)}/chỉ"
            f"\n  Giá hiện tại: {format_vnd(current_buy_price)}/chỉ"
            f"\n  Tổng vốn: {format_vnd(total_cost)}"
            f"\n  Giá trị hiện tại: {format_vnd(market_value)}"
            f"\n  Lãi/lỗ tạm tính: {format_vnd(pnl)}"
        )

    total_pnl = total_market_all - total_cost_all
    lines.append("\n---")
    lines.append(f"Tổng vốn toàn bộ: {format_vnd(total_cost_all)}")
    lines.append(f"Tổng giá trị hiện tại: {format_vnd(total_market_all)}")
    lines.append(f"Tổng lãi/lỗ tạm tính: {format_vnd(total_pnl)}")

    await update.message.reply_text("\n".join(lines))


async def xoa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    if not context.args:
        return await update.message.reply_text("Cách dùng: /xoa <id>")

    try:
        tx_id = int(context.args[0])
    except ValueError:
        return await update.message.reply_text("ID phải là số nguyên.")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()

    if deleted:
        await update.message.reply_text(f"Đã xóa giao dịch #{tx_id}.")
    else:
        await update.message.reply_text(f"Không tìm thấy giao dịch #{tx_id}.")


# ================= ADD TRANSACTION FLOW =================

async def them_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return await unauthorized(update)

    await update.message.reply_text("Nhập ngày mua theo dạng dd/mm/yyyy, ví dụ 15/03/2026")
    return BUY_DATE


async def them_buy_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        datetime.strptime(text, "%d/%m/%Y")
    except ValueError:
        await update.message.reply_text("Ngày không đúng định dạng. Hãy nhập dd/mm/yyyy")
        return BUY_DATE

    context.user_data["buy_date"] = text
    await update.message.reply_text("Nhập số lượng vàng (đơn vị chỉ), ví dụ 2.5")
    return QUANTITY


async def them_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace(",", ".")
    try:
        qty = float(text)
        if qty <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Số lượng không hợp lệ. Ví dụ: 1 hoặc 2.5")
        return QUANTITY

    context.user_data["quantity_chi"] = qty
    await update.message.reply_text("Nhập giá mua tại thời điểm mua (đồng/chỉ), ví dụ 9200000")
    return BUY_PRICE


async def them_buy_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace(".", "").replace(",", "")
    try:
        price = float(text)
        if price <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Giá mua không hợp lệ. Ví dụ: 9200000")
        return BUY_PRICE

    context.user_data["buy_price_per_chi"] = price

    keyboard = [
        [
            InlineKeyboardButton("SJC miếng", callback_data="sjc_mieng"),
            InlineKeyboardButton("Nhẫn SJC 9999", callback_data="sjc_nhan_9999"),
        ]
    ]
    await update.message.reply_text(
        "Chọn loại vàng:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return GOLD_TYPE


async def them_gold_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    gold_type = query.data
    context.user_data["gold_type"] = gold_type

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO transactions (buy_date, quantity_chi, buy_price_per_chi, gold_type, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        context.user_data["buy_date"],
        context.user_data["quantity_chi"],
        context.user_data["buy_price_per_chi"],
        gold_type,
        datetime.now().isoformat()
    ))
    conn.commit()
    tx_id = cur.lastrowid
    conn.close()

    await query.edit_message_text(
        "Đã lưu giao dịch:\n"
        f"ID: #{tx_id}\n"
        f"Ngày mua: {context.user_data['buy_date']}\n"
        f"Số lượng: {context.user_data['quantity_chi']:g} chỉ\n"
        f"Giá mua: {format_vnd(context.user_data['buy_price_per_chi'])}/chỉ\n"
        f"Loại vàng: {GOLD_TYPE_LABELS.get(gold_type, gold_type)}"
    )

    context.user_data.clear()
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Đã hủy thao tác.")
    return ConversationHandler.END


def main():
    if not BOT_TOKEN:
        raise ValueError("Bạn chưa điền BOT_TOKEN trong file .env")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("them", them_start)],
        states={
            BUY_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, them_buy_date)],
            QUANTITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, them_quantity)],
            BUY_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, them_buy_price)],
            GOLD_TYPE: [CallbackQueryHandler(them_gold_type)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("id", my_id))
    app.add_handler(CommandHandler("gia", gia))
    app.add_handler(CommandHandler("xem", xem))
    app.add_handler(CommandHandler("taisan", taisan))
    app.add_handler(CommandHandler("xoa", xoa))
    app.add_handler(conv)

    print("Bot đang chạy...")
    app.run_polling()


if __name__ == "__main__":
    main()