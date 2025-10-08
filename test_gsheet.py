import gspread
from oauth2client.service_account import ServiceAccountCredentials

# подключаемся к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    "live-place-telegramm-bot-47bf9aa7cdec.json", scope
)
client = gspread.authorize(creds)

# ID таблицы
SPREADSHEET_ID = "1xiWRBOXhuiZa1O5gjZzIHJ9qP1zAPhNaDtCgY9itHVI"  # оставь как есть

# открываем таблицу
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

# читаем данные
data = sheet.get_all_records()
print("Данные из таблицы:")
for row in data:
    print(row)
