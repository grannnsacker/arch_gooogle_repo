import asyncio
from datetime import datetime

from conf import SPREADSHEET_ID, SERVICE_ACCOUNT_FILE

import gspread
from google.oauth2.service_account import Credentials


class SpreadsheetRepository:
    def __init__(self):
        self.service_account_info = SERVICE_ACCOUNT_FILE
        self.scope = ['https://www.googleapis.com/auth/spreadsheets']
        self.spreadsheet_id = SPREADSHEET_ID

    async def get_sheet(self):
        creds = Credentials.from_service_account_info(self.service_account_info, scopes=self.scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.spreadsheet_id).sheet1
        return sheet

    async def init_name(self, tg_handler, username, team_name):
        sheet = await self.get_sheet()
        column_c_values = sheet.col_values(3)
        t = len(column_c_values) + 1
        sheet.update_cell(t, 3, str(tg_handler))
        sheet.update_cell(t, 4, str(username))
        sheet.update_cell(t, 5, str(team_name))
        sheet.update_cell(t, 6, f"=СУММПРОИЗВ(ЕСЛИ(ЕЧИСЛО(G{t}:CV{t}); G{t}:CV{t}; 0))")

    async def append_from_end(self, user_id: str, points, answer: str, question_id: int):
        sheet = await self.get_sheet()
        column_c_values = sheet.col_values(3)
        row = len(column_c_values) - 1 - column_c_values[::-1].index(user_id) + 1
        col = 7 + question_id * 2
        sheet.update_cell(row, col, points)
        values = sheet.cell(row, col + 1).value  # уже какие то ответы
        if values is None:
            sheet.update_cell(row, col + 1, answer)
        else:
            sheet.update_cell(row, col + 1, values + ';' + answer)

    async def update_cell_in_column_a(self, user_id, value):
        sheet = await self.get_sheet()
        column_c_values = sheet.col_values(3)
        row_index = len(column_c_values) - 1 - column_c_values[::-1].index(user_id) + 1
        sheet.update_cell(row_index, 1, value)  # Обновляем значение в колонке A (1 - это колонка A)

    async def get_points(self, user_id: str):
        sheet = await self.get_sheet()
        column_c_values = sheet.col_values(3)
        row = len(column_c_values) - 1 - column_c_values[::-1].index(user_id) + 1

        col = 6 # F
        value = sheet.cell(row, col).value
        print(value)
        if value:
            return value
        else:
            return 0

    async def write_end_time(self, user_id: str):
        sheet = await self.get_sheet()
        column_c_values = sheet.col_values(3)
        row = len(column_c_values) - 1 - column_c_values[::-1].index(user_id) + 1

        col = 2
        sheet.update_cell(row, col, str(datetime.now()))

    async def parse_method(self, func, *args):
        method = getattr(self, func)
        result = await method(*args)
        if result is not None:
            return result



