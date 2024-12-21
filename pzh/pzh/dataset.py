import os
from pathlib import Path


import asyncio
import typer
from loguru import logger
from tqdm import tqdm
from enum import Enum
from typing import Optional
import json

import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.callbacks import BaseCallbackHandler
from langchain_openai import ChatOpenAI
from tqdm.asyncio import tqdm

from pzh.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

class BatchCallback(BaseCallbackHandler):
	def __init__(self, total: int):
		super().__init__()
		self.count = 0
		self.progress_bar = tqdm(total=total) # define a progress bar

	# Override on_llm_end method. This is called after every response from LLM
	def on_llm_end(self, response, *, run_id, parent_run_id = None, **kwargs):
		self.count += 1
		self.progress_bar.update(1)
          
	def __del__(self):
		self.progress_bar.close()

class Category(Enum):
    CLAIM = "claim"
    SUGGESTION = "suggestion"
    GRATTITUDE = "grattitude"


class FinancialCatetory(Enum):
    NON_FINANCIAL = 0
    FINANCAIL = 1


class ReasonCategory(Enum):
    EMPLOYEE_ERROR = "employee_error"
    TERMS = "terms"
    TECHINCAL = "techincal"
    SERVICE_SPEED = "service_speed"



class Classification(BaseModel):
    """
    Классификация отзывов о банке.
    category
    """
    review_category: int = Field(..., description="0 - Претензия, 1 -  Предложение, 2 - Благодарность")
    financial: int = Field(..., description="1 если финансовое, 0 если нет")
    reason_category: int = Field(..., description="0 - Ошибка сотрудника; 1 - Несогласие с тарифами; 2 - Технический сбой; 3 - Скорость обслуживания;")

async def classify_text(idx, row, llm, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Simulate an async LLM call (replace with your LLM code)
            logger.debug("Started processing")
            llm_resp = await llm.invoke({"title": row.title, "text": row.text, "grade": row.grade})
            logger.debug("Ended processing")
            return idx, llm_resp.review_category, llm_resp.financial, llm_resp.reason_category
        except Exception:
            if attempt < max_retries - 1:
                continue
            else:
                return None

def process(data):
    tagging_prompt = ChatPromptTemplate.from_messages(
    [
        SystemMessage(content="""
Ты - помощник для классификации отзывов о банке. 
В ответе предоставь строго JSON с полями 'review_category', 'financial' и 'reason_category', без дополнительного текста. 
Формат: {format_instructions}
                      
Правила классификации 'review_category':
- 0: Претензия (например, "Недоволен", "Плохо", "Ужасно", "Почему так долго?", "Разочарован", есть выраженное недовольство)
- 1: Предложение (например, "Я думаю, вы могли бы улучшить...","Предлагаю сделать...")
- 2: Благодарность (например, "Спасибо", "Благодарю", "Отлично", "Мне всё понравилось")

Если клиент благодарит или доволен, без претензий – выбирай 2 (благодарность).

financial:
- 1 если претензия или предложение затрагивает финансовые условия (тарифы, проценты, деньги)
- 0 иначе

reason_category:
- 0: Ошибка сотрудника (упоминание конкретных сотрудников или офисов, намёк на человеческий фактор)
- 1: Тарифы/условия (слова «Тариф/Правила/Договор»)
- 2: Тех.сбой («Сбой/Авария/Проблема/Завис»)
- 3: Скорость обслуживания («Медленно/Долго/Быстро/Оперативно»)

Убедись, что при благодарности нет лишних жалоб. 
Пример:
"Спасибо за быстрое обслуживание" -> review_category: 2, financial: 0, reason_category: 3

"""),
    HumanMessage(content="""
Выдели необходимую информацмию из отзыва о банке.
Верни только JSON, соответствующий модели 'Classification'. 
Название: {title}
Оценка: {grade}
Текст:
{text}
""")])
    
    llm = ChatOpenAI(model="t-tech/T-lite-it-1.0", api_key="EMPTY", base_url=os.environ.get("OPENAI_BASE_URL")).with_structured_output(Classification)
    chain = tagging_prompt | llm
    cb = BatchCallback(len(data)) 
    return chain.batch(data, config={"callbacks": [cb]}, return_exceptions=True)
@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR / "banki_reviews325k.json",
    output_path: Path = PROCESSED_DATA_DIR / "banki_reviews325k.csv",
    # ----------------------------------------------
):
    with open(input_path) as f:
        data = json.load(f)
    df = pd.DataFrame.from_dict(data['data'])
    df = df.set_index(df.id)
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset...")
    logger.info("Getting dict...")
    data = []
    for idx, row in df.iterrows():
        data.append({ "title": row.title, "text": row.text, "grade": row.grade})
    
    logger.success("Got dict.")
    logger.info("Classifing...")
    new_data = process(data)
    logger.success("Classified.")
    logger.info("Saving...")
    result = { "review_category": [], "financial": [], "reason_category": []}
    for cls in new_data:
        result["review_category"].append(cls.review_category)
        result["financial"].append(cls.financial)
        result["reason_category"].append(cls.reason_category)
    df["review_category"] = result["review_category"]
    df["financial"] = result["financial"]
    df["reason_category"] = result["reason_category"]
    df.to_csv(output_path)
    logger.success("Saved.")
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
