
from datetime import datetime, date
from airflow.exceptions import AirflowFailException
import logging


class AMLUtils:

    @staticmethod
    def parse_date(date_txt: str) -> date:
        if date_txt is None or date_txt.strip() == '':
            return None

        try:
            parsed_date = datetime.strptime(date_txt, '%Y-%m-%d')
            return parsed_date
        except ValueError:
            msg = f"Invalid date value:{date_txt}, expected format: yyyy-MM-dd, e.g. 2022-10-19"
            logging.error(msg)
            raise AirflowFailException(msg)
