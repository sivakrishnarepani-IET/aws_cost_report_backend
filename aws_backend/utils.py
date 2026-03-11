#utils.py
import duckdb
from datetime import datetime

DB_PATH = "../aws_duck_data/aws_cur_data.duckdb"

def date_range_check(selected_start_date, selected_end_date):

    # convert string → datetime with timestamp
    selected_start_date = datetime.strptime(selected_start_date, "%d-%m-%Y %H:%M")
    selected_end_date = datetime.strptime(selected_end_date, "%d-%m-%Y %H:%M")

    query = """
    SELECT DISTINCT billing_period_start, billing_period_end
    FROM aws_cur
    WHERE ? >= billing_period_start
    AND ? <= billing_period_end
    LIMIT 1
    """

    with duckdb.connect(DB_PATH) as con:
        result = con.execute(query, [selected_start_date, selected_end_date]).fetchone()

    if result:
        return True, {
            "bill_start_date": result[0],
            "bill_end_date": result[1]
        }

    return False, "Selected dates are not within a single billing period"


# Example call
# result, info = date_range_check("05-12-2025 10:00", "20-12-2025 15:30")
def get_all_bill_range_dates():
    try:
        with duckdb.connect(DB_PATH) as con:
            query = f"""
            SELECT 
            distinct billing_period_start,billing_period_end
            from aws_cur
            """
            result = con.execute(query).fetchall()
            return True,[
                {
                    'bill_start_date':row[0],
                    'bill_end_date':row[1]
                }
                for row in result
            ]

    except Exception as e:
        return False,str(e)

def check_and_normalize_dates(start_date=None, end_date=None):
    """
    Normalize dates and validate billing period.

    Rules:
    - If no dates → use latest billing period
    - If date has no timestamp → add 00:00
    - If date equals billing start/end → set 05:30
    """

    try:

        with duckdb.connect(DB_PATH) as con:

            # get billing periods
            bill_query = """
            SELECT DISTINCT billing_period_start, billing_period_end
            FROM aws_cur
            ORDER BY billing_period_start
            """
            bill_ranges = con.execute(bill_query).fetchall()

            if not bill_ranges:
                return False, "Billing period not found"

            latest_bill_start, latest_bill_end = bill_ranges[-1] # TAKE TIME ZONE FROM HERE AND ADD IN BELOW STRPTIME
            timezone = latest_bill_start.tzinfo

            # if no params use latest bill period
            if not start_date and not end_date:
                start_dt = latest_bill_start
                end_dt = latest_bill_end

            else:

                # def normalize(date_str,flag):

                #     date_str = date_str.strip()

                #     try:
                #         return datetime.strptime(date_str, "%d-%m-%Y %H:%M").time().replace(tzinfo=timezone)
                #     except:
                #         date_obj = datetime.strptime(date_str, "%d-%m-%Y")

                #         # billing boundary rule
                #         if date_obj.date() == latest_bill_start.date():
                #             return datetime.combine(
                #                 date_obj.date(),
                #                 datetime.strptime("05:30", "%H:%M").time().replace(tzinfo=timezone)
                #             )

                #         if date_obj.date() == latest_bill_end.date():
                #             return datetime.combine(
                #                 date_obj.date(),
                #                 datetime.strptime("05:30", "%H:%M").time().replace(tzinfo=timezone)
                #             )

                #         if flag == 'start': 
                #             return datetime.combine(
                #                 date_obj.date(),
                #                 datetime.strptime("00:00", "%H:%M").time().replace(tzinfo=timezone)
                #             )
                #         if flag == 'end':
                #             return datetime.combine(
                #                 date_obj.date(),
                #                 datetime.strptime("23:59", "%H:%M").time().replace(tzinfo=timezone)
                #             )

                def normalize(date_str, flag):
                    date_str = date_str.strip()

                    # 1️⃣ Try ISO format (React / JS sends this)
                    try:
                        return datetime.fromisoformat(date_str)
                    except:
                        pass

                    # 2️⃣ Try DD-MM-YYYY HH:MM
                    try:
                        return datetime.strptime(date_str, "%d-%m-%Y %H:%M")
                    except:
                        pass

                    # 3️⃣ Try DD-MM-YYYY
                    try:
                        date_obj = datetime.strptime(date_str, "%d-%m-%Y")

                        # billing boundary rule
                        if date_obj.date() == latest_bill_start.date():
                            return datetime.combine(
                                date_obj.date(),
                                datetime.strptime("05:30", "%H:%M").time()
                            )

                        if date_obj.date() == latest_bill_end.date():
                            return datetime.combine(
                                date_obj.date(),
                                datetime.strptime("05:30", "%H:%M").time()
                            )

                        if flag == "start":
                            return datetime.combine(
                                date_obj.date(),
                                datetime.strptime("00:00", "%H:%M").time()
                            )

                        if flag == "end":
                            return datetime.combine(
                                date_obj.date(),
                                datetime.strptime("23:59", "%H:%M").time()
                            )

                    except:
                        raise ValueError("Invalid date format")
                start_dt = normalize(start_date,flag='start')
                end_dt = normalize(end_date,flag='end')

            # validate billing period
            check_query = """
            SELECT 1
            FROM aws_cur
            WHERE ? >= billing_period_start
            AND ? <= billing_period_end
            LIMIT 1
            """

            valid = con.execute(check_query, [start_dt, end_dt]).fetchone()

            if not valid:
                return False, "Selected dates are not within a single billing period"

            return True, {
                "usage_start_date": start_dt,
                "usage_end_date": end_dt
            }

    except Exception as e:
        return False, str(e)
    