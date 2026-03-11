from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from utils import date_range_check,get_all_bill_range_dates
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv('DB_PATH')


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 0
@app.get("/")
def root():
    return {"status": "FastAPI running"}

# 1. ok
@app.get('/get_unique_bill_periods')
def get_unique_bill_period_dates():
    context = {
        "success": 1,
        "message": "Bill Periods Fetched Successfully!",
        "data": []
    }
    try:
        with duckdb.connect(DB_PATH) as con:
            query = f"""
            SELECT 
            distinct (billing_period_start),(billing_period_end)
            from aws_cur
            """
            result = con.execute(query).fetchall()
            context['data']=[
                {
                    'bill_start_date':row[0],
                    'bill_end_date':row[1]
                }
                for row in result
            ]
    except Exception as e:
        context['success']=0
        context['message']=str(e)
    return context


from fastapi import Query
from utils import check_and_normalize_dates

# 2 ok   Done
@app.get("/daily-report")
def get_daily_reports(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):

    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:

        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'122')

        with duckdb.connect(DB_PATH) as con:

            query = """
            SELECT
                DATE(usage_start_date) AS starting_date,
                ROUND(SUM(actual_cost),6) AS total_cost
            FROM aws_cur
            WHERE usage_start_date >= ?
            AND usage_end_date <= ?
            GROUP BY DATE(usage_start_date)
            ORDER BY starting_date
            """

            df = con.execute(query, [start_date, end_date]).fetch_df()

            context["data"] = df.to_dict(orient="records")

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 3. ok
@app.get("/cost_by_service")
def get_cost_by_service(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):

    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]
        print(start_date,end_date,'130')
        with duckdb.connect(DB_PATH) as con:

            # query = """
            # SELECT
            #     service_name,
            #     SUM(actual_cost) AS cost
            # FROM aws_cur
            # WHERE usage_start_date >= ?
            # AND usage_end_date <= ?
            # GROUP BY service_name
            # ORDER BY cost DESC
            # """

            query = """
                SELECT
                    service_name,
                    SUM(actual_cost) AS cost
                FROM aws_cur
                WHERE usage_start_date >= ?
                AND usage_end_date <= ?
                AND service_name IS NOT NULL
                AND service_name <> ''
                GROUP BY service_name
                ORDER BY cost DESC;
            """

            result = con.execute(query, [start_date, end_date]).fetchall()

            context["data"] = [
                {
                    "product_name": row[0],
                    "cost": float(row[1])
                }
                for row in result
            ]

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 4. ok
@app.get("/cost_by_account")
def get_cost_by_account(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'184')

        with duckdb.connect(DB_PATH) as con:

            query = """
            SELECT
                usage_account_id,
                SUM(actual_cost) AS cost
            FROM aws_cur
            WHERE usage_start_date >= ?
            AND usage_end_date <= ?
            GROUP BY usage_account_id
            ORDER BY cost DESC
            """

            result = con.execute(query, [start_date, end_date]).fetchall()

            context["data"] = [
                {
                    "account_id": row[0],
                    "cost": float(row[1])
                }
                for row in result
            ]

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context


# 4.1 ok
@app.get("/cost_by_environment")
def get_cost_by_environment(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'122')

        with duckdb.connect(DB_PATH) as con:

            query = """
            SELECT
                environment,
                SUM(actual_cost) AS cost
            FROM aws_cur
            WHERE environment IS NOT NULL
            AND usage_start_date >= ?
            AND usage_end_date <= ?
            GROUP BY environment
            ORDER BY cost DESC
            """

            result = con.execute(query, [start_date, end_date]).fetchall()

            context["data"] = [
                {
                    "environment": row[0],
                    "cost": float(row[1])
                }
                for row in result
            ]

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 5 ok
@app.get("/cost_by_pricing_model")
def get_cost_by_pricing_model(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'296')

        with duckdb.connect(DB_PATH) as con:
            query = """
SELECT
    pricing_model,
    SUM(cost) AS total_cost
FROM (
    SELECT 
        actual_cost AS cost,
        CASE

            WHEN charge_type = 'Usage'
                AND pricing_term != 'Spot'
                THEN 'On-Demand'

            WHEN pricing_term = 'Spot'
                THEN 'Spot'

            WHEN charge_type IN (
                'DiscountedUsage',
                'RIFee'
            )
                THEN 'Reserved Instance'

            WHEN charge_type IN (
                'SavingsPlanCoveredUsage',
                'SavingsPlanRecurringFee',
                'SavingsPlanNegation'
            )
                THEN 'Savings Plan'

            WHEN charge_type IN (
                'Tax',
                'EdpDiscount',
                'BundledDiscount'
            )
                THEN 'Exclude'

            ELSE 'Other'

            END AS pricing_model

            FROM aws_cur
            WHERE usage_start_date >= ?
            AND usage_end_date <= ?
        ) t
        GROUP BY pricing_model
        ORDER BY total_cost DESC
        """

            query1 = """
            SELECT
                pricing_model,
                SUM(cost) AS total_cost
            FROM (
                SELECT 
                    actual_cost AS cost,
                    CASE

                        WHEN charge_type = 'Usage'
                            AND pricing_term != 'Spot'
                            THEN 'On-Demand'

                        WHEN pricing_term = 'Spot'
                            THEN 'Spot'

                        WHEN charge_type IN (
                            'DiscountedUsage',
                            'RIFee'
                        )
                            THEN 'Reserved Instance'

                        WHEN charge_type IN (
                            'SavingsPlanCoveredUsage',
                            'SavingsPlanRecurringFee',
                            'SavingsPlanNegation'
                        )
                            THEN 'Savings Plan'

                        WHEN charge_type IN (
                            'Tax',
                            'EdpDiscount',
                            'BundledDiscount'
                        )
                            THEN 'Exclude'

                        ELSE 'Other'

                    END AS pricing_model

                FROM aws_cur
            )
            from aws_cur
            WHERE usage_start_date >= ?
            AND usage_end_date <= ?
            GROUP BY pricing_model
            ORDER BY total_cost DESC
            """


            result = con.execute(query, [start_date, end_date]).fetchall()

            context["data"] = [
                {
                    "pricing_model": row[0],
                    "cost": float(row[1])
                }
                for row in result
            ]

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 6 ok
@app.get("/coverage_summary")
def get_coverage_summary(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'6')

        with duckdb.connect(DB_PATH) as con:

            query = """

            SELECT

            -- Covered Usage (Savings Plan + Reserved Instance)
            SUM(
                CASE
                    WHEN charge_type IN
                    ('SavingsPlanCoveredUsage','DiscountedUsage')
                    THEN ABS(actual_cost)
                    ELSE 0
                END
            ) AS discounted_usage,


            -- OnDemand Usage
            SUM(
                CASE
                    WHEN charge_type = 'Usage'
                    THEN ABS(actual_cost)
                    ELSE 0
                END
            ) AS ondemand_usage,


            -- Total Usage
            SUM(
                CASE
                    WHEN charge_type IN
                    ('Usage','SavingsPlanCoveredUsage','DiscountedUsage')
                    THEN ABS(actual_cost)
                    ELSE 0
                END
            ) AS total_usage,


            -- Coverage %
            (
            SUM(
                CASE
                    WHEN charge_type IN
                    ('SavingsPlanCoveredUsage','DiscountedUsage')
                    THEN ABS(actual_cost)
                    ELSE 0
                END
            )
            /
            NULLIF(
            SUM(
                CASE
                    WHEN charge_type IN
                    ('Usage','SavingsPlanCoveredUsage','DiscountedUsage')
                    THEN ABS(actual_cost)
                    ELSE 0
                END
            ),0)

            ) * 100 AS coverage_percent

            FROM aws_cur
            WHERE usage_start_date >= ?
            AND usage_end_date <= ?

            """

            result = con.execute(query, [start_date, end_date]).fetchone()

            context["data"] = {
                "discounted_usage": float(result[0] or 0),
                "ondemand_usage": float(result[1] or 0),
                "total_usage": float(result[2] or 0),
                "coverage_percent": float(result[3] or 0)
            }

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 7 ok
@app.get("/commitment_utilization")
def get_commitment_utilization(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):

    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": {}
    }

    try:

        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        with duckdb.connect(DB_PATH) as con:

            # -------------------------------
            # Savings Plan Utilization
            # -------------------------------
            sp_query = """
            SELECT
                SUM(TRY_CAST(used_commitment AS DOUBLE)) AS used,
                SUM(TRY_CAST(total_commitment AS DOUBLE)) AS total,
                SUM(TRY_CAST(used_commitment AS DOUBLE))
                /
                NULLIF(SUM(TRY_CAST(total_commitment AS DOUBLE)),0) * 100
                AS utilization_percent
            FROM aws_cur
            WHERE charge_type = 'SavingsPlanRecurringFee'
            AND usage_start_date >= ?
            AND usage_end_date <= ?
            """

            sp_result = con.execute(
                sp_query, [start_date, end_date]
            ).fetchone()

            # -------------------------------
            # Reserved Instance Utilization
            # -------------------------------
            ri_query = """
            SELECT
                SUM(TRY_CAST(unused_reserved_units AS DOUBLE)) AS unused,
                SUM(TRY_CAST(reserved_units AS DOUBLE)) AS total_reserved,
                SUM(TRY_CAST(reserved_units AS DOUBLE))
                -
                SUM(TRY_CAST(unused_reserved_units AS DOUBLE))
                AS used,
                CASE
                    WHEN SUM(TRY_CAST(reserved_units AS DOUBLE)) = 0
                    THEN 0
                    ELSE
                    (
                        SUM(TRY_CAST(reserved_units AS DOUBLE))
                        -
                        SUM(TRY_CAST(unused_reserved_units AS DOUBLE))
                    )
                    /
                    SUM(TRY_CAST(reserved_units AS DOUBLE)) * 100
                END AS utilization_percent
            FROM aws_cur
            WHERE charge_type = 'RIFee'
            AND usage_start_date >= ?
            AND usage_end_date <= ?
            """

            ri_result = con.execute(
                ri_query, [start_date, end_date]
            ).fetchone()

            sp_used = float(sp_result[0] or 0)
            sp_total = float(sp_result[1] or 0)
            sp_util = float(sp_result[2] or 0)

            ri_unused = float(ri_result[0] or 0)
            ri_total = float(ri_result[1] or 0)
            ri_used = float(ri_result[2] or 0)
            ri_util = float(ri_result[3] or 0)


            combined_total = sp_total + ri_total
            combined_used = sp_used + ri_used

            if combined_total == 0:
                combined_util = 0
            else:
                combined_util = (combined_used / combined_total) * 100

            unused_util = 100 - combined_util

            context["data"] = {
                "used_commitment_percent": round(combined_util, 2),
                "unused_commitment_percent": round(unused_util, 2),
                "savings_plan_utilization_percent": round(sp_util, 2),
                "reserved_instance_utilization_percent": round(ri_util, 2)
            }

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context



# def get_commitment_utilization(
#     usage_start_date: str = Query(None),
#     usage_end_date: str = Query(None)
# ):
#     context = {
#         "success": 1,
#         "message": "Data Fetched Successfully!",
#         "data": {}
#     }

#     try:
#         status, result = check_and_normalize_dates(
#             usage_start_date,
#             usage_end_date
#         )

#         if not status:
#             raise ValueError(result)

#         start_date = result["usage_start_date"]
#         end_date = result["usage_end_date"]

#         print(start_date,end_date,'7')

#         with duckdb.connect(DB_PATH) as con:

#             # Savings Plan Utilization
#             sp_query = """

#             SELECT

#             SUM(TRY_CAST(used_commitment AS DOUBLE)) AS used,

#             SUM(TRY_CAST(total_commitment AS DOUBLE)) AS total,

#             SUM(TRY_CAST(used_commitment AS DOUBLE))
#             /
#             NULLIF(
#                 SUM(TRY_CAST(total_commitment AS DOUBLE)),0
#             ) * 100 AS utilization_percent

#             FROM aws_cur

#             WHERE charge_type = 'SavingsPlanRecurringFee'
#             AND usage_start_date >= ?
#             AND usage_end_date <= ?
#             """

#             sp_result = con.execute(sp_query, [start_date, end_date]).fetchone()


#             # Reserved Instance Utilization
#             ri_query = """

#             SELECT

#             SUM(TRY_CAST(unused_reserved_units AS DOUBLE)) AS unused,

#             SUM(TRY_CAST(reserved_units AS DOUBLE)) AS total_reserved,

#             SUM(TRY_CAST(reserved_units AS DOUBLE))
#             -
#             SUM(TRY_CAST(unused_reserved_units AS DOUBLE))
#             AS diff,

#             CASE
#                 WHEN SUM(TRY_CAST(reserved_units AS DOUBLE)) = 0
#                 THEN 0
#                 ELSE
#                 (
#                     SUM(TRY_CAST(reserved_units AS DOUBLE))
#                     -
#                     SUM(TRY_CAST(unused_reserved_units AS DOUBLE))
#                 )
#                 /
#                 SUM(TRY_CAST(reserved_units AS DOUBLE))
#                 * 100
#             END AS utilization_percent

#             FROM aws_cur

#             WHERE charge_type = 'RIFee'
#             AND usage_start_date = ?


#             """
#             print(con.execute(ri_query,[start_date]).fetch_df())
#             ri_result = con.execute(ri_query, [start_date]).fetchone()

#             print(ri_result,'777')

#             sp_util = float(sp_result[2] or 0)
#             ri_util = float(ri_result[3] or 0)


#             combined_util = 
#             unused_util = 


#             context["data"] = {
#                 "used_commitment_percent": combined_util,
#                 "unused_commitment_percent": unused_util,
#                 "savings_plan_utilization_percent": sp_util,
#                 "reserved_instance_utilization_percent": ri_util
#             }

#     except Exception as e:
#         context["success"] = 0
#         context["message"] = str(e)

#     return context


# ok
# 8. fetch all regions
@app.get('/all_regions')
def get_all_regions():
    context = {
        "success":1,
        "message":"fetched all regions successfully!",
        "data":[]
    }
    query = """
    SELECT
    DISTINCT usage_region
    FROM aws_cur
    WHERE usage_region IS NOT NULL 
    AND usage_region NOT IN ('',)
    """
    try:
        with duckdb.connect(DB_PATH) as con:
            result =con.execute(query).fetchall()
            context["data"] = [row[0] for row in result]

    except Exception as e:
        context['success']=0
        context['message']=str(e)
    return context

# ok
# 9. GET ALL REGION WISE SERVICES
@app.get('/region_services')
def get_services_in_one_region():
    context = {
    "success":1,
    "message":"fetched all regions successfully!",
    "data":[]
    }
    
    query = """
    SELECT
    DISTINCT usage_region
    FROM aws_cur
    WHERE usage_region IS NOT NULL 
    AND usage_region NOT IN ('',)
    """
    try:
        with duckdb.connect(DB_PATH) as con:
            result =con.execute(query).fetchall()
            # context["data"] = [row[0] for row in result]
            for row in result:
                res = con.execute("SELECT DISTINCT service_name FROM aws_cur WHERE usage_region = ? order by usage_region",[row[0]]).fetchall()
                services_arr = [ser[0] for ser in res]
                context['data'].append({
                    "region_name":row[0],
                    "services":services_arr
                    })

    except Exception as e:
        context['success']=0
        context['message']=str(e)
    return context


# need service_name,usage_region as query params,usage_start_date,usage_end_date
# return service_name,usage_start_date,usage_amount as below query
# 10. 
# OK
@app.get("/service_usage")
def get_service_usage(
    service_name: str = Query(...),
    usage_region: str = Query(...),
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    """ This it the function which return each service usage to see it's 24hrs usage 
    in between given dates, service_name, usage_region as query params"""

    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'728')
        
        with duckdb.connect(DB_PATH) as con:

            query = f"""
            SELECT
                concat(service_name, ' (', pricing_unit, ')') AS service_name,
                (usage_start_date).strftime('%H:%M') AS usage_start_time,
                SUM(usage_quantity) as usage_amount
            FROM aws_cur
            WHERE usage_region = ?
            AND service_name = ?
            AND charge_type = 'Usage'
            AND usage_start_date BETWEEN ? AND ?
            GROUP BY service_name, usage_start_date,pricing_unit
            ORDER BY usage_start_date
            """

            result = con.execute(query, [usage_region, service_name, start_date, end_date]).fetchall()
            # print(result)

        added_services_with_diff_units = {} # sevice_name : indx(saved in context data

        for row in result:
            if row[0] not in added_services_with_diff_units.keys():
                next_indx =len(context['data'])
                context['data'].append({
                    row[0]:[{"usage_start_time": str(row[1]), "usage_quantity": row[2]}]
                })
                added_services_with_diff_units[row[0]]=next_indx
            else:
                context['data'][added_services_with_diff_units[row[0]]][row[0]].append({"usage_start_time": str(row[1]), "usage_quantity": row[2]})

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context


# 11 ok
@app.get("/all_users")
def get_all_users():
    
    context = {
        "success": 1,
        "message": "Data Fetched Successfully!",
        "data": []
    }


    try:
        with duckdb.connect(DB_PATH) as con:

            query = """
            SELECT DISTINCT usage_account_id
            FROM aws_cur
            ORDER BY usage_account_id
            """

            result = con.execute(query).fetchall()

            context["data"] = [row[0] for row in result]

    except Exception as e:
        context["success"] = 0
        context["message"] = str(e)

    return context

# 12
@app.get("/region_service_cost")
def get_reg_ser_cost(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    
    context = {
    "success": 1,
    "message": "Data Fetched Successfully!",
    "data": []
    }

    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'12')

        with duckdb.connect(DB_PATH) as con:
            query = """
            select 
            usage_region,
            service_name,
            sum(actual_cost) as total_cost,

            from aws_cur

            where charge_type='Usage'
            AND usage_start_date >= ?
            AND usage_end_date <= ?

            group by usage_region,service_name

            """
            result = con.execute(query,[start_date,end_date]).fetchall()

            temp = {}
            # res = [] # [{reg:[{},{},{}]},]

            for row in result:
                if row[0] not in temp.keys():
                    temp[row[0]] = len(context['data'])
                    context['data'].append({
                        row[0]:[{'service name':row[1],"total_cost":row[2]}]
                    })
                else:
                    context['data'][temp[row[0]]][row[0]].append({
                        'service name':row[1],"total_cost":row[2]
                    })

    except Exception as e:
        context['success']=0
        context['message']=str(e)
    return context


@app.get("/number_cards")
def get_numbers(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        'success':1,
        "message":"data fetched successfully",
        "data":[]
    }
    
    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        print(start_date,end_date,'1015')
        query = """
SELECT
            SUM(actual_cost) AS total_bill_amount,
            COUNT(DISTINCT usage_account_id) AS number_of_users,
            COUNT(DISTINCT usage_region) AS number_of_regions
        FROM aws_cur
        WHERE charge_type = 'Usage'
        AND usage_start_date >= ?
        AND usage_end_date <= ?
        order by total_bill_amount
        """
       
        with duckdb.connect(DB_PATH) as con:
            result = con.execute(query,[start_date,end_date]).fetchall()[0]
        print(result)
        context['data'] = {
            "total_bill_amount":result[0],
            "number_of_users":result[1],
            "number_of_regions":result[2]
        }
    except Exception as e:
        context['success']=0
        context['message']=str(e)

    return context



@app.get("/service_region_cost")
def get_service_region_cost(service: str = Query(...)):
    
    with duckdb.connect(DB_PATH) as con:

        query = """
            SELECT usage_region, SUM(actual_cost) as total_cost
            FROM aws_cur
            WHERE service_name = ?
            GROUP BY usage_region
            ORDER BY total_cost DESC
        """

        # con.execute(query, (service,))
        rows = con.execute(query,[service]).fetchall()

    result = [
        {"region": row[0], "cost": float(row[1])}
        for row in rows
    ]

    return {
        "success": 1,
        "service": service,
        "data": result
    }

@app.get("/get_top_ten")
def get_top_ten(
    usage_start_date: str = Query(None),
    usage_end_date: str = Query(None)
):
    context = {
        'success':1,
        "message":'data fetched successfully',
        "data":{}
    }
    
    try:
        status, result = check_and_normalize_dates(
            usage_start_date,
            usage_end_date
        )

        if not status:
            raise ValueError(result)

        start_date = result["usage_start_date"]
        end_date = result["usage_end_date"]

        # print(start_date,end_date,'1015')
        query = """
        SELECT
    service_name,
    SUM(actual_cost) AS total_bill_amount
FROM aws_cur
WHERE charge_type = 'Usage'
AND usage_start_date >= ?
AND usage_end_date <= ?
GROUP BY service_name
ORDER BY total_bill_amount DESC
LIMIT 10
        """
       
        with duckdb.connect(DB_PATH) as con:
            result = con.execute(query,[start_date,end_date]).fetchall()
        # print(result)
        context['data'] = {
            "top_ten_paying_services":[{"service_name":service_name,"cost":cost} for (service_name,cost) in result],
            # "top_ten_.."
        }
    except Exception as e:
        context['success']=0
        context['message']=str(e)

    return context