#! /usr/bin/python3
# from tabulate import tabulate
import argparse
import boto3
import json
import logging
from boto3.dynamodb.conditions import Key
from time import gmtime, strftime
from dataclasses import dataclass

"""
    Generate inserts for the month-end stripe feed
    ** Make this actually run the full process **
"""

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', 
    filename='looker-ext-user.log',
    encoding='utf-8',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.CRITICAL)


@dataclass
class SelfPayRecord:
    reference_id: str
    current_status: int
    stripe_price: tuple
    stripe_price_currency: str
    stripe_payment_id: str
    stripe_date: str


def get_json_from_s3(bucket: str):
    """
    Get the data out of a S3 bucket
    """
    raise NotImplementedError

def get_data_from_dynamo(dynamodb=None, table_name='hub-selfpay-prod-dynamodb'):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(table_name)
    response = table.query(Select='ALL_ATTRIBUTES')
    return response


def execute_report_queries(query_list: list[str]):
    raise NotImplementedError


def extract_price(platform_data: dict) -> tuple:
    # in testing sometimes currency is not returned
    #   so giving it no value
    try:
        currency = platform_data['M']['currency']['S']
    except KeyError:
        currency = ""
    amount = platform_data['M']['stripePrice']['N']
    return (currency, amount)

def format_reference_id(raw_reference_string) -> str:
    return raw_reference_string.replace("::WFD::PROD", "")

def convert_epoch_time(i: int) -> str:
    t = gmtime(int(i))
    return strftime("%Y-%m-%d", t)


def populate_self_pay_record(d: dict) -> SelfPayRecord:
    # extract returns a tuple of currency and amount
    extracted_price = extract_price(d['platformData'])
    record = SelfPayRecord(
        format_reference_id(d['referenceId']['S']),
        d['currentStatus']['N'],
        extracted_price[1], 
        extracted_price[0],
        d['stripePaymentId']['S'],
        convert_epoch_time(d['ts']['N'])
    )
    return record


def generate_temp_table_schema():
    return "create table #tmp (" \
            "\n\t  reference_id int" \
            "\n\t, current_status int" \
            "\n\t, stripe_price numeric(10, 2)" \
            "\n\t, stripe_price_currency nvarchar(3)" \
            "\n\t, stripe_payment_id nvarchar(50)" \
            "\n\t, stripe_date datetime)" \
            "\ngo"


def generate_insert_from_records(r: SelfPayRecord) -> str:
    insert_statement = "insert into #tmp values" \
                       "({}, {}, {}, '{}', '{}', '{}')".format(
                           r.reference_id,
                           r.current_status,
                           r.stripe_price,
                           r.stripe_price_currency,
                           r.stripe_payment_id,
                           r.stripe_date)
    return insert_statement


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--self_pay_file",
        help="JSON file containing the records needed in the self-pay feed",
        required=True)
    parser.add_argument(
        "--dynamo_db_url",
        help="URL to DynamoDB endpoint",
        required=False)
    parser.add_argument(
        "--dynamo_db_table",
        help="DynamoDB table to scan/query",
        required=False)


    args = parser.parse_args()

    # print(generate_temp_table_schema())
    # get_data_from_dynamo(dynamodb=args.dynamo_db_url, table=args.dynamo_db_table)

    # json returned from dynamo is in list of length, data
    #   so we'll extract the data element and use it from there
    with open(args.self_pay_file) as f:
        data = json.load(f)      
        extracted_data = data[1] 
        for row in extracted_data:
            spr = populate_self_pay_record(row)
            print(generate_insert_from_records(spr))


if __name__ == "__main__":
    main()