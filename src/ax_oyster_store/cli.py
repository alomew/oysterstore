import argparse
from datetime import date

from . import func

def load_all_f(args):
    return func.load_all_csvs()

def ynab_csv_f(args):
    parsed_start_date = None
    parsed_end_date = None

    if args.start_date:
        parsed_start_date = date.fromisoformat(args.start_date)
    
    if args.end_date:
        parsed_end_date = date.fromisoformat(args.end_date)
    
    return func.write_csv_for_ynab(parsed_start_date, parsed_end_date)

def balance_f(args):
    return func.show_balance()

def app():
    parser = argparse.ArgumentParser()

    subparser = parser.add_subparsers(required=True)

    load_all_parser = subparser.add_parser("loadall")
    load_all_parser.set_defaults(f = load_all_f)

    ynab_csv_parser = subparser.add_parser("ynabcsv")
    ynab_csv_parser.set_defaults(f = ynab_csv_f)
    ynab_csv_parser.add_argument("--start", dest="start_date")
    ynab_csv_parser.add_argument("--end", dest="end_date")

    balance_parser = subparser.add_parser("balance")
    balance_parser.set_defaults(f = balance_f)

    args = parser.parse_args()

    args.f(args)

    return