import sqlite3
import csv
from datetime import datetime, timedelta
import re
import shutil
from pathlib import Path
import tempfile

DB_FIELDS = ["Date", "Start Time", "End Time", "Journey/Action", "Charge", "Credit", "Balance", "Note"]

DB_DIR = Path(Path.home(), "projects/oyster-store")

DB_FILE = Path(DB_DIR, "oyster.sqlite")

CSV_DIR = Path(DB_DIR, "csv")

def get_conn():
    return sqlite3.connect(DB_FILE, autocommit=False)

def setup_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("create table journey_history(date TEXT, start_time TEXT, end_time TEXT, journey_action TEXT, charge INTEGER, credit INTEGER, balance INTEGER, note TEXT);")

    conn.commit()
    conn.close()

def main():
    conn = get_conn()
    cur = conn.cursor()

def parse_oyster_date(d):
    dt = datetime.strptime(d, "%d-%b-%Y")

    return dt.date().isoformat()

def string_to_pennies(s):
    m = re.match(r"(\d+).(\d\d)", s)

    if m is not None:
        return int(m.group(1)) * 100 + int(m.group(2))
    else:
        return None

def fixup_csv_entry(e):
    e["Date"] = parse_oyster_date(e["Date"])

    for k in ["Charge", "Credit", "Balance"]:
        e[k] = string_to_pennies(e[k])

def entries_from_csv(filename):
    with open(filename, newline='') as f:
        # skip the first blank line
        f.readline()
        reader = csv.DictReader(f)

        assert list(reader.fieldnames) == DB_FIELDS
        
        entries = list(reader)

        for e in entries: fixup_csv_entry(e)

        return entries

def tuple_entry(e):
    return tuple([e[k] for k in DB_FIELDS])

def add_entries_to_db(entries):
    assert len(entries) > 0

    min_date = min(entries, key=lambda e: e["Date"])["Date"]
    max_date = max(entries, key = lambda e: e["Date"])["Date"]
    conn = get_conn()
    cur = conn.cursor()

    res = cur.execute("select date from journey_history where date(?) <= date <= date(?)", (min_date, max_date))

    db_dates = set(x[0] for x in res.fetchall())

    for e in entries:
        if e["Date"] in db_dates:
            print(f"Skipping... {e["Date"]} {e["Start Time"]}-{e["End Time"]}")

    diff_date_entries = [tuple_entry(e) for e in entries if e["Date"] not in db_dates]

    cur.executemany("insert into journey_history values (?, ?, ?, ?, ?, ?, ?, ?);", diff_date_entries)

    conn.commit()

    conn.close()


def load_csv(filename):
    entries = entries_from_csv(filename)

    add_entries_to_db(entries)

    filefilename = Path(filename).name

    shutil.move(filename, Path(CSV_DIR, "loaded", filefilename))

def load_all_csvs():
    for filepath in CSV_DIR.iterdir():
        if filepath.is_file and filepath.suffix == ".csv":
            load_csv(filepath)

def db_row_to_ynab(l):
    return [l[0], # date
            l[1], # payee
            l[2], # memo
            l[3] / 100 if l[3] is not None else "", # outflow
            l[4] / 100 if l[4] is not None else "" # inflow
            ]

def write_csv_for_ynab(
        start_date = None,
        end_date = None):

    start_date = start_date or datetime.today().date() - timedelta(weeks=2)
    end_date = end_date or datetime.max.date()

    conn = get_conn()
    cur = conn.cursor()

    with tempfile.NamedTemporaryFile("w", newline='', delete=False, suffix="_YNAB.csv") as tf:
        csv_writer = csv.writer(tf)

        csv_writer.writerow(["Date", "Payee", "Memo", "Outflow", "Inflow"])

        for row in cur.execute("select date, case when journey_action like 'Auto top-up%' then 'Transfer: Amex' when charge is not null then 'TFL' else '' end, start_time || '-' || end_time || ' ' || journey_action, charge, credit from journey_history where ? <= date and date <= ?",
                               (start_date.isoformat(), end_date.isoformat())).fetchall():
            csv_writer.writerow(db_row_to_ynab(row))

        print("Written to:", tf.file.name)

def show_balance():
    conn = get_conn()
    cur = conn.cursor()

    bal = cur.execute("select balance from journey_history order by date desc limit 1;").fetchone()[0] / 100
    late_date = cur.execute("select max(date(date)) from journey_history;").fetchone()[0]

    print(f"[lastest {late_date}]")
    print(f"Current balance is: £{bal:.02f}")

if __name__ == "__main__":
    load_all_csvs()

