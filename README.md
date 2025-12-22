# Oyster Store

The original implementation was in Racket but it has been partially-ported to Python. The old implementation is still around for comparison whilst I port the last bits and pieces.

Parses CSV Oyster statements from Transport for London website and stores the data basically as-is in s SQLite database.

Can generate a transaction file suitable for YNAB (You Need a Budget) budget software, to track each journey as if it was on a debit card.