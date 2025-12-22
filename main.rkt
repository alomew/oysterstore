#lang at-exp racket

(require csv-reading
         csv-writing
         db
         gregor)

;; connect to the sqlite db
(define db-connection (sqlite3-connect #:database "oyster.sqlite"))

;; setup the table (mimic the csv structure)
#;(query-exec db-connection "create table journey_history(date TEXT, start_time TEXT, end_time TEXT, journey_action TEXT, charge INTEGER, credit INTEGER, balance INTEGER, note TEXT);")

;; read a csv file
(define/match (fix-entry _)
  [((list d st et ja ch cr ba nt)) (list (parse-oyster-date d) st et ja (string->pennies ch) (string->pennies cr) (string->pennies ba) nt)])
(define (csv-entries pth)
  (define entries
    (call-with-input-file pth
      (lambda (in) (csv->list in))))
  (unless (equal? (list-ref entries 1)
                  (list "Date" "Start Time" "End Time" "Journey/Action" "Charge" "Credit" "Balance" "Note"))
    (error "CSV file does not conform to spec"))
  (map fix-entry (drop entries 2)))

;; fix the date format, and make every balance in pennies
(define (parse-oyster-date d)
  (date->iso8601 (parse-date d "dd-MMM-y")))
(define (string->pennies s) 
  (let ([n (string->number s 10 'number-or-false 'decimal-as-exact)])
    (if n
      (* 100 n)
      sql-null)))

(define (add-entries-to-db es)
  ;; don't allow overwriting the same date range
  (define sorted-dates (sort (for/list [[e es]] (iso8601->date (first e))) date<?))
  (define min-date (first sorted-dates))
  (define max-date (last sorted-dates))
  (define r (query-list db-connection "select 1 from journey_history where date(?) <= date and date <= date(?) limit 1" (date->iso8601 min-date) (date->iso8601 max-date)))
  (if (empty? r)
    (call-with-transaction db-connection
        (thunk
        (for [(e es)]
            (apply query-exec db-connection "insert into journey_history values (?, ?, ?, ?, ?, ?, ?, ?)" e))))
    (displayln "Not overwriting the same period")))

;; adds entries to the db and moves the csv file to the "loaded" directory
(define (load-csv pth)
  (add-entries-to-db (csv-entries pth))
  (rename-file-or-directory pth (build-path "csv/loaded" (file-name-from-path pth))))

(define (load-all-csvs)
  (for ([f- (directory-list "csv" #:build? #t)]
         #:do [(define f (path->string f-))]
         #:when (string-suffix? f ".csv"))
    (load-csv f)))


(define (tidy-row-for-ynab v)
  (match (vector->list v)
   [(list d p m outflow inflow) (list d p m (if (sql-null? outflow) "" (/ outflow 100)) (if (sql-null? inflow) "" (/ inflow 100)))]))

(define (csv-for-ynab [start-date (-weeks (today) 2)] [end-date (date 9999 12 31)])
  (define temp-path (make-temporary-file "oystertmp~a.csv"))
  (printf "Writing to: ~a" temp-path)
  (with-output-to-file temp-path
    #:exists 'truncate
    (thunk
      (display-table (cons (list "Date" "Payee" "Memo" "Outflow" "Inflow")
                          (map tidy-row-for-ynab (query-rows db-connection "select date, case when journey_action like 'Auto top-up%' then 'Transfer: Amex' when charge is not null then 'TFL' else '' end, start_time || '-' || end_time || ' ' || journey_action, charge, credit from journey_history where ? <= date and date <= ?"
                          (date->iso8601 start-date)
                          (date->iso8601 end-date))))))))

(define (show-balance)
  (define bal (query-value db-connection "select balance from journey_history order by date desc limit 1;"))
  (define late-date (query-value db-connection "select max(date(date)) from journey_history;"))
  (printf "[latest ~a]\nCurrent balance is: £~a" late-date (~r (/ bal 100) #:precision '(= 2))))

(define (help)
  (displayln @~a{
                 (load-all-csvs) -- pull data from any csvs that have not yet been loaded.
                 (csv-for-ynab [start-date] [end-date]) -- create a ynab format csv file covering start-date to end-date.
                   defaults to: 2 weeks ago until forever
                 (show-balance) -- for ynab reconciliation, show current balance}))
