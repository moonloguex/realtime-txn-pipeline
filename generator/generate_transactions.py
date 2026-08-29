import random
import time

import psycopg2

DSN = "host=localhost port=5433 dbname=bankdb user=bankuser password=bankpass"
INTERVAL_RANGE = (0.5, 2.0)  # seconds between transactions
AMOUNT_RANGE = (-50000, 50000)  # negative = withdrawal, positive = deposit


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM accounts")
    accounts = cur.fetchall()

    print(f"generating transactions for {len(accounts)} accounts, ctrl-c to stop")
    while True:
        account_id, name = random.choice(accounts)
        amount = random.randint(*AMOUNT_RANGE)
        cur.execute(
            "INSERT INTO transactions (account_id, amount) VALUES (%s, %s) RETURNING id",
            (account_id, amount),
        )
        txn_id = cur.fetchone()[0]
        print(f"txn={txn_id} account={name} amount={amount}")
        time.sleep(random.uniform(*INTERVAL_RANGE))


if __name__ == "__main__":
    main()
