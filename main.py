from db import Data, Database
from timeit import timeit


def update(db):
    keys = [str(i) for i in range(20)]
    i = 0
    for i in range(10000):
        db.update_field(keys[i % len(keys)], Data("USD/BTC", 12.3, 0.0001))
        i += 1
    print(db)


if __name__ == "__main__":
    db = Database("db.log", "db.snap")
    update(db)

