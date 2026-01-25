from lstore.db import Database

def test_create_table():
    db = Database()

    table1 = db.create_table("table1", 5, 1)

    result = db.get_table("table1")
    print(result)
    assert result == table1