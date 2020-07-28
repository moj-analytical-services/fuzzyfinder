import tempfile
from fuzzyfinder.database import SearchDatabase
from fuzzyfinder.record import Record

def test_integrity():

    # Want to test database insert functionality to check that:
    # 1. It's not possible to add the same unique_id twice
    # 2. Token counts are computed correctly when you try and add the same unique_id twice

    record_dicts = []
    record_dict = {"uid": 0, "value": "hello"}
    record_dicts.append(record_dict)

    # test it works at the level of the record
    record = Record(record_dict, unique_id_col='uid')

    # Test it works at the level of the db
    db_filename = tempfile.NamedTemporaryFile().name
    db = SearchDatabase(db_filename)

    db.write_list_dicts_parallel(record_dicts, unique_id_col = 'uid', batch_size=5)

    # Check records are written without uid values

    sql = 'select * from df'
    rec = db.conn.execute(sql).fetchall()[0]
    assert rec['unique_id'] == '0'
    assert '0' not in rec['concat_all']

    # Reconnect to file and check the unique_id_col is correct
    db2 = SearchDatabase(db_filename)

    assert db2.unique_id_col == 'uid'

