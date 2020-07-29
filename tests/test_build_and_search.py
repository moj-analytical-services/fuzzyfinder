import tempfile
import pytest


from fuzzyfinder.database import SearchDatabase


def test_build_and_search():


    db_filename = tempfile.NamedTemporaryFile().name


    db = SearchDatabase(db_filename)
    rec1 = {'unique_id': 1, 'first_name': 'robin', 'surname': 'linacre'}
    rec2 = {'unique_id': 2, 'first_name': 'robyn', 'surname': 'linaker'}
    rec3 = {'unique_id': 3, 'first_name': 'robin', 'surname': 'linacre'}
    rec3 = {'unique_id': 4, 'first_name': 'david', 'surname': 'smith'}


    dicts = [rec1, rec2, rec3]
    db.write_list_dicts_parallel(dicts, unique_id_col='unique_id')

    db.build_or_replace_stats_tables()

    search_rec = {'unique_id': 4, 'first_name': 'robin', 'surname': None}

    
    assert '1' in db.find_potental_matches(search_rec).keys()

    # With record caching, we want to make sure that if the search rec is changed but the unique id 
    # is for some reason left the same, we get different search results 

    search_rec = {'unique_id': 4, 'first_name': 'david', 'surname': None}
    
    assert '4' in db.find_potental_matches(search_rec).keys()
