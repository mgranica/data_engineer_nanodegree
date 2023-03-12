
class DataValidation:
    validations = {
        "users":[
            {'test_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        ], 
        "songs":[
            {'test_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
        ], 
        "artists":[
            {'test_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        ],
        "time":[
            {'test_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0},
        ], 
        "songplays":[
            {'test_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        ]
    }
