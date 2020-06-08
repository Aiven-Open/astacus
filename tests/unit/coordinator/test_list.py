"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the list endpoint behaves as advertised
"""


def test_api_list(client, populated_mstorage):
    assert populated_mstorage

    response = client.get("/list")
    assert response.status_code == 200, response.json()

    assert response.json() == {
        'storages': [{
            'backups': [{
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'nodes': 1,
                'files': 1,
                'name': '1',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6,
                'upload_size': 6,
                'upload_stored_size': 10,
            }, {
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'nodes': 1,
                'files': 1,
                'name': '2',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6,
                'upload_size': 6,
                'upload_stored_size': 10,
            }],
            'storage_name': 'x'
        }, {
            'backups': [{
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'nodes': 1,
                'files': 1,
                'name': '3',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6,
                'upload_size': 6,
                'upload_stored_size': 10,
            }],
            'storage_name': 'y'
        }],
    }
