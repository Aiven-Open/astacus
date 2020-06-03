"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the list endpoint behaves as advertised
"""

from .test_restore import BACKUP_MANIFEST


def test_api_list(client, mstorage):
    x = mstorage.get_storage("x")
    x.upload_json("backup-1", BACKUP_MANIFEST)
    x.upload_json("backup-2", BACKUP_MANIFEST)
    y = mstorage.get_storage("y")
    y.upload_json("backup-3", BACKUP_MANIFEST)
    response = client.get("/list")
    assert response.status_code == 200, response.json()
    assert response.json() == {
        'storages': [{
            'backups': [{
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'files': 1,
                'name': '1',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6
            }, {
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'files': 1,
                'name': '2',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6
            }],
            'storage_name': 'x'
        }, {
            'backups': [{
                'attempt': 1,
                'end': '2020-02-02T12:34:56',
                'files': 1,
                'name': '3',
                'plugin': 'files',
                'start': '2020-01-01T21:43:00',
                'total_size': 6
            }],
            'storage_name': 'y'
        }],
    }
