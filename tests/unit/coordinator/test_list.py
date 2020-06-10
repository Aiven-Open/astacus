"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the list endpoint behaves as advertised
"""
from astacus.coordinator import api


def test_api_list(client, populated_mstorage, mocker):
    assert populated_mstorage

    def _run():
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

    _run()

    # Second run should come from cache
    m = mocker.patch.object(api, "list_backups")
    _run()
    assert not m.called
