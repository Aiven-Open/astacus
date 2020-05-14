"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""


def test_api_lock_unlock(client):
    # Play with lock
    response = client.post("/node/lock")
    assert response.status_code == 422, response.json()
    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already locked"

    # Play with relock
    response = client.post("/node/relock")
    assert response.status_code == 422, response.json()
    response = client.post("/node/relock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/relock?locker=y&ttl=10")
    assert response.status_code == 403, response.json()
    assert response.json()["detail"] == "Locked by someone else"

    # Play with unlock
    response = client.post("/node/unlock?locker=y")
    assert response.status_code == 403, response.json()
    assert response.json()["detail"] == "Locked by someone else"

    response = client.post("/node/unlock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.post("/node/unlock?locker=x")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already unlocked"

    # Ensure unlocked relock doesn't work
    response = client.post("/node/relock?locker=x&ttl=10")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Not locked"
