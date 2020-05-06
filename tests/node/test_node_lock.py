"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""


def test_lock_unlock(client):
    # Play with lock
    response = client.get("/node/lock")
    assert response.status_code == 422, response.json()
    response = client.get("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.get("/node/lock?locker=x&ttl=10")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already locked"

    # Play with relock
    response = client.get("/node/relock")
    assert response.status_code == 422, response.json()
    response = client.get("/node/relock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.get("/node/relock?locker=y&ttl=10")
    assert response.status_code == 403, response.json()
    assert response.json()["detail"] == "Locked by someone else"

    # Play with unlock
    response = client.get("/node/unlock?locker=y")
    assert response.status_code == 403, response.json()
    assert response.json()["detail"] == "Locked by someone else"

    response = client.get("/node/unlock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.get("/node/unlock?locker=x")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already unlocked"

    # Ensure unlocked relock doesn't work
    response = client.get("/node/relock?locker=x&ttl=10")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Not locked"
