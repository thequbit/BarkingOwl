from busaccess import BusAccess
import uuid

def callback(payload):
    print payload

ba = BusAccess(my_id=uuid.uuid4())
ba.set_callback(callback)
ba.listen()
