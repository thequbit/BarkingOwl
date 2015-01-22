from busaccess import BusAccess
import uuid

def callback(payload):
    print "New Payload:"
    print "---------------------------"
    print payload
    print "\n\n"
    if payload['command'] == 'global_shutdown':
        global ba
        ba.stop_listening()

ba = BusAccess(uid=uuid.uuid4())
ba.set_callback(callback)
#ba.start_listening()
