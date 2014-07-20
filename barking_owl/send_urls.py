from busaccess import BusAccess
import uuid

def callback(payload):
    print payload

ba = BusAccess(my_id=str(uuid.uuid4()))
ba.set_callback(callback)
ba.send_message(
    command = 'set_dispatcher_urls',
    destination_id = 'broadcast',
    message = {
        'urls': [
            {
                'target_url': 'http://timduffy.me',
                'title': 'TimDuffy.me',
                'max_link_level': 3,
                'doc_type': 'application/pdf',
                'allowed_domains': [],
            }
        ]
    },
)

print 'Done'
