import json
import proton
from threading import Thread
from datetime import datetime
from proton.reactor import Container
from proton.handlers import MessagingHandler

import logging

logger = logging.getLogger()

class AMQPClient(MessagingHandler):
    def __init__(self, url, username, password,module=None,subscriptions=None,message_handler=None):
        super(AMQPClient, self).__init__()
        self.message_handler = message_handler
        logger.info("*" * 200)
        logger.info("AMQP Client Initialized")
        logger.info(self.message_handler)

        self.starttime = datetime.now()
        self.VERSION = "1.0.0"
        self.url = url
        self.username = username
        self.password = password
        self.receiver = None
        self.senders = {}
        self.container = None
        self.receivers= {}
        self.subscriptions=subscriptions
        self.conn = None
        self.start()
        self.message_handler = message_handler
        self.module = module
        self.sent=0
        

    def start(self):
        # Start the Container's run method in a separate thread
        thread = Thread(target=self.run, daemon=True)
        thread.start()

    def run(self):
        Container(self).run()

    def on_start(self, event):
        logger.info("Starting AMQP client...")
        self.container = event.container
        self.conn = event.container.connect(
            self.url,
            user=self.username,
            password=self.password
        )
        for sub in self.subscriptions or []:
            # Subscribe to each topic in the subscriptions list
            self.receivers[sub] = event.container.create_receiver(self.conn, sub)
            print(f"Subscribed to {sub}")

    def on_connection_opened(self, event):
        logger.info("Connected to AMQP broker.")

    def on_connection_closed(self, event):
        logger.info("Disconnected from AMQP broker.")

    def on_message(self, event):
        address = event.receiver.source.address
        if event.message.properties:
            headers = {k: v for k, v in event.message.properties.items()}
        else:
            headers = {}

        if self.message_handler:
            if not isinstance(event.message.body, str):
                logger.warning("Message body is  bytes, converting to string.")
                event.message.body = event.message.body.tobytes()
                self.message_handler(address, event.message.body.decode('utf-8'), headers)
            else:
                self.message_handler(address, event.message.body, headers)
        else:
            logger.warning("No message handler defined. Message will not be processed.")

    def generate_life_sign(self):
        return {
            "error": "OK",
            "type": "lifesign",
            "eventtype": "lifesign",
            "module": self.module["name"],
            "version": self.module["version"],
            "alive": 1,
            "sent": self.sent,
            "amqclientversion": self.VERSION,
            "starttimets": self.starttime.timestamp(),
            "starttime": str(self.starttime),
            "connections": len(self.receivers),
            "senders": len(self.senders)
        }
    
    def send_message(self, topic, message,headers=None):
        
        self.sent += 1
        if topic not in self.senders:
            self.senders[topic] = self.container.create_sender(self.conn, topic)

        # Serialize dict to JSON if needed
        msg_body = json.dumps(message)
        msg_body="hello world"
        msg_body={"message":"hello world"}
        msg = proton.Message(body=msg_body,content_encoding="utf-8")

        if headers:
            msg.properties = headers
        #msg.properties={"a":1}

        logger.info(f"Sending message to {topic}: {msg.body}")
        self.senders[topic].send(msg)
        logger.info(f"Message sent to {topic}: {message}")

    def send_life_sign(self,variables=None):        
        logger.info("#=- Send Module Life Sign.")
        lifesignstruct = self.generate_life_sign()

        if variables !=None:
            for key in variables:
                lifesignstruct[key]=variables[key]

        logger.info("====>"*10)
        logger.info(json.dumps(lifesignstruct))
        
        #logger.info(json.dumps(lifesignstruct))
        logger.info("====>"*10)
        variable=   {}
        try:
            self.send_message("queue://NYX_MODULE_INFO", lifesignstruct,variables)
        except:
            logger.error("Unable to send life sign. Error in sending message.", exc_info=True)
    
    


