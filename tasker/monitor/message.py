import enum
import datetime
import msgpack


class MessageStruct(enum.Enum):
    hostname = 0
    worker_name = 1
    message_type = 2
    message_value = 3
    date = 4


class MessageType(enum.Enum):
    process = 0
    success = 1
    failure = 2
    retry = 3
    heartbeat = 4


class Message:
    def __init__(self, hostname, worker_name, message_type, message_value, date):
        '''
        '''
        self.hostname = hostname
        self.worker_name = worker_name
        self.message_type = message_type
        self.message_value = message_value
        self.date = date

    def serialize(self):
        '''
        '''
        message = {
            MessageStruct.hostname.value: self.hostname,
            MessageStruct.worker_name.value: self.worker_name,
            MessageStruct.message_type.value: self.message_type.value,
            MessageStruct.message_value.value: self.message_value,
            MessageStruct.date.value: self.date.timestamp(),
        }

        serialized_message = msgpack.dumps(message)

        return serialized_message

    @staticmethod
    def unserialize(message):
        '''
        '''
        unserialized_message = msgpack.loads(message, encoding='utf-8')

        hostname = unserialized_message[MessageStruct.hostname.value]
        worker_name = unserialized_message[MessageStruct.worker_name.value]
        message_type = MessageType(unserialized_message[MessageStruct.message_type.value])
        message_value = unserialized_message[MessageStruct.message_value.value]
        date = datetime.datetime.utcfromtimestamp(unserialized_message[MessageStruct.date.value])

        message = Message(
            hostname=hostname,
            worker_name=worker_name,
            message_type=message_type,
            message_value=message_value,
            date=date,
        )

        return message
