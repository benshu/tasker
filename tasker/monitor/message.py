import pickle


class Message:
    @staticmethod
    def serialize(
        hostname,
        worker_name,
        message_type,
        message_value,
    ):
        message = [
            hostname,
            worker_name,
            message_type,
            message_value,
        ]

        serialized_message = pickle.dumps(
            obj=message,
        )

        return serialized_message

    @staticmethod
    def unserialize(
        message,
    ):
        unserialized_message = pickle.loads(
            data=message,
            encoding='utf-8',
        )

        hostname = unserialized_message[0]
        worker_name = unserialized_message[1]
        message_type = unserialized_message[2]
        message_value = unserialized_message[3]

        message = {
            'hostname': hostname,
            'worker_name': worker_name,
            'type': message_type,
            'value': message_value,
        }

        return message
