def build_rcs_payload(
    to: str,
    content,
    custom_message_id: str,
    sender: str
):
    return {
        "messages": [
            {
                "sender": sender,
                "destinations": [
                    {
                        "to": to,
                        "messageId": custom_message_id
                    }
                ],
                "content": content
            }
        ]
    }


def build_rcs_payload_batch(messages, sender: str):
    return {
        "messages": [
            {
                "sender": sender,
                "destinations": [
                    {
                        "to": item["to"],
                        "messageId": item["custom_message_id"]
                    }
                ],
                "content": item["content"]
            }
            for item in messages
        ]
    }
