from airflow.notifications.basenotifier import BaseNotifier
import os
import requests
import json


class Simple_DC_Notifier(BaseNotifier):
    def __init__(self, message):
        self.url = os.environ['discord_webhook']
        self.message = message

    def post(
        self,
        content: str = None,
        username: str = None,
        avatar_url: str = None,
        embeds: str = None,
    ):
        if content is None:
            raise ValueError("required one of content, file, embeds")
        data = {}
        if content:
            data["content"] = content
        if username:
            data["username"] = username
        if avatar_url:
            data["avatar_url"] = avatar_url
        if embeds:
            data["embeds"] = embeds
        return requests.post(
            self.url, {"payload_json": json.dumps(data)}
        )

    def notify(self, context):
        # Send notification here, below is an example
        title = f">>Task: {context['task_instance'].task_id} << \n"
        self.post(content=title + self.message)
