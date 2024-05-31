#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from functools import cached_property
import requests
from airflow.notifications.basenotifier import BaseNotifier

ICON_URL: str = "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png"


class DiscordNotifier(BaseNotifier):
    """
    Discord BaseNotifier.
    """

    # A property that specifies the attributes that can be templated.

    def __init__(
        self,
        discord_webhook_url: str = "discord_webhook_default",
        text: str = "This is a default message",
        username: str = "Airflow",
        avatar_url: str = ICON_URL,
        tts: bool = False,
    ):
        super().__init__()
        self.discord_webhook_url = discord_webhook_url
        self.text = text
        self.username = username
        self.avatar_url = avatar_url
        # If you're having problems with tts not being recognized in __init__(),
        # you can define that after instantiating the class
        self.tts = tts

    def notify(self, context):
        """Send a message to a Discord channel."""
        task_instance = context['task_instance']
        dag_id = context['dag'].dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']
        try_number = task_instance.try_number

        message = (
            f"**{self.text}**\n\n"
            f"**Dag:** {dag_id}\n"
            f"**Task:** {task_id}\n"
            f"**Execution Time:** {execution_date}\n"
            f"**Try Number:** {try_number}\n"
        )
        data = {
            "content": message,
            "username": self.username,
            "avatar_url": self.avatar_url,
            "tts": self.tts
        }
        response = requests.post(self.discord_webhook_url, json=data)
