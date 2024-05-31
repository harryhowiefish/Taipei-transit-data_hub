
from utils.discord_notifications import DiscordNotifier
from dotenv import load_dotenv
import os
load_dotenv()
discord_webhook_url = os.getenv("discord_webhook")


def notify_success(context):
    print("Task success callback triggered.")
    discord_notifier = DiscordNotifier(
        discord_webhook_url=discord_webhook_url,
        if_sucess=True,
        # text="DAG has succeeded!",
        username="Airflow Bot(success)"
    )
    discord_notifier.notify(context)


def notify_failure(context):
    print("Task failure callback triggered.")
    discord_notifier = DiscordNotifier(
        discord_webhook_url=discord_webhook_url,
        if_sucess=False,
        # text="DAG has failed!",
        username="Airflow Bot(failure)"
    )
    discord_notifier.notify(context)


def task_failure_alert(context):
    print(
        f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")
