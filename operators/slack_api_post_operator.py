import json
from airflow.models import Variable
from airflow.operators.slack_operator import SlackAPIOperator
from typing import Any, List, Optional, Sequence


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel
    Examples:
    https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/_modules/airflow/providers/slack/operators/slack.html#SlackAPIPostOperator
    """
    template_fields: Sequence[str] = (
        'username', 'text', 'attachments', 'blocks', 'channel')

    ui_color = '#b6f0dd'

    def __init__(
        self,
        channel: str = '#general',
        username: str = '467',
        text: str = 'No message has been set.',
        icon_url: str = 'https://raw.githubusercontent.com/apache/'
        'airflow/main/airflow/www/static/pin_100.png',
        attachments: Optional[List] = None,
        blocks: Optional[List] = None,
        **kwargs,
    ) -> None:
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []
        super().__init__(method=self.method, **kwargs)

    def construct_api_call_params(self) -> Any:
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
            'blocks': json.dumps(self.blocks),

        }


class SlackAPIPostOperatorHatiware(SlackAPIPostOperator):
    def __init__(self, **kwargs):
        self.token = Variable.get("slack_token")
        super().__init__(token=self.token, **kwargs)
