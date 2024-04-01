import json
from dataclasses import dataclass
from typing import Literal, TypedDict, List, Union, Optional
import requests

from config import secret
from modules.logger import get_logger

AttachmentColorType = Literal['info', 'good', 'warning', 'danger']
AttachmentFieldType = TypedDict(
    'AttachmentFieldType', {
        'title': str,  # optional
        'value': str,
        'short': bool
    },
    total=False,
)
AttachmentType = TypedDict(
    'AttachmentType', {
        'title': Optional[str],  # optional
        'text': str,  # optional
        'fallback': str,
        'pretext': str,
        'fields': List[AttachmentFieldType],  # optional
        'color': AttachmentColorType
    },
    total=False,
)

logger = get_logger()


@dataclass
class SlackMessage:
    channel: str
    ts: str
    text: str
    user: str


class SlackClient:
    def __init__(self, title, identifier):
        self.token = secret.SLACK_TOKEN
        self.channel = secret.SLACK_CHANNEL

        # default message configs
        self.title = title
        self.identifier = identifier
        self.color = 'info'

        # attachment colors
        self.colors = {
            'info': '#0394fc',
            'good': 'good',
            'warning': 'warning',
            'danger': 'danger'
        }

    @staticmethod
    def _handle_response(response):
        if response.status_code == 200:
            result = response.json()
            is_success = result.get('ok')

            if not is_success:
                error = result.get('error')
                raise Exception(f'Fail to send slack message, error: {error}')
            else:
                return result
        else:
            raise Exception(f'Fail to send slack message, error: {response.text}')

    def _post_message(self, attachments: str):
        """
        :param attachments: 'attachment' payload of Slack post message api
        """
        slack_request_url = 'https://slack.com/api/chat.postMessage'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = [
            ('token', self.token),
            ('channel', self.channel),
            ('as_user', 'true'),
            ('attachments', attachments),
        ]
        response = requests.post(slack_request_url, data=data, headers=headers)
        self._handle_response(response)

    def create_attachments(
            self,
            fields: List[AttachmentFieldType],
            color: AttachmentColorType = 'danger',
            fallback=None,
            pretext=None,
    ):
        """
        :param pretext: Text before attachment
        :param fields: Attachment fields
        :param color: Color of attachments
        :param fallback: Summary of message. Will be used in notifications etc.
        :return:
        """
        attachment: AttachmentType = {'color': self.colors[color]}
        if len(fields) > 1:
            attachment['fields'] = fields
        else:
            message = fields[0]
            attachment['title'] = message.get('title')
            attachment['text'] = message['value']
        if fallback:
            attachment['fallback'] = fallback
            attachment['pretext'] = fallback
        if pretext is not None:
            attachment['pretext'] = pretext
        return json.dumps([attachment])

    @staticmethod
    def _create_fields(message, subtitle):
        if not message:
            raise ValueError('messages is empty')
        # set fields
        else:
            if isinstance(message[0], dict):
                fields = message
            elif isinstance(message, str):
                fields = [{'title': subtitle, 'value': message}] if subtitle else [{'value': message}]
            else:
                fields = [{'value': message} for message in message]
        return fields

    def send_message(
            self,
            message: Union[str, List[str], List[AttachmentFieldType]],
            title: str = None,
            subtitle: Optional[str] = None,
            summary: Optional[str] = None,
            color: AttachmentColorType = None,
    ):
        """
        :param message: Message to send. Can be either a string, a list of strings or a list of attachment fields
            If message is a string, it can be used with title to create an attachment field.
            If a list of strings is provided, each string will be used as a value of an attachment field.
            If a list of attachment fields is provided, it will be used as is
        :param title: Title of the message. Will be used in pretext & fallback
        :param subtitle: Title of the message. Will be used in attachment field if message is a string
        :param summary: Extra summary of the message. Will be used in fallback
        :param color: Color of the message
        """
        fields = self._create_fields(message, subtitle)
        summary = subtitle or summary

        # set pretext
        header = f'*{title or self.title} ({self.identifier})*'

        # set fallback
        fallback = header
        if summary:
            fallback = f'{header} - {summary}'

        attachments = self.create_attachments(
            fields=fields,
            color=color or self.color,
            fallback=fallback,
            pretext=header,
        )

        logger.info(f'Slack message: {attachments}')

        if self.token and self.channel:
            self._post_message(attachments)
