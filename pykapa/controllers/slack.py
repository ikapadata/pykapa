import slackclient

from pykapa.gen_funcs import read_json_file
from pykapa.settings.config import load_config_file


class PykapaSlackClient(slackclient.SlackClient):

    def __init__(self, token, error_channel):
        # config = load_config_file(config_filename)
        self.token = token
        self.error_channel = error_channel
        super(PykapaSlackClient, self).__init__(self.token)

    def post_error(self, message):
        self.api_call('chat.postMessage', channel=self.error_channel, text=message)



def slack_post(channel_name, slack_msg):
    json_bot = read_json_file('./data/authentication/slack/bot_token.json')
    bot_token = json_bot['BOT_TOKEN']
    slack_client = slackclient.SlackClient(bot_token)
    slack_client.api_call('chat.postMessage', channel=channel_name, text=slack_msg)
    # slack_client.chat_postMessage( channel = channel_name, text = slack_msg)