import os
import requests
import yaml

slack_url = os.environ.get('SLACK_WEBHOOK_URL')
release_slack_url = os.environ.get('SLACK_WEBHOOK_URL_RELEASE')
job_name = os.environ.get('CI_JOB_NAME')
deploy_env = os.environ.get('DEPLOY_ENV')
branch = os.environ.get('CI_COMMIT_REF_NAME')
version = os.environ.get('CI_COMMIT_TAG')


def in_progress_deployment():
    pipeline_url = os.getenv('CI_PIPELINE_URL')
    message_text = f':hourglass_flowing_sand: <{pipeline_url} | *{job_name}*> | Environment: *{deploy_env}* | Branch: *{branch}* | Version: *{version}*'
    send_slack_notification(message_text)

def notify_deployment_status(status):
    pipeline_url = os.environ.get('CI_PIPELINE_URL')
    job_status = os.getenv('CI_JOB_STATUS')
    if job_status == 'failed':
        message_text = f':x: <{pipeline_url} | *{job_name}*> | Environment: *{deploy_env}*'
    else:
        message_text = f':white_check_mark: <{pipeline_url} | *{job_name}*> | Environment: *{deploy_env}* | Branch: *{branch}* | Version: *{version}*'

    send_slack_notification(message_text)

def send_slack_notification(message):
    if slack_url:
        send_to_slack(slack_url, message)
    # If the environment is prod, send to the dp-release channel as well
    if deploy_env == 'prod' and release_slack_url:
        send_to_slack(release_slack_url, message)

def send_to_slack(url, message):
    data = {'text': message}
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise ValueError(f'Request to Slack returned an error {response.status_code}, the response is:\n{response.text}')