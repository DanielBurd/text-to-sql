import os
import base64
import sqlite3
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend
import matplotlib.pyplot as plt
from io import BytesIO, StringIO
import sys
import json
import re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
from slack_sdk.web import WebClient
from dotenv import load_dotenv
from model_implementation import get_code, log_feedback, explanation
import threading

# Connect to an in-memory SQLite database
conn = sqlite3.connect(':memory:', check_same_thread=False)
cursor = conn.cursor()

# Create fact_sessions table
cursor.execute('''
CREATE TABLE fact_sessions
(
    session_creation_ts timestamp NOT NULL,
    user_id varchar(100),
    session_id varchar(256),
    platform varchar(100),
    app_version varchar(100),
    client_type varchar(100),
    client_language varchar(100),
    container_type varchar(100),
    ip_country varchar(100),
    time_zone varchar(100),
    previous_login_ts timestamp
);
''')

# Create fact_rewards table
cursor.execute('''
CREATE TABLE fact_rewards
(
    event_ts timestamp,
    user_id varchar(100),
    session_id varchar(300),
    segment_id int,
    bundle_id int,
    sku_id int,
    amount int,
    event_type varchar(200),
    reward_request_id varchar(200),
    transaction_id int
);
''')

# Create fact_balance table
cursor.execute('''
CREATE TABLE fact_balance 
(
    event_ts timestamp,
    user_id varchar(100),
    received_item_id varchar(100),
    current_item_balance int,
    received_item_quantity int,
    source_type varchar(100),
    source_id varchar(100),
    source_trigger varchar(100),
    correlation_id varchar(200)
);
''')

# Create fact_purchases table
cursor.execute('''
CREATE TABLE fact_purchases
(
    event_ts timestamp NOT NULL,
    user_id varchar(100),
    transaction_id varchar(256),
    price_usd numeric(18,3),
    currency varchar(256),
    platform varchar(100),
    session_id varchar(300),
    transaction_source_id int,
    segment_id int,
    payment_quantity int,
    transaction_amount numeric(18,3),
    sku_id int,
    is_ftd boolean
);
''')

# Create fact_install table
cursor.execute('''
CREATE TABLE fact_install
(
    user_id varchar(100),
    install_ts timestamp,
    install_version varchar(100),
    platform varchar(100)
);
''')

# Commit the table creations
conn.commit()

# Reading data files
fact_balance = pd.read_csv('Query Analysis/fact_balance.csv')
fact_install = pd.read_csv('Query Analysis/fact_install.csv')
fact_purchases = pd.read_csv('Query Analysis/fact_purchases.csv')
fact_rewards = pd.read_csv('Query Analysis/fact_rewards.csv')
fact_sessions = pd.read_csv('Query Analysis/fact_sessions.csv')

# Loading the data into the SQLite database
fact_balance.to_sql('fact_balance', conn, if_exists='append', index=False)
fact_install.to_sql('fact_install', conn, if_exists='append', index=False)
fact_purchases.to_sql('fact_purchases', conn, if_exists='append', index=False)
fact_rewards.to_sql('fact_rewards', conn, if_exists='append', index=False)
fact_sessions.to_sql('fact_sessions', conn, if_exists='append', index=False)


#Please note that the .env file is a local file that contains the private tokens and keys that are being paid for by us. It is not included in the repository. if you would like to run the code, please create a .env file and add the following keys to it: SLACK_BOT_TOKEN, SLACK_APP_TOKEN, API_KEY 
#or contact us for the keys.
load_dotenv('.env')

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))

# Store user input and generated code globally to use in feedback functions
current_user_input = ""
current_generated_code = ""

def execute_code_with_timeout(code, globals_dict, locals_dict, timeout=120):
    def target():
        exec(code, globals_dict, locals_dict)

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError("Code execution exceeded the timeout limit")


@app.message(".*")
def message_handler(message, say):
    global current_user_input, current_generated_code

    # Print the entire message for debugging
    print(f"Received message: {message}")

    # Ignore messages that have a subtype (e.g., 'message_changed', 'bot_message')
    if "subtype" in message:
        return

    if "client_msg_id" not in message:
        return

    user_input = message['text']
    current_user_input = user_input
    print(f"User input: {user_input}")

    code = get_code(user_input)
    current_generated_code = code.replace('```python\n', '').replace('\n```', '').strip()
    print(current_generated_code)
    current_explanation = explanation(current_generated_code)

    # print(f"Generated code: {code}")

    # Redirect the output of the code execution to a plot
    locals_dict = {
        "pd": pd,
        "plt    ": plt,
        "conn": conn,
        "BytesIO": BytesIO,
    }

    # Capture the printed output
    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()

    try:
        # exec(current_generated_code, globals(), locals_dict)
        execute_code_with_timeout(current_generated_code, globals(), locals_dict)
        # Get the printed output
        print("Execution successful.")
        output = mystdout.getvalue()
        print(f"Execution output: {output}")

        # Save the plot to a file
        plot_file_path = "/tmp/plot.png"
        plt.savefig(plot_file_path)
        plt.close()

        # Upload the file to Slack
        try:
            with open(plot_file_path, 'rb') as file_content:
                response = client.files_upload_v2(
                    channels=[message['channel']],
                    file=file_content,
                    title="Generated Plot"
                )
            
            # Send a message indicating the file upload and request feedback
            say(
                text="Please provide feedback on the plot using the buttons below:",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{current_explanation}\nPlease provide feedback on the plot using the buttons below:"
                        }
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "Yes"
                                },
                                "value": "yes",
                                "action_id": "feedback_yes"
                            },
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "No"
                                },
                                "value": "no",
                                "action_id": "feedback_no"
                            },
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "Don't Know"
                                },
                                "value": "dont_know",
                                "action_id": "feedback_dont_know"
                            }
                        ]
                    }
                ]
            )

            print("Plot sent to Slack.")

        except SlackApiError as e:
            print(f"Error uploading file: {e.response['error']}")

    except Exception as e:
        print(f"Error executing code: {str(e)}")

    finally:
        # Restore stdout
        sys.stdout = old_stdout

# Handle feedback actions
@app.action("feedback_yes")
def handle_feedback_yes(ack, body, say):
    ack()
    user = body['user']['id']
    print(f"User {user} selected 'Yes'.")
    log_feedback(current_user_input, current_generated_code, "yes")
    say(f"Thank you for your feedback! If you have any further questions, please don't hesitate to ask.")

@app.action("feedback_no")
def handle_feedback_no(ack, body, say):
    ack()
    user = body['user']['id']
    print(f"User {user} selected 'No'.")
    log_feedback(current_user_input, current_generated_code, "no")
    say(f"Thank you for your feedback! If you have any further questions, please don't hesitate to ask.")

@app.action("feedback_dont_know")
def handle_feedback_dont_know(ack, body, say):
    ack()
    user = body['user']['id']
    print(f"User {user} selected 'Don't Know'.")
    log_feedback(current_user_input, current_generated_code, "dont_know")
    say(f"Thank you for your feedback! If you have any further questions, please don't hesitate to ask.")
    
# Handle message events
@app.event("message")
def handle_message_events(body):
    print(body)

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()


