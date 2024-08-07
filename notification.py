import requests
from requests.auth import HTTPBasicAuth
import time

ATLAS_HOST = 'http://localhost:21000'
NOTIFICATION_ENDPOINT = '/api/atlas/v2/notifications'
USERNAME = 'admin'
PASSWORD = 'admin'

def get_notifications():
    response = requests.get(
        f"{ATLAS_HOST}{NOTIFICATION_ENDPOINT}",
        auth=HTTPBasicAuth(USERNAME, PASSWORD)
    )
    if response.status_code == 200:
        notifications = response.json()
        return notifications
    else:
        print(f"Failed to fetch notifications: {response.status_code}")
        return None

def main():
    while True:
        notifications = get_notifications()
        if notifications:
            print("Notifications:")
            for notification in notifications.get('entities', []):
                print(notification)
        time.sleep(10)  # Poll every 10 seconds

if __name__ == "__main__":
    main()
