import csv
import time
import random
from datetime import datetime, timedelta
import mysql.connector
from redminelib import Redmine

admin_api_key = '1061e0df14b13cd320c769a78f6c30c9b53ad370'
redmine = Redmine('http://localhost:80', key=admin_api_key)

# Database connection details
db_config = {
    'host': 'your_db_host',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'database': 'redmine_db'
}

# Custom order of statuses
custom_order = [1, 8, 13, 12, 19, 9, 2, 11, 14, 10, 3, 21, 17, 5]

def find_next_status(current_status):
    current_index = custom_order.index(current_status)
    if current_index < len(custom_order) - 1:
        return custom_order[current_index + 1]
    return None

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def update_journal_date(journal_id, new_date):
    try:
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()
        update_query = """
        UPDATE journals
        SET created_on = %s
        WHERE id = %s
        """
        cursor.execute(update_query, (new_date, journal_id))
        db.commit()
        cursor.close()
        db.close()
    except Exception as e:
        print(f"Failed to update journal {journal_id} date: {e}")

def verify_update(redmine, ticket_id, expected_status):
    try:
        updated_issue = redmine.issue.get(ticket_id, include=['status'])
        status_history = updated_issue.journals
        latest_status_id = None

        for journal in status_history:
            for detail in journal.details:
                if detail['name'] == 'status_id':
                    latest_status_id = int(detail['new_value'])

        if latest_status_id == expected_status:
            print(f"Successfully updated issue {ticket_id} to status {expected_status} verified.")
        else:
            print(f"Failed to update issue {ticket_id} to status {expected_status}. Please check for related subtasks or other issues.")
    except Exception as e:
        print(f"Verification failed for issue {ticket_id}: {e}")

def register_redmine_update(redmine, ticket_id, notes=None):
    try:
        issue = redmine.issue.get(ticket_id, include=['journals'])
        current_status = issue.status.id
        last_update_date = datetime.strptime(issue.updated_on, '%Y-%m-%dT%H:%M:%SZ')
        today = datetime.now()

        while current_status != 5:
            next_status = find_next_status(current_status)
            if next_status is None:
                break

            issue.status_id = next_status
            issue.save()
            print(f"Successfully updated issue {ticket_id} to status {next_status}.")

            time.sleep(2)
            verify_update(redmine, ticket_id, next_status)

            journals = issue.journals
            latest_journal_id = max(journal.id for journal in journals)
            new_date = random_date(last_update_date, today)
            update_journal_date(latest_journal_id, new_date)

            last_update_date = new_date + timedelta(days=1)
            current_status = next_status

        if notes:
            issue.notes = notes
            issue.save()
            print(f"Added notes to issue {ticket_id}: {notes}")
    except Exception as e:
        print(f"Failed to update issue {ticket_id}: {e}")

def process_csv_file(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            login = row['login']
            ticket_id = int(row['ticket_id'])
            notes = row.get('notes', None)

            print(f"Processing issue {ticket_id} for user {login}.")
            if notes:
                print(f"Notes to be added: {notes}")

            impersonate_redmine = Redmine('http://localhost:80', impersonate=login, key=admin_api_key)
            register_redmine_update(impersonate_redmine, ticket_id, notes)

csv_file_path = 'work.csv'
process_csv_file(csv_file_path)
