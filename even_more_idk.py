import csv
import time
from datetime import datetime, timedelta
import mysql.connector
from redminelib import Redmine

admin_api_key = 'd994f5fa07528c8f5969c60d5be84cf7c2a6b0a4'
redmine = Redmine('http://0.0.0.0:10083', key=admin_api_key)

db_config = {
    'host': '172.20.0.3',
    'port': '3306',
    'user': 'redmine',
    'password': 'password',
    'database': 'redmine_production'
}

# Custom order of statuses
custom_order = [1, 8, 13, 12, 19, 9, 2, 11, 14, 10, 3, 21, 17, 5]

# Statuses assigned to the product owner
product_owner_statuses = [1, 8, 13, 12, 19]

# Statuses assigned to the QA owner
qa_owner_statuses = [9, 2, 11, 14, 10, 3, 21, 17, 5]

def find_next_status(current_status):
    current_index = custom_order.index(current_status)
    if current_index < len(custom_order) - 1:
        return custom_order[current_index + 1]
    return None

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
        updated_issue = redmine.issue.get(ticket_id, include=['status', 'journals'])
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

def wait_for_journal_update(redmine, ticket_id, status_id, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        issue = redmine.issue.get(ticket_id, include=['journals'])
        journals = issue.journals
        for journal in journals:
            for detail in journal.details:
                if detail['name'] == 'status_id' and int(detail['new_value']) == status_id:
                    return journal.id
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for journal update for issue {ticket_id} and status {status_id}")

def ensure_time_window():
    now = datetime.now()
    if now.hour < 10:
        wait_time = (datetime(now.year, now.month, now.day, 10) - now).total_seconds()
        print(f"Waiting until 10am. Sleeping for {wait_time} seconds.")
        time.sleep(wait_time)
    elif now.hour >= 22:
        next_day_10am = datetime(now.year, now.month, now.day, 10) + timedelta(days=1)
        wait_time = (next_day_10am - now).total_seconds()
        print(f"Current time is past 10pm. Waiting until 10am next day. Sleeping for {wait_time} seconds.")
        time.sleep(wait_time)

def get_product_owner_login(ticket_id):
    try:
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor(dictionary=True)
        query = """
        SELECT u.login
        FROM custom_values cv
        JOIN users u ON cv.value = u.id
        WHERE cv.customized_id = %s AND cv.custom_field_id = 2
        """
        cursor.execute(query, (ticket_id,))
        result = cursor.fetchone()
        cursor.close()
        db.close()
        if result:
            return result['login']
        else:
            return None
    except Exception as e:
        print(f"Failed to get product owner login for ticket {ticket_id}: {e}")
        return None

def register_redmine_update(redmine, ticket_id, notes=None):
    try:
        issue = redmine.issue.get(ticket_id, include=['journals'])
        current_status = issue.status.id
        last_update_date = issue.updated_on  # No need to parse this, it's already a datetime object
        today = datetime.now()

        # Calculate number of status changes needed
        statuses_to_change = [status for status in custom_order if custom_order.index(status) > custom_order.index(current_status) and custom_order.index(status) <= custom_order.index(5)]
        num_status_changes = len(statuses_to_change)

        # Calculate the interval between status changes
        total_days = (today - last_update_date).days
        if num_status_changes > 0:
            interval = total_days // num_status_changes
        else:
            interval = 1  # Default to 1 day if no changes are needed

        # Determine the product owner login
        product_owner_login = get_product_owner_login(ticket_id)
        if not product_owner_login:
            print(f"No product owner found for issue {ticket_id}. Skipping this issue.")
            return

        # Process status changes by product owner
        if product_owner_statuses:
            product_owner_redmine = Redmine('http://0.0.0.0:10083', impersonate=product_owner_login, key=admin_api_key)
            for next_status in product_owner_statuses:
                if current_status < next_status and custom_order.index(next_status) <= custom_order.index(5):
                    ensure_time_window()
                    last_update_date += timedelta(days=interval)
                    issue = product_owner_redmine.issue.get(ticket_id)
                    issue.status_id = next_status
                    issue.save()
                    print(f"Product owner updated issue {ticket_id} to status {next_status}.")

                    # Wait for the journal entry to be created
                    journal_id = wait_for_journal_update(product_owner_redmine, ticket_id, next_status)

                    # Update the journal date
                    update_journal_date(journal_id, last_update_date)

                    time.sleep(2)
                    verify_update(product_owner_redmine, ticket_id, next_status)
                    current_status = next_status

        # Process status changes by QA owner
        if qa_owner_statuses:
            qa_owner_redmine = redmine
            for next_status in qa_owner_statuses:
                if current_status < next_status and custom_order.index(next_status) <= custom_order.index(5):
                    ensure_time_window()
                    last_update_date += timedelta(days=interval)
                    issue = qa_owner_redmine.issue.get(ticket_id)
                    issue.status_id = next_status
                    issue.save()
                    print(f"QA owner updated issue {ticket_id} to status {next_status}.")

                    # Wait for the journal entry to be created
                    journal_id = wait_for_journal_update(qa_owner_redmine, ticket_id, next_status)

                    # Update the journal date
                    update_journal_date(journal_id, last_update_date)

                    time.sleep(2)
                    verify_update(qa_owner_redmine, ticket_id, next_status)
                    current_status = next_status

        if notes:
            issue = qa_owner_redmine.issue.get(ticket_id)
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

            impersonate_redmine = Redmine('http://0.0.0.0:10083', impersonate=login, key=admin_api_key)
            register_redmine_update(impersonate_redmine, ticket_id, notes)

csv_file_path = 'work.csv'
process_csv_file(csv_file_path)
