import csv
import time
from datetime import datetime, timedelta
import mysql.connector
from redminelib import Redmine

admin_api_key = 'd994f5fa07528c8f5969c60d5be84cf7c2a6b0a4'
redmine_url = 'http://0.0.0.0:10083'
redmine = Redmine(redmine_url, key=admin_api_key)

db_config = {
    'host': '172.20.0.3',
    'port': '3306',
    'user': 'redmine',
    'password': 'password',
    'database': 'redmine_production'
}

# Custom order of statuses
product_owner_statuses = [1, 8, 13, 12, 19]
qa_owner_statuses = [9, 2, 11, 14, 10, 3, 21, 17, 5]
complete_status_order = product_owner_statuses + qa_owner_statuses

def update_issue_status_db(ticket_id, status_id, user_id, change_date):
    try:
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()

        # Update the status in the issues table
        update_issue_query = """
        UPDATE issues
        SET status_id = %s, updated_on = %s
        WHERE id = %s
        """
        cursor.execute(update_issue_query, (status_id, change_date, ticket_id))

        # Insert a new journal entry for the status change
        insert_journal_query = """
        INSERT INTO journals (journalized_id, journalized_type, user_id, notes, created_on)
        VALUES (%s, 'Issue', %s, %s, %s)
        """
        cursor.execute(insert_journal_query, (ticket_id, user_id, f'Status changed to {status_id}', change_date))
        journal_id = cursor.lastrowid

        # Insert the details of the status change into journal details
        insert_detail_query = """
        INSERT INTO journal_details (journal_id, property, prop_key, old_value, value)
        VALUES (%s, 'attr', 'status_id', NULL, %s)
        """
        cursor.execute(insert_detail_query, (journal_id, status_id))

        db.commit()
        cursor.close()
        db.close()

        print(f"Updated issue {ticket_id} to status {status_id} in the database.")
    except Exception as e:
        print(f"Failed to update issue {ticket_id} in the database: {e}")

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

def get_user_id_by_login(login):
    try:
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor(dictionary=True)
        query = "SELECT id FROM users WHERE login = %s"
        cursor.execute(query, (login,))
        result = cursor.fetchone()
        cursor.close()
        db.close()
        if result:
            return result['id']
        else:
            return None
    except Exception as e:
        print(f"Failed to get user ID for login {login}: {e}")
        return None

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

def process_ticket(ticket_id, product_owner_login, qa_login, notes=None):
    try:
        issue = redmine.issue.get(ticket_id, include=['journals'])
        current_status = issue.status.id
        last_update_date = issue.updated_on  # No need to parse this, it's already a datetime object
        today = datetime.now()

        # Calculate number of status changes needed
        statuses_to_change = [status for status in complete_status_order if complete_status_order.index(status) > complete_status_order.index(current_status)]
        num_status_changes = len(statuses_to_change)

        # Calculate the interval between status changes
        total_days = (today - last_update_date).days
        interval = total_days // num_status_changes if num_status_changes > 0 else 1

        # Get user IDs for product owner and QA owner
        product_owner_id = get_user_id_by_login(product_owner_login)
        qa_owner_id = get_user_id_by_login(qa_login)

        # Process status changes by product owner
        for next_status in product_owner_statuses:
            if next_status in statuses_to_change:
                ensure_time_window()
                last_update_date += timedelta(days=interval)
                update_issue_status_db(ticket_id, next_status, product_owner_id, last_update_date)

        # Process status changes by QA owner
        for next_status in qa_owner_statuses:
            if next_status in statuses_to_change:
                ensure_time_window()
                last_update_date += timedelta(days=interval)
                update_issue_status_db(ticket_id, next_status, qa_owner_id, last_update_date)

        # Add notes if provided
        if notes:
            issue = redmine.issue.get(ticket_id)
            issue.notes = notes
            issue.save()
            print(f"Added notes to issue {ticket_id}: {notes}")
    except Exception as e:
        print(f"Failed to update issue {ticket_id}: {e}")

def process_csv_file(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            qa_login = row['login']
            ticket_id = int(row['ticket_id'])
            notes = row.get('notes', None)

            print(f"Processing issue {ticket_id} for user {qa_login}.")
            if notes:
                print(f"Notes to be added: {notes}")

            product_owner_login = get_product_owner_login(ticket_id)
            if not product_owner_login:
                print(f"No product owner found for issue {ticket_id}. Skipping this issue.")
                continue

            process_ticket(ticket_id, product_owner_login, qa_login, notes)

csv_file_path = 'work.csv'
process_csv_file(csv_file_path)
