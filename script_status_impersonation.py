import csv
import time
from redminelib import Redmine

# Initialize the Redmine connection once
admin_api_key = '1061e0df14b13cd320c769a78f6c30c9b53ad370'
redmine = Redmine('http://localhost:80', key=admin_api_key)

def find(lst, num):
    for i in range(len(lst)):
        if lst[i] == num:
            return i
    return -1

def verify_update(redmine, ticket_id, expected_status):
    try:
        updated_issue = redmine.issue.get(ticket_id, include=['status'])
        status_history = updated_issue.journals
        latest_status_id = None

        # Iterate over the journals to find the latest status update
        for journal in status_history:
            for detail in journal.details:
                if detail['name'] == 'status_id':
                    latest_status_id = int(detail['new_value'])

        if latest_status_id == expected_status:
            print(f"Successfully updated issue {ticket_id} to status {expected_status}.")
        else:
            print(f"Failed to update issue {ticket_id} to status {expected_status}. Please check for related subtasks or other issues.")
    except Exception as e:
        print(f"Verification failed for issue {ticket_id}: {e}")

def register_redmine_update(redmine, ticket_id, status, notes=None):
    try:
        issue = redmine.issue.get(ticket_id, include=['journals'])

        # Ensure required fields are set
        if hasattr(issue, 'due_date') and not issue.due_date:
            issue.due_date = '2024-12-31'  # Example due date
        
        # Update status
        issue.status_id = status
        
        issue.save()
        print(f"Successfully updated issue {ticket_id} to status {status}.")

        # Add a delay before verifying the update
        time.sleep(1)
        
        # Verify if the status has been updated
        verify_update(redmine, ticket_id, status)
            
        # Update notes if provided
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
            status = int(row['status'])
            notes = row.get('notes', None)

            # Print statement to indicate which issue is being processed
            print(f"Processing issue {ticket_id} for user {login} with status {status}.")
            if notes:
                print(f"Notes to be added: {notes}")

            # Create a Redmine object for impersonation
            impersonate_redmine = Redmine('http://localhost:80', impersonate=login, key=admin_api_key)
            register_redmine_update(impersonate_redmine, ticket_id, status, notes)

csv_file_path = 'work.csv'
process_csv_file(csv_file_path)

