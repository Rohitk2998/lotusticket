import requests
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import imaplib
import email
from email.header import decode_header

# Email configuration
EMAIL_CONFIG = {
    'user': 'catch-all@elitedodgers.com',
    'password': 'rjn6hqx!XZK6tyj-gzy',
    'hostname': 'mail.automatiq.email',
    'imap_port': 993,
    'smtp_port': 465,
    'connection_type': 'ssl/tls'
}

params = {
            # "has_match": "false",  # Only unmatched
            # "is_pending": "no" ,
            "date_from": '2026-02-01',
            "date_to":   '2026-02-03',
            "page_size": 10000
        }

def connect_to_email() -> Optional[imaplib.IMAP4_SSL]:
    """
    Connect to the email server using IMAP SSL.
    
    Returns:
        IMAP4_SSL connection object or None if connection fails
    """
    try:
        mail = imaplib.IMAP4_SSL(EMAIL_CONFIG['hostname'], EMAIL_CONFIG['imap_port'])
        mail.login(EMAIL_CONFIG['user'], EMAIL_CONFIG['password'])
        print(f"✓ Successfully connected to email: {EMAIL_CONFIG['user']}")
        return mail
    except Exception as e:
        print(f"✗ Failed to connect to email: {e}")
        return None

def decode_email_subject(subject: str) -> str:
    """
    Decode email subject that may contain encoded characters.
    
    Args:
        subject: Raw email subject string
        
    Returns:
        Decoded subject string
    """
    if not subject:
        return ""
    
    decoded_parts = []
    for part, encoding in decode_header(subject):
        if isinstance(part, bytes):
            try:
                decoded_parts.append(part.decode(encoding or 'utf-8'))
            except:
                decoded_parts.append(part.decode('utf-8', errors='ignore'))
        else:
            decoded_parts.append(str(part))
    
    return ''.join(decoded_parts)

def extract_email_body(msg) -> str:
    """
    Extract plain text body from email message.
    
    Args:
        msg: Email message object
        
    Returns:
        Email body as plain text
    """
    body = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    body = part.get_payload(decode=True).decode()
                    break
                except:
                    continue
    else:
        try:
            body = msg.get_payload(decode=True).decode()
        except:
            pass
    
    return body

def list_email_folders() -> List[str]:
    """
    List all available email folders in the mailbox.
    
    Returns:
        List of folder names
    """
    folders = []
    
    try:
        # Connect to email
        mail = connect_to_email()
        if not mail:
            return folders
        
        # List all folders
        status, folder_list = mail.list()
        
        if status == 'OK':
            print("\nAvailable email folders:")
            for folder in folder_list:
                # Decode folder name
                folder_str = folder.decode() if isinstance(folder, bytes) else folder
                print(f"  - {folder_str}")
                folders.append(folder_str)
        
        mail.logout()
        return folders
        
    except Exception as e:
        print(f"✗ Error listing folders: {e}")
        return folders

def search_emails_fast(search_text: str, folder: str = 'INBOX') -> List[Dict]:
    """
    FAST email search using server-side IMAP search. Searches subject and body.
    This is much faster than downloading all emails and searching locally.
    
    Args:
        search_text: Text to search for (e.g., '130696091')
        folder: Folder to search in (default: 'INBOX')
        
    Returns:
        List of matching email dictionaries
    """
    matching_emails = []
    
    try:
        print(f"\n🔍 Searching for '{search_text}' in folder: {folder}")
        
        # Connect to email
        mail = connect_to_email()
        if not mail:
            return matching_emails
        
        # Select folder
        try:
            status, data = mail.select(folder)
            if status != 'OK':
                print(f"✗ Could not select folder '{folder}'")
                mail.logout()
                return matching_emails
        except Exception as e:
            print(f"✗ Error selecting folder '{folder}': {e}")
            mail.logout()
            return matching_emails
        
        # Use IMAP server-side search - MUCH FASTER!
        # Search in both subject and body
        search_criteria = f'OR SUBJECT "{search_text}" BODY "{search_text}"'
        
        status, message_ids = mail.search(None, search_criteria)
        
        if status != 'OK' or not message_ids[0]:
            print(f"✗ No emails found with '{search_text}'")
            mail.close()
            mail.logout()
            return matching_emails
        
        email_ids = message_ids[0].split()
        print(f"✓ Found {len(email_ids)} matching email(s) - Fetching details...")
        
        # Fetch only the matching emails
        for email_id in email_ids:
            try:
                status, msg_data = mail.fetch(email_id, '(RFC822)')
                
                if status != 'OK':
                    continue
                
                # Parse email
                msg = email.message_from_bytes(msg_data[0][1])
                
                # Get email date
                email_date_str = msg.get('Date')
                try:
                    email_date = email.utils.parsedate_to_datetime(email_date_str)
                except:
                    email_date = None
                
                # Decode subject and sender
                subject = decode_email_subject(msg.get('Subject', ''))
                sender = msg.get('From', '')
                
                # Extract body
                body = extract_email_body(msg)
                
                # Create email data dict
                email_data = {
                    'email_id': email_id.decode() if isinstance(email_id, bytes) else email_id,
                    'subject': subject,
                    'sender': sender,
                    'date': email_date,
                    'date_str': email_date.strftime('%Y-%m-%d %H:%M:%S %Z') if email_date else 'Unknown',
                    'body': body,
                    'folder': folder
                }
                
                matching_emails.append(email_data)
                print(f"  ✓ {subject[:80]}...")
                
            except Exception as e:
                print(f"  ✗ Error processing email {email_id}: {e}")
                continue
        
        # Cleanup
        mail.close()
        mail.logout()
        
        # Sort by date (newest first)
        matching_emails.sort(key=lambda x: x['date'] if x['date'] else datetime.min, reverse=True)
        
        print(f"✓ Retrieved {len(matching_emails)} matching email(s)")
        return matching_emails
        
    except Exception as e:
        print(f"✗ Error searching emails: {e}")
        return matching_emails

def search_all_folders_fast(search_text: str) -> List[Dict]:
    """
    FAST search across all email folders using server-side IMAP search.
    
    Args:
        search_text: Text to search for (e.g., '130696091')
        
    Returns:
        List of all matching emails from all folders
    """
    all_matches = []
    
    print(f"\n{'='*80}")
    print(f"🚀 FAST SEARCH FOR: '{search_text}'")
    print(f"{'='*80}")
    
    # Get all folders
    mail = connect_to_email()
    if not mail:
        return all_matches
    
    status, folder_list = mail.list()
    mail.logout()
    
    if status != 'OK':
        print("✗ Could not list folders")
        return all_matches
    
    # Parse folder names
    folder_names = []
    for folder_line in folder_list:
        folder_str = folder_line.decode() if isinstance(folder_line, bytes) else folder_line
        parts = folder_str.split('"')
        if len(parts) >= 3:
            folder_name = parts[-2]
            folder_names.append(folder_name)
    
    print(f"Searching {len(folder_names)} folders...\n")
    
    # Search each folder
    for folder in folder_names:
        try:
            matches = search_emails_fast(search_text, folder)
            if matches:
                all_matches.extend(matches)
        except Exception as e:
            print(f"✗ Error searching folder '{folder}': {e}")
            continue
    
    print(f"\n{'='*80}")
    print(f"✓ TOTAL: Found {len(all_matches)} email(s) containing '{search_text}'")
    print(f"{'='*80}")
    
    return all_matches

def fetch_emails_for_date(target_date: str = None, search_text: Optional[str] = None, time_start: Optional[str] = None, time_end: Optional[str] = None, folder: str = 'INBOX', search_all: bool = False) -> List[Dict]:
    """
    Fetch all emails for a particular date with optional search text and time filtering.
    
    Args:
        target_date: Date string in format 'YYYY-MM-DD' (e.g., '2026-02-02'). If None and search_all=True, searches all emails
        search_text: Optional text to search for in email subject and body (e.g., '130696091')
        time_start: Optional start time in format 'HH:MM' (e.g., '09:00'). If None, no time filtering
        time_end: Optional end time in format 'HH:MM' (e.g., '17:00'). If None, no time filtering
        folder: Email folder to search in (default: 'INBOX')
        search_all: If True, searches ALL emails regardless of date
        
    Returns:
        List of email dictionaries containing subject, sender, date, and body
    """
    emails_list = []
    matching_emails = []
    
    try:
        if search_all:
            print(f"\nSearching ALL emails in folder: {folder}")
        else:
            # Parse target date
            target_date_obj = datetime.fromisoformat(target_date)
            
            # Only apply time filtering if both time_start and time_end are provided
            use_time_filter = time_start is not None and time_end is not None
            
            if use_time_filter:
                start_hour, start_min = map(int, time_start.split(':'))
                search_start_naive = target_date_obj.replace(hour=start_hour, minute=start_min, second=0)
                end_hour, end_min = map(int, time_end.split(':'))
                search_end_naive = target_date_obj.replace(hour=end_hour, minute=end_min, second=59)
                print(f"\nFetching emails for date: {target_date}")
                print(f"Time range: {search_start_naive.strftime('%Y-%m-%d %H:%M')} to {search_end_naive.strftime('%Y-%m-%d %H:%M')}")
            else:
                print(f"\nFetching ALL emails for date: {target_date} in folder: {folder}")
        
        if search_text:
            print(f"Searching for text: '{search_text}'")
        
        # Connect to email
        mail = connect_to_email()
        if not mail:
            return emails_list
        
        # Try to select the specified folder
        try:
            status, data = mail.select(folder)
            if status != 'OK':
                print(f"✗ Could not select '{folder}' folder")
                mail.logout()
                return emails_list
            print(f"✓ Selected folder '{folder}' successfully")
        except Exception as e:
            print(f"✗ Error selecting folder '{folder}': {e}")
            mail.logout()
            return emails_list
        
        # Search for emails
        if search_all:
            # Search ALL emails
            search_criteria = 'ALL'
        else:
            # Search on specific date
            search_date = target_date_obj.strftime('%d-%b-%Y')
            search_criteria = f'(ON "{search_date}")'
        
        status, messages = mail.search(None, search_criteria)
        
        if status != 'OK':
            print("✗ No messages found")
            mail.close()
            mail.logout()
            return emails_list
        
        email_ids = messages[0].split()
        print(f"Found {len(email_ids)} total emails in folder")
        
        emails_in_time_range = 0
        
        # Fetch each email
        count = 0
        for email_id in reversed(email_ids):  # Process newest first
            try:
                status, msg_data = mail.fetch(email_id, '(RFC822)')
                
                if status != 'OK':
                    continue
                
                # Parse email
                msg = email.message_from_bytes(msg_data[0][1])
                
                # Get email date
                email_date_str = msg.get('Date')
                try:
                    email_date = email.utils.parsedate_to_datetime(email_date_str)
                except:
                    email_date = None
                
                # Only apply time filtering if specified and not search_all
                if not search_all and target_date and use_time_filter and email_date:
                    # Convert email_date to naive datetime for comparison
                    email_date_naive = email_date.replace(tzinfo=None) if email_date.tzinfo else email_date
                    
                    # Check if email is within time range (comparing naive datetimes)
                    if not (search_start_naive <= email_date_naive <= search_end_naive):
                        continue
                
                emails_in_time_range += 1
                
                # Decode subject and sender
                subject = decode_email_subject(msg.get('Subject', ''))
                sender = msg.get('From', '')
                
                # Skip excluded subjects
                if subject == 'Rate Your Recent Experience' or 'Relix Issues Featuring Your Favorite' in subject:
                    count += 1
                    continue
            
                # Extract body
                body = extract_email_body(msg)
                
                # Create email data dict
                email_data = {
                    'email_id': email_id.decode() if isinstance(email_id, bytes) else email_id,
                    'subject': subject,
                    'sender': sender,
                    'date': email_date,
                    'date_str': email_date.strftime('%Y-%m-%d %H:%M:%S %Z') if email_date else 'Unknown',
                    'body': body
                }
                
                # Add to all emails list
                emails_list.append(email_data)
                
                # If searching for specific text, check if it's in subject or body
                if search_text:
                    if search_text in subject or search_text in body:
                        matching_emails.append(email_data)
                        print(f"  ✓ FOUND '{search_text}' in email: {subject[:80]}...")
                
            except Exception as e:
                print(f"  Error processing email {email_id}: {e}")
                continue
        
        # Cleanup
        mail.close()
        mail.logout()
        
        print(f"✓ Retrieved {emails_in_time_range} emails")
        
        if search_text:
            print(f"✓ Found {len(matching_emails)} emails containing '{search_text}'")
            # Sort matching emails by date (newest first)
            matching_emails.sort(key=lambda x: x['date'] if x['date'] else datetime.min, reverse=True)
            return matching_emails
        else:
            # Sort all emails by date (newest first)
            emails_list.sort(key=lambda x: x['date'] if x['date'] else datetime.min, reverse=True)
            return emails_list
        
    except Exception as e:
        print(f"✗ Error fetching emails: {e}")
        return emails_list

def get_unmatched_transactions():
    url = 'https://portal.revealmarkets.com/public/api/v1/purchasing/banking-transactions'
    headers = {
                "Authorization": f"Token 8915365073157a5e061cc9174ef262419b1220e9",
                "Content-Type": "application/json"
            }
    count = 0
    reveal = [] 
    print("Parameters for unmatched transactions: ", params )
    while url:
        response = requests.get(url, params=params, headers=headers)
        kk = response.json().get('results', [])   
        count+= len(kk) 
        val = response.json().get('next', None)
        first = response.json().get('results', None)[0]
        reveal.extend(kk)
        url = response.json().get('next', None)   
        return reveal

def get_purchases(is_email_check: bool = False, unmatched_ids: Optional[List[int]] = None) -> List[Dict]:
    url = 'https://skybox.vividseats.com/services/purchases'
    skybox_headers = {
            'X-Api-Token': '8293a10f-6546-457c-8644-2b58a753617a',
            'X-Account': '5052',
            'X-Application-Token': '2140c962-2c86-4826-899a-20e6ae8fad31',
            'Content-Type': 'application/json'
        }
    
    start_date = '2026-02-01'
    end_date = '2026-02-01'
    start_date = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)
    formatted_start_date = (
            start_date
            .strftime("%Y-%m-%dT%H:%M:%S.000Z")
        )

    formatted_end_date = (
            end_date
            .replace(hour=23, minute=59, second=59, microsecond=999000)
            .strftime("%Y-%m-%dT%H:%M:%S.999Z")
        )

    params = {
                'paymentStatus': 'UNPAID',
                'createdDateFrom': formatted_start_date,
                'createdDateTo': formatted_end_date,
                'minOutstandingBalance': 0.01,
                'paymentMethod': 'CREDITCARD'
            }
    if is_email_check:
        params = [
            ('paymentStatus', 'UNPAID'),
            ('minOutstandingBalance', 0.01),
        ]
        # Add unmatched purchase IDs if provided
        if unmatched_ids:
            for purchase_id in sorted(unmatched_ids):
                params.append(('id', purchase_id))
        else:
            # Default test IDs for manual testing
            params.extend([
                ('id', 352463950),
                ('id', 352462772)
            ])
    print("Params for get_purchases: ", params)

    response = requests.get(url, headers=skybox_headers, params=params, timeout=30)
    data = response.json()
    purchases = data.get('rows', [])
    return purchases

def get_purchase_details(purchase_id: int) -> Optional[Dict]:
    """
    Fetch detailed purchase information including line items from Skybox API
    
    Args:
        purchase_id: The Skybox purchase ID
        
    Returns:
        Purchase details with line items or None if request fails
    """
    url = f'https://skybox.vividseats.com/services/purchases/{purchase_id}'
    skybox_headers = {
        'X-Api-Token': '8293a10f-6546-457c-8644-2b58a753617a',
        'X-Account': '5052',
        'X-Application-Token': '2140c962-2c86-4826-899a-20e6ae8fad31',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=skybox_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching purchase {purchase_id}: {e}")
        return None

def match_purchases_by_description() -> List[Dict]:
    """
    Match Skybox purchases with Reveal Market transactions based on description in range_matches.
    
    More efficient approach: Iterate through Skybox purchases (fewer) and search through 
    Reveal Market transactions (more) for matching descriptions.
    
    Date range logic:
    - Skybox: Uses exact transaction date (e.g., 2026-01-27 to 2026-01-27)
    - Reveal Market: Uses transaction date + 3 days (e.g., 2026-01-27 to 2026-01-30)
      because Reveal Market transactions can take up to 3 days to appear
    
    Returns:
        List of all matching Skybox purchase details with matched Reveal transactions
    """
    all_matches = []
    
    # Fetch Skybox purchases (fewer items, so iterate through these)
    skybox_purchases = get_purchases(is_email_check=False)
    print(f"Found {len(skybox_purchases)} Skybox purchases to process")
    
    # Fetch all Reveal Market transactions (more items, so search through these)
    reveal_transactions = get_unmatched_transactions()
    print(f"Found {len(reveal_transactions)} Reveal Market transactions to search\n")
    count = 1 
    purchase_detail_mapping = {}
    # Process each Skybox purchase
    for purchase in skybox_purchases:
        purchase_id = purchase.get('id')
        print("Purchase Count: ", count, 'Id: ', purchase_id)
        count += 1
        # Get detailed purchase info with line items
        purchase_details = get_purchase_details(purchase_id)
        purchase_detail_mapping[purchase_id] = purchase_details
        if not purchase_details:
            continue
        
        # Check each line item's description
        lines = purchase_details.get('lines', [])
        for line in lines:
            line_description = line.get('description', '')
            
            if not line_description:
                continue
            
            # Search through all Reveal transactions for matching range_match descriptions
            for reveal_transaction in reveal_transactions:
                range_matches = reveal_transaction.get('range_matches', [])
                
                # Skip if no range_matches
                if not range_matches:
                    continue
                
                # Check each range_match
                for range_match in range_matches:
                    range_description = range_match.get('description', '')
                    
                    # Skip if range_match description is empty
                    if not range_description:
                        continue
                    
                    # Check if descriptions match
                    if range_description.strip() == line_description.strip():
                        all_matches.append({
                            'skybox_purchase_id': purchase_id,
                            'skybox_purchase': purchase_details,
                            'matched_line': line,
                            'reveal_transaction_id': reveal_transaction.get('id'),
                            'reveal_amount': reveal_transaction.get('amount'),
                            'reveal_date': reveal_transaction.get('date'),
                            'reveal_description': reveal_transaction.get('description'),
                            'range_match': range_match,
                            'match_type': 'description_exact'
                        })
                        # print(f"Match found: Skybox Purchase {purchase_id} <-> Reveal TX {reveal_transaction.get('id')}")
    print("Purchase Count: ", count)
    
    print(f"\nTotal matches found: {len(all_matches)}")
    return all_matches , purchase_detail_mapping

def get_matched_purchase_ids() -> List[int]:
    """
    Get all unique Skybox purchase IDs where range_match descriptions were found.
    
    Returns:
        List of unique purchase IDs that had matching descriptions
    """
    matches = match_purchases_by_description()
    
    # Extract unique purchase IDs
    matched_purchase_ids = list(set([match['skybox_purchase_id'] for match in matches]))
    matched_purchase_ids.sort()
    
    print(f"\nUnique Skybox Purchase IDs with matches: {len(matched_purchase_ids)}")
    print(f"Purchase IDs: {matched_purchase_ids}")
    
    return matched_purchase_ids

def get_unmatched_purchase_ids() -> List[int]:
    """
    Get all Skybox purchase IDs where descriptions did NOT match any Reveal transaction.
    
    Returns:
        List of purchase IDs that had no matching descriptions
    """
    # Fetch all Skybox purchases
    skybox_purchases = get_purchases(is_email_check=False)
    all_purchase_ids = {purchase.get('id') for purchase in skybox_purchases}
    print(f"Total Skybox purchases: {len(all_purchase_ids)}")
    
    # Get matched purchase IDs
    matches = match_purchases_by_description()
    matched_purchase_ids = {match['skybox_purchase_id'] for match in matches}
    print(f"Matched purchases: {len(matched_purchase_ids)}")
    
    # Find unmatched purchase IDs
    unmatched_purchase_ids = list(all_purchase_ids - matched_purchase_ids)
    unmatched_purchase_ids.sort()
    
    print(f"\nUnmatched Skybox Purchase IDs: {len(unmatched_purchase_ids)}")
    print(f"Unmatched Purchase IDs: {unmatched_purchase_ids}")
    
    return unmatched_purchase_ids

def get_matched_and_unmatched_purchase_ids() -> Dict[str, List[int]]:
    """
    Get both matched and unmatched Skybox purchase IDs in a single function call.
    This is more efficient as it only calls match_purchases_by_description() once.
    
    Returns:
        Dictionary with 'matched' and 'unmatched' lists of purchase IDs
    """
    # Fetch all Skybox purchases
    skybox_purchases = get_purchases(is_email_check=False)
    all_purchase_ids = {purchase.get('id') for purchase in skybox_purchases}
    print(f"Total Skybox purchases: {len(all_purchase_ids)}")
    
    # Get matches (only called once)
    matches, purchase_detail_mapping = match_purchases_by_description()
    
    # Extract matched purchase IDs
    matched_purchase_ids = list({match['skybox_purchase_id'] for match in matches})
    matched_purchase_ids.sort()
    
    # Find unmatched purchase IDs
    unmatched_purchase_ids = list(all_purchase_ids - set(matched_purchase_ids))
    unmatched_purchase_ids.sort()
    
    # Print summary
    print(f"\nUnique Skybox Purchase IDs with matches: {len(matched_purchase_ids)}")
    print(f"Matched Purchase IDs: {matched_purchase_ids}")
    
    print(f"\nUnmatched Skybox Purchase IDs: {len(unmatched_purchase_ids)}")
    print(f"Unmatched Purchase IDs: {unmatched_purchase_ids}")
    
    # Filter unmatched purchases by payment method ACH
    ach_unmatched = []
    print(f"\n{'='*80}")
    print("UNMATCHED PURCHASES WITH PAYMENT METHOD: ACH")
    print(f"{'='*80}")
    

    print(f"{'='*80}")
    
    return purchase_detail_mapping,{
        'matched': matched_purchase_ids,
        'unmatched': unmatched_purchase_ids,
        'ach_unmatched': [item['id'] for item in ach_unmatched],
        'ach_unmatched_details': ach_unmatched,
        'total_purchases': len(all_purchase_ids),
        'total_matches': len(matches)
    }

# Run the combined function
print("="*80)
print("GETTING MATCHED AND UNMATCHED PURCHASE IDs")
print("="*80)
purchase_detail_mapping, result = get_matched_and_unmatched_purchase_ids()
# print(f"\n{'='*80}")
import ipdb; ipdb.set_trace()
print(f"SUMMARY:")
print(f"Total Purchases: {result['total_purchases']}")
print(f"Matched Purchases: {len(result['matched'])}")
print(f"Unmatched Purchases: {len(result['unmatched'])}")
print(f"Unmatched ACH Purchases: {len(result['ach_unmatched'])}")
print(f"Total Match Records: {result['total_matches']}")
print(f"{'='*80}")

# ============================================================================
# FAST EMAIL SEARCH - Uses server-side IMAP search (INSTANT results!)
# ============================================================================

# Search for order number across all folders
# emails = search_all_folders_fast('130696091')

# # Display results
# if emails:
#     print(f"\n{'='*80}")
#     print(f"📧 FOUND {len(emails)} EMAIL(S):")
#     print(f"{'='*80}")
    
#     for i, email_data in enumerate(emails, 1):
#         print(f"\n{i}. Subject: {email_data['subject']}")
#         print(f"   From: {email_data['sender']}")
#         print(f"   Date: {email_data['date_str']}")
#         print(f"   Folder: {email_data['folder']}")
#         print(f"   Body preview:\n   {email_data['body'][:400]}...")
#         print(f"\n   {'─'*76}")
# else:
#     print("\n✗ No emails found with '130696091'")