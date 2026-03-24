"""
IndexNow Approval Webhook - Google Cloud Run
=============================================
Receives approval/rejection from Power Automate and records to BigQuery.
On approval: Fetches URLs from queue, submits to IndexNow, logs results.
On rejection: Updates status and logs the rejection.

Deploy to Cloud Run:
  gcloud run deploy indexnow-approval-webhook --source . --region us-central1 --allow-unauthenticated
"""

import os
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Configuration
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "merck-466618")
BQ_DATASET = os.environ.get("BQ_DATASET", "qa_automation")
INDEXNOW_APPROVALS_TABLE = os.environ.get("INDEXNOW_APPROVALS_TABLE", "indexnow_approvals")
INDEXNOW_QUEUE_TABLE = os.environ.get("INDEXNOW_QUEUE_TABLE", "indexnow_queue")
INDEXNOW_LOG_TABLE = os.environ.get("INDEXNOW_LOG_TABLE", "IndexNow_log_report")
URL_CHANGES_TABLE = os.environ.get("URL_CHANGES_TABLE", "url_changes")
INDEXNOW_API_TABLE = os.environ.get("INDEXNOW_API_TABLE", "indexnow_api")
INDEXNOW_BATCH_SIZE = 10000  # Max URLs per IndexNow request
SCRIPT_NAME = "indexnow-approval-webhook"

app = Flask(__name__)


def get_bq_client():
    """Get BigQuery client - uses default credentials in Cloud Run"""
    return bigquery.Client(project=BQ_PROJECT_ID)


def normalize_run_date(value):
    """Normalize any date string to YYYY-MM-DD for BigQuery DATE fields.
    
    Handles formats sent by Power Automate and sitemap_compare.py:
      - YYYY-MM-DD           (already correct)
      - DD-MM-YYYY
      - DD-MM-YYYY_HH-MM-SS  (sitemap folder format)
      - YYYY-MM-DDTHH:MM:SS  (ISO format)
    Returns None if value is missing or unparseable.
    """
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%m-%Y_%H-%M-%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value.split("T")[0] if "T" in value else value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    print(f"WARNING: Could not normalize run_date '{value}' - will query without date filter")
    return None


def get_indexnow_credentials():
    """Fetch IndexNow API credentials from BigQuery, keyed by host"""
    try:
        client = get_bq_client()
        query = f"""
        SELECT api_key, host, key_location
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_API_TABLE}`
        """
        results = client.query(query).result()
        
        creds = {}
        for row in results:
            creds[row.host] = {
                'api_key': row.api_key,
                'host': row.host,
                'key_location': row.key_location
            }
        print(f"Loaded IndexNow credentials for {len(creds)} hosts")
        return creds
    except Exception as e:
        print(f"Error fetching IndexNow credentials: {e}")
        return {}


def extract_hostname(url):
    """Extract hostname from URL"""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except:
        return ""


def get_queued_urls(language, run_date=None):
    """Get URLs from indexnow_queue for a language.
    
    If run_date is provided and returns 0 rows, falls back to language-only query
    so that cross-day approvals (queue inserted Mar 19, approved Mar 24) still work.
    """
    try:
        client = get_bq_client()
        base_query = f"""
        SELECT url, change_type, first_response, final_response, has_redirects, run_date
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_QUEUE_TABLE}`
        WHERE language = @language
        """

        def _run_query(extra_clause, extra_params):
            params = [bigquery.ScalarQueryParameter("language", "STRING", language)] + extra_params
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            results = client.query(base_query + extra_clause, job_config=job_config).result()
            urls = []
            for row in results:
                urls.append({
                    'url': row.url,
                    'change_type': row.change_type,
                    'first_response': row.first_response,
                    'final_response': row.final_response,
                    'has_redirects': row.has_redirects,
                    'run_date': str(row.run_date)
                })
            return urls

        # Try with run_date filter first
        if run_date:
            urls = _run_query(
                " AND run_date = @run_date",
                [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
            )
            if urls:
                print(f"Found {len(urls)} URLs in queue for {language} (run_date={run_date})")
                return urls
            # Fallback: no date filter (handles cross-day approvals)
            print(f"No URLs found for {language} with run_date={run_date}, falling back to language-only query")

        urls = _run_query("", [])
        print(f"Found {len(urls)} URLs in queue for {language} (no date filter)")
        return urls

    except Exception as e:
        print(f"Error getting queued URLs: {e}")
        return []


def submit_to_indexnow_batch(urls, credentials):
    """Submit URLs to IndexNow API in batches, grouped by host"""
    if not urls or not credentials:
        return [], 0, 0
    
    timestamp = datetime.utcnow().isoformat()
    log_entries = []
    success_count = 0
    fail_count = 0
    
    # Group URLs by host
    by_host = {}
    for url_info in urls:
        url = url_info['url']
        host = extract_hostname(url)
        if host in credentials:
            by_host.setdefault(host, []).append(url_info)
        else:
            # No credentials for this host
            log_entries.append({
                "date_submitted": timestamp,
                "URL": url,
                "keylocation": "",
                "host": host,
                "indexnow_response": "Skipped",
                "note": f"No credentials for {host}",
                "script_name": SCRIPT_NAME
            })
            fail_count += 1
    
    # Submit in batches per host
    for host, items in by_host.items():
        creds = credentials[host]
        
        for i in range(0, len(items), INDEXNOW_BATCH_SIZE):
            chunk = items[i:i+INDEXNOW_BATCH_SIZE]
            url_list = [item['url'] for item in chunk]
            
            payload = {
                "host": creds['host'],
                "key": creds['api_key'],
                "keyLocation": creds['key_location'],
                "urlList": url_list
            }
            
            try:
                response = requests.post(
                    "https://api.indexnow.org/indexnow",
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=30
                )
                code = response.status_code
                success = code in (200, 202)
            except requests.exceptions.Timeout:
                code = "Timeout"
                success = False
            except Exception as e:
                code = f"Error: {str(e)[:50]}"
                success = False
            
            # Log each URL
            for item in chunk:
                log_entries.append({
                    "date_submitted": timestamp,
                    "URL": item['url'],
                    "keylocation": creds['key_location'],
                    "host": host,
                    "indexnow_response": str(code),
                    "note": f"Approved batch submission - {item.get('change_type', 'unknown')}",
                    "script_name": SCRIPT_NAME
                })
                if success:
                    success_count += 1
                else:
                    fail_count += 1
            
            print(f"Submitted batch of {len(chunk)} URLs to {host}: {code}")
    
    return log_entries, success_count, fail_count


def save_indexnow_log(log_entries):
    """Save IndexNow log entries to BigQuery"""
    if not log_entries:
        return True
    
    try:
        client = get_bq_client()
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_LOG_TABLE}"
        
        errors = client.insert_rows_json(table_id, log_entries)
        
        if errors:
            print(f"Error saving IndexNow log: {errors}")
            return False
        
        print(f"Saved {len(log_entries)} entries to IndexNow_log_report")
        return True
    except Exception as e:
        print(f"Error saving IndexNow log: {e}")
        return False


def delete_from_queue(language, run_date=None):
    """Delete processed URLs from indexnow_queue.
    
    If run_date is provided, deletes only that date's rows.
    Falls back to language-only delete if streaming buffer prevents date-filtered delete.
    """
    try:
        client = get_bq_client()

        def _delete(extra_clause, extra_params):
            params = [bigquery.ScalarQueryParameter("language", "STRING", language)] + extra_params
            query = f"""
            DELETE FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_QUEUE_TABLE}`
            WHERE language = @language{extra_clause}
            """
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            client.query(query, job_config=job_config).result()

        if run_date:
            try:
                _delete(
                    " AND run_date = @run_date",
                    [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
                )
                print(f"Deleted {language} (run_date={run_date}) from queue")
                return True
            except Exception as e:
                if "streaming buffer" in str(e):
                    print(f"Streaming buffer active for {language} - queue rows will remain until buffer flushes (~90 min)")
                    return False
                raise  # re-raise non-buffer errors

        _delete("", [])
        print(f"Deleted all {language} URLs from queue")
        return True

    except Exception as e:
        if "streaming buffer" in str(e):
            print(f"Cannot delete {language} from queue yet (streaming buffer - normal for recent inserts)")
        else:
            print(f"Error deleting from queue: {e}")
        return False


def record_approval(run_date, language, action, responder, notes=None):
    """Record approval/rejection to BigQuery indexnow_approvals table"""
    try:
        client = get_bq_client()
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_APPROVALS_TABLE}"
        
        # Map action to status
        status = "approved" if action.lower() in ["approve", "approved"] else "rejected"
        
        rows_to_insert = [{
            "run_date": run_date,
            "language": language,
            "status": status,
            "approved_by": responder,
            "approved_at": datetime.utcnow().isoformat(),
            "notes": notes or "",
            "source": "teams_workflow"
        }]
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f"BigQuery insert errors: {errors}")
            return False, str(errors)
        
        print(f"Recorded {status} for {language} (run_date: {run_date}) by {responder}")
        return True, f"Recorded {status} for {language}"
        
    except Exception as e:
        print(f"Error recording approval: {str(e)}")
        return False, str(e)


def update_queue_status(language, action, responder, run_date=None):
    """Update indexnow_queue status for all pending URLs of a language"""
    try:
        client = get_bq_client()
        status = "approved" if action.lower() in ["approve", "approved"] else "rejected"
        
        query = f"""
        UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_QUEUE_TABLE}`
        SET status = @status,
            approved_by = @approved_by
        WHERE language = @language 
          AND status = 'pending_approval'
        """
        
        params = [
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("approved_by", "STRING", responder),
            bigquery.ScalarQueryParameter("language", "STRING", language),
        ]
        
        if run_date:
            query += " AND run_date = @run_date"
            params.append(bigquery.ScalarQueryParameter("run_date", "DATE", run_date))
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = client.query(query, job_config=job_config).result()
        
        print(f"Updated indexnow_queue: {language} -> {status}")
        return True, f"Queue updated to {status}"
        
    except Exception as e:
        print(f"Error updating queue: {str(e)}")
        return False, str(e)


def update_url_changes_status(language, new_status, run_date=None):
    """Update submission_status in url_changes for rejected URLs"""
    try:
        client = get_bq_client()
        
        query = f"""
        UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET}.{URL_CHANGES_TABLE}`
        SET submission_status = @new_status
        WHERE language = @language 
          AND submission_status = 'pending_approval'
        """
        
        params = [
            bigquery.ScalarQueryParameter("new_status", "STRING", new_status),
            bigquery.ScalarQueryParameter("language", "STRING", language),
        ]
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        client.query(query, job_config=job_config).result()
        
        print(f"Updated url_changes submission_status: {language} -> {new_status}")
        return True
        
    except Exception as e:
        print(f"Error updating url_changes: {str(e)}")
        return False


def get_queue_count(language, status="approved"):
    """Get count of URLs in queue for a language with given status"""
    try:
        client = get_bq_client()
        
        query = f"""
        SELECT COUNT(*) as count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{INDEXNOW_QUEUE_TABLE}`
        WHERE language = @language AND status = @status
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("language", "STRING", language),
                bigquery.ScalarQueryParameter("status", "STRING", status),
            ]
        )
        
        result = client.query(query, job_config=job_config).result()
        for row in result:
            return row.count
        return 0
        
    except Exception as e:
        print(f"Error getting queue count: {str(e)}")
        return 0


@app.route('/approval', methods=['POST'])
def handle_approval():
    """Handle approval webhook from Power Automate
    
    On approval: Fetches URLs from queue, submits to IndexNow, logs results
    On rejection: Records rejection and updates status
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Extract fields
        run_date_raw = data.get('run_date')
        run_date = normalize_run_date(run_date_raw)   # always YYYY-MM-DD or None
        language = data.get('language')
        action = data.get('action')
        responder = data.get('approved_by', data.get('responder', 'Unknown'))
        notes = data.get('notes', '')
        
        print(f"Received: run_date_raw={run_date_raw!r} -> normalized={run_date!r}, language={language!r}, action={action!r}")
        
        # Validate required fields
        if not all([language, action]):
            return jsonify({
                "error": "Missing required fields",
                "required": ["language", "action"],
                "note": "run_date is optional - if missing, language-only queue lookup is used"
            }), 400
        
        status = "approved" if action.lower() in ["approve", "approved"] else "rejected"
        print(f"Processing {status} for {language} (run_date: {run_date}) by {responder}")
        
        # 1. Record to indexnow_approvals table
        record_success, record_msg = record_approval(run_date, language, action, responder, notes)
        
        # Initialize response data
        response_data = {
            "success": record_success,
            "run_date": run_date,
            "language": language,
            "status": status,
            "approved_by": responder,
        }
        
        if status == "approved":
            # APPROVAL FLOW: Submit to IndexNow immediately
            print(f"Starting IndexNow submission for {language}...")
            
            # Get IndexNow credentials
            credentials = get_indexnow_credentials()
            if not credentials:
                response_data["error"] = "Failed to load IndexNow credentials"
                return jsonify(response_data), 500
            
            # Get URLs from queue
            urls = get_queued_urls(language, run_date)
            if not urls:
                response_data["message"] = f"No URLs found in queue for {language}"
                response_data["urls_submitted"] = 0
                return jsonify(response_data), 200
            
            # Submit to IndexNow
            log_entries, success_count, fail_count = submit_to_indexnow_batch(urls, credentials)
            
            # Save log to BigQuery
            log_saved = save_indexnow_log(log_entries)
            
            # Try to delete from queue (may fail if in streaming buffer)
            queue_deleted = delete_from_queue(language, run_date)
            
            # Update url_changes status
            update_url_changes_status(language, "submitted", run_date)
            
            response_data.update({
                "message": f"Submitted {success_count} URLs to IndexNow for {language}",
                "urls_submitted": success_count,
                "urls_failed": fail_count,
                "total_urls": len(urls),
                "log_saved": log_saved,
                "queue_deleted": queue_deleted
            })
            
            print(f"Completed: {success_count} submitted, {fail_count} failed for {language}")
            
        else:
            # REJECTION FLOW: Just update status
            update_url_changes_status(language, "rejected", run_date)
            
            # Try to delete from queue
            delete_from_queue(language, run_date)
            
            response_data["message"] = f"Rejected {language} - URLs will not be submitted"
            print(f"Rejected {language}")
        
        return jsonify(response_data), 200
            
    except Exception as e:
        print(f"Error in handle_approval: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/process', methods=['POST'])
def trigger_processing():
    """
    Trigger processing of approved URLs for a language.
    This endpoint can be called after approval to start IndexNow submissions.
    
    Note: For full processing, the process_approved_indexnow.py script should be
    deployed as a separate Cloud Run job or Cloud Function.
    """
    try:
        data = request.get_json() or {}
        language = data.get('language')
        
        # Get count of approved URLs
        if language:
            count = get_queue_count(language, "approved")
            return jsonify({
                "message": f"Found {count} approved URLs for {language}",
                "language": language,
                "urls_pending": count,
                "note": "Run process_approved_indexnow.py --language {language} to submit these URLs"
            }), 200
        else:
            return jsonify({
                "error": "Language parameter required",
                "usage": "POST /process with {\"language\": \"en-us\"}"
            }), 400
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "indexnow-approval-webhook"}), 200


@app.route('/', methods=['GET'])
def home():
    """Home page with usage info"""
    return jsonify({
        "service": "IndexNow Approval Webhook",
        "version": "3.0.0",
        "description": "Handles IndexNow approval workflow - submits URLs on approval",
        "endpoints": {
            "POST /approval": "Process approval/rejection - submits to IndexNow on approval",
            "POST /process": "Check URLs pending in queue",
            "GET /health": "Health check"
        },
        "example_approval_payload": {
            "run_date": "2026-03-18",
            "language": "en-us",
            "action": "approved",
            "approved_by": "john.doe@company.com",
            "notes": "Approved after review"
        },
        "workflow": [
            "1. sitemap_compare.py detects changes and queues URLs to indexnow_queue",
            "2. If missing URLs > threshold, Teams approval card is sent",
            "3. Approver clicks Approve/Reject in Teams",
            "4. Power Automate calls POST /approval",
            "5. On APPROVAL: URLs submitted to IndexNow API immediately, logged to IndexNow_log_report",
            "6. On REJECTION: Status updated, URLs not submitted"
        ],
        "tables_updated": {
            "indexnow_approvals": "Records approval/rejection with timestamp and approver",
            "IndexNow_log_report": "Logs all IndexNow API submissions",
            "url_changes": "Updates submission_status to 'submitted' or 'rejected'"
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
