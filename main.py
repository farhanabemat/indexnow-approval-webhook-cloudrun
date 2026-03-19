"""
IndexNow Approval Webhook - Google Cloud Run
=============================================
Receives approval/rejection from Power Automate and records to BigQuery.
Also updates indexnow_queue status and triggers URL processing.

Deploy to Cloud Run:
  gcloud run deploy indexnow-approval-webhook --source . --region us-central1 --allow-unauthenticated
"""

import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Configuration
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "merck-466618")
BQ_DATASET = os.environ.get("BQ_DATASET", "qa_automation")
INDEXNOW_APPROVALS_TABLE = os.environ.get("INDEXNOW_APPROVALS_TABLE", "indexnow_approvals")
INDEXNOW_QUEUE_TABLE = os.environ.get("INDEXNOW_QUEUE_TABLE", "indexnow_queue")
URL_CHANGES_TABLE = os.environ.get("URL_CHANGES_TABLE", "url_changes")

app = Flask(__name__)


def get_bq_client():
    """Get BigQuery client - uses default credentials in Cloud Run"""
    return bigquery.Client(project=BQ_PROJECT_ID)


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
    """Handle approval webhook from Power Automate"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Extract fields
        run_date = data.get('run_date')
        language = data.get('language')
        action = data.get('action')
        responder = data.get('approved_by', data.get('responder', 'Unknown'))
        notes = data.get('notes', '')
        
        # Validate required fields
        if not all([run_date, language, action]):
            return jsonify({
                "error": "Missing required fields",
                "required": ["run_date", "language", "action"]
            }), 400
        
        status = "approved" if action.lower() in ["approve", "approved"] else "rejected"
        
        # 1. Record to indexnow_approvals table
        record_success, record_msg = record_approval(run_date, language, action, responder, notes)
        
        # 2. Update indexnow_queue status
        queue_success, queue_msg = update_queue_status(language, action, responder, run_date)
        
        # 3. If rejected, also update url_changes
        if status == "rejected":
            update_url_changes_status(language, "rejected", run_date)
        
        # 4. Get count of URLs ready for processing (if approved)
        urls_ready = 0
        if status == "approved":
            urls_ready = get_queue_count(language, "approved")
        
        if record_success:
            response_data = {
                "success": True,
                "message": f"Recorded {status} for {language}",
                "run_date": run_date,
                "language": language,
                "status": status,
                "approved_by": responder,
                "queue_updated": queue_success,
            }
            
            if status == "approved":
                response_data["urls_ready_for_submission"] = urls_ready
                response_data["next_step"] = "Run process_approved_indexnow.py to submit URLs to IndexNow"
            
            return jsonify(response_data), 200
        else:
            return jsonify({
                "success": False,
                "error": record_msg
            }), 500
            
    except Exception as e:
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
        "version": "2.0.0",
        "endpoints": {
            "POST /approval": "Record approval/rejection and update queue status",
            "POST /process": "Check approved URLs ready for processing",
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
            "1. sitemap_compare.py queues URLs needing approval to indexnow_queue",
            "2. Teams notification sent to approvers",
            "3. Approver clicks Approve/Reject in Teams",
            "4. Power Automate calls POST /approval",
            "5. Queue status updated, approval recorded",
            "6. Run process_approved_indexnow.py to submit approved URLs"
        ]
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
