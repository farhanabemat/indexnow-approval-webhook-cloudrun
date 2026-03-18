"""
IndexNow Approval Webhook - Google Cloud Run
=============================================
Receives approval/rejection from Power Automate and records to BigQuery.

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

app = Flask(__name__)


def get_bq_client():
    """Get BigQuery client - uses default credentials in Cloud Run"""
    return bigquery.Client(project=BQ_PROJECT_ID)


def record_approval(run_date, language, action, responder, notes=None):
    """Record approval/rejection to BigQuery"""
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
        responder = data.get('responder', 'Unknown')
        notes = data.get('notes', '')
        
        # Validate required fields
        if not all([run_date, language, action]):
            return jsonify({
                "error": "Missing required fields",
                "required": ["run_date", "language", "action"]
            }), 400
        
        # Record to BigQuery
        success, message = record_approval(run_date, language, action, responder, notes)
        
        if success:
            return jsonify({
                "success": True,
                "message": message,
                "run_date": run_date,
                "language": language,
                "status": "approved" if action.lower() in ["approve", "approved"] else "rejected",
                "approved_by": responder
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": message
            }), 500
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "indexnow-approval-webhook"}), 200


@app.route('/', methods=['GET'])
def home():
    """Home page with usage info"""
    return jsonify({
        "service": "IndexNow Approval Webhook",
        "version": "1.0.0",
        "endpoints": {
            "POST /approval": "Record approval/rejection from Power Automate",
            "GET /health": "Health check"
        },
        "example_payload": {
            "run_date": "2026-03-18",
            "language": "en-us",
            "action": "approve",
            "responder": "John Doe",
            "notes": "Approved after review"
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
