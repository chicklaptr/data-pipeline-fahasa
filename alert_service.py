import os
import smtplib
import json
from minio import Minio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#cấu hình dịch vụ gửi mail
SMTP_HOST=os.getenv("SMTP_HOST")
SMTP_PORT=int(os.getenv("SMTP_PORT","587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
ALERT_EMAIL_TO =os.getenv("ALERT_EMAIL_TO")
#cấu hình minio để đọc processed file lấy đúng process_run_id
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT","minio:9000")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET=os.getenv("MINIO_BUCKET","data-lake")

#hàm gửi mail
def send_email_alert(subject,body):
    if not all([SMTP_HOST,SMTP_PORT,SMTP_USER,SMTP_PASSWORD,ALERT_EMAIL_TO]):
        raise ValueError("Missing SMTP configuration")
    
    #Tiêu đề email
    msg=MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"]=ALERT_EMAIL_TO
    msg["Subject"]=subject
    msg.attach(MIMEText(body,"plain","utf-8"))
    
    with smtplib.SMTP(SMTP_HOST,SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER,SMTP_PASSWORD)
        server.sendmail(SMTP_USER,ALERT_EMAIL_TO,msg.as_string())

#hàm lấy process_run_id
def get_process_run_id_from_processed_file(processed_file_minio:str):
    client=Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    response=None
    try:
        response=client.get_object(MINIO_BUCKET,processed_file_minio)
        processed_data=json.loads(response.read())
        process_metadata=processed_data.get("process_metadata",{})
        process_run_id=process_metadata.get("process_run_id")
        
        if not process_run_id:
            raise ValueError("Missing process_run_id in processed file")
        
        return process_run_id
    finally:
        if response is not None:
            response.close()
            response.release_conn()
            
            
#hàm kiểm tra pipeline xem có lỗi gì để alert ko
def build_alert_message(process_run_id,quality_status,load_status,rejected_count,failure_rate):
    alerts=[]
    
    if quality_status == "failed":
        alerts.append(("CRITICAL","QUALITY_GATE_FAILED",
                       f"Quality gate failed for process_run_id={process_run_id}"))
    if load_status=="failed":
        alerts.append(("CRITICAL","LOAD_FAILED",
                       f"Load step failed for process_run_id={process_run_id}"))
    if rejected_count >50:
        alerts.append(("WARNING","HIGH_REJECTED_COUNT",
                       f"Rejected records too high: {rejected_count} for process_run_id={process_run_id}"))
    if failure_rate > 5:
        alerts.append(("WARNING","HIGH_FAILURE_RATE",
                       f"Failure rate too high: {failure_rate:.2f} for process_run_id={process_run_id}"))
        
    return alerts

#hàm tạo định dạng tin nhắn để gửi
def format_email_body(process_run_id,alerts,quality_status,load_status,rejected_count,failure_rate):
    lines=[
        f"Pipeline Alert for process_run_id: {process_run_id}",
        "",
        f"Quality gate status: {quality_status}",
        f"Load status: {load_status}",
        f"Rejected records: {rejected_count}",
        f"Failure_rate: {failure_rate}",
        "",
        "Triggered alerts:"
    ]
    
    for level,alert_type,message in alerts:
        lines.append(f"- [{level}] {alert_type}: {message}")
    
    lines.append("")
    lines.append("Please check pipeline_task_audit and data_quality_reports for more details.")
    
    return "\n".join(lines)

#hàm để chèn alerts lưu trữ
def insert_alert(conn,process_run_id,alert_level,alert_type,message):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_alerts(
                process_run_id,
                alert_level,
                alert_type,
                message
            )
            VALUES (%s,%s,%s,%s)
            """,(
                process_run_id,
                alert_level,
                alert_type,
                message
            ))
    conn.commit()

#hàm logic tổng
def check_and_send_alert(conn,process_run_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
            products_failed,
            total_products_seen
            FROM data_quality_reports
            WHERE process_run_id = %s
            LIMIT 1;    
                """,(process_run_id,))
        quality_row=cur.fetchone()
        cur.execute("""
            SELECT status
            FROM pipeline_task_audit
            WHERE process_run_id=%s
            AND task_name = 'process_data'
            ORDER BY started_at DESC
            LIMIT 1;
                    """,(process_run_id,))
        process_row=cur.fetchone()
        
        cur.execute("""
            SELECT status
            FROM pipeline_task_audit
            WHERE process_run_id=%s AND task_name='load_data'
            ORDER BY started_at DESC
            LIMIT 1;    
                """,(process_run_id,))
        load_row=cur.fetchone()
        
        rejected_count = quality_row[0] if quality_row else 0
        total = quality_row[1] if quality_row else 0
        failure_rate = (rejected_count / total * 100) if total else 0
        
        quality_status = process_row[0] if process_row else "unknown"
        load_status =load_row[0] if load_row else "unknown"
        
        alerts = build_alert_message(
            process_run_id=process_run_id,
            quality_status=quality_status,
            load_status=load_status,
            rejected_count=rejected_count,
            failure_rate=failure_rate
        )
        if not alerts:
            return
        for level,alert_type,message in alerts:
            insert_alert(conn,process_run_id,level,alert_type,message)
        
        #tiêu đề
        subject=f"[PIPELINE ALERT] Run {process_run_id} has issues"
        #nội dung
        body=format_email_body(
            process_run_id=process_run_id,
            alerts=alerts,
            quality_status=quality_status,
            load_status=load_status,
            rejected_count=rejected_count,
            failure_rate=failure_rate
        )
        
        send_email_alert(subject,body)

    