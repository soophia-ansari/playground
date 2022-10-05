WITH tmp AS
(
  SELECT pm1.*,
         pm2.event_params AS username,
         pm3.load_time,
		pm4.pk_session_id as export_session         
  FROM (SELECT *
        FROM audit.portal_metrics
        WHERE event_name = 'report-tree-click') pm1
    LEFT JOIN (SELECT *
               FROM audit.portal_metrics
               WHERE event_name = 'login_click') pm2 ON pm1.pk_session_id = pm2.pk_session_id
    LEFT JOIN (SELECT pk_session_id,
                      pk_report_id,
                      AVG(event_params::INT) load_time
               FROM audit.portal_metrics
               WHERE event_name = 'raw_report_load'
               GROUP BY pk_report_id,
                        pk_session_id) pm3
           ON pm1.pk_session_id = pm3.pk_session_id
          AND pm1.pk_report_id = pm3.pk_report_id
    left join (select pk_report_id,
    				pk_session_id, 
    				title, 
    				export_type,
    				pk_user_id
    			from web2.portal_report_export
    			where status = 'SUCCESS') pm4
    			on pm1.pk_session_id = pm4.pk_session_id
    			and pm1.pk_report_id = pm3.pk_report_id
)
SELECT load_ts,
       tmp.pk_report_id,
       tmp.event_name,
       pr.report_type,
       pr.display_report_name,
       tmp.org_id,
       pk_session_id,
       username,
       COUNT(DISTINCT pk_session_id) click_count,
       COUNT(DISTINCT username) user_count,
       COUNT (distinct export_session) export_count,
       AVG(load_time) avg_load_time from tmp 
       left JOIN web2.portal_report pr ON pr.pk_report_id = tmp.pk_report_id where display_report_name IS NOT NULL AND username NOT LIKE '%carejourney%' GROUP BY tmp.pk_report_id,
       pr.report_type,
       tmp.event_name,
       pr.display_report_name,
       tmp.org_id,
       tmp.load_ts,
       pk_session_id,
       username order BY load_ts DESC