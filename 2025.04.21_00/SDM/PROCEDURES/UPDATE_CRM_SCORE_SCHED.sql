--liquibase formatted.sql
--changeset michael.cawayan:SDM.UPDATE_CRM_SCORE_SCHED contextFilter:PH endDelimiter:/ runOnChange:true

BEGIN
  DBMS_SCHEDULER.CREATE_SCHEDULE (
    schedule_name   => 'SDM.UPDATE_CRM_SCORE_DAILY',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY; BYHOUR=11; BYMINUTE=10',
    comments        => 'Will run every 11:10 everyday'
  );
  DBMS_SCHEDULER.CREATE_PROGRAM (
    program_name   => 'SDM.UPDATE_CRM_SCORE_PROG',
    program_type   => 'STORED_PROCEDURE',
    program_action => 'SDM.UPDATE_CRM_SCORE',
    enabled        => TRUE
  );
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'SDM.UPDATE_CRM_SCORE_JOB',
    program_name    => 'SDM.UPDATE_CRM_SCORE_PROG',
    schedule_name   => 'SDM.UPDATE_CRM_SCORE_DAILY',
    enabled         => TRUE
  );
END;
/