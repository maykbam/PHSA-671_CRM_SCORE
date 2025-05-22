--liquibase formatted.sql
--changeset michael.cawayan:SDM.UPDATE_CRM_SCORE contextFilter:PH endDelimiter:/ runOnChange:true

CREATE OR REPLACE PROCEDURE SDM.UPDATE_CRM_SCORE IS
    v_new_dates        SYS_REFCURSOR;
    v_date1            DATE;
    v_date2            DATE;
    v_latest_date      DATE;
    v_current_date_a   DATE;
    v_latest_date_a    DATE;
    v_latest_date_b    DATE;
    v_target_table     VARCHAR2(30);
    v_sql              CLOB;
    v_last_job_status  VARCHAR2(20);
    v_last_run_date    DATE;
BEGIN
    -- Step 1: Fetch latest available date from view
    LOOP
    SELECT MAX(DTIME_INSERTED)
    INTO v_date1
    FROM SDM_ETL.V_CRM_SCORE
    WHERE ROWNUM = 1;

    v_latest_date := v_date1;

    EXIT WHEN TRUNC(v_latest_date) = TRUNC(SYSDATE);

    DBMS_SESSION.SLEEP(300);
END LOOP;

    -- Step 2: Get current config date for CRM_SCORE_A
    BEGIN
        SELECT DTIME_INSERTED INTO v_current_date_a
        FROM SDM.CRM_SCORE_CONFIG
        WHERE TABLE_NAME = 'CRM_SCORE_A';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_current_date_a := NULL;
            -- Insert initial rows for both CRM_SCORE_A and CRM_SCORE_B
            INSERT ALL 
                INTO SDM.CRM_SCORE_CONFIG (TABLE_NAME, DTIME_INSERTED, FLAG_ACTIVE)
                VALUES ('CRM_SCORE_A', NULL, NULL)
                INTO SDM.CRM_SCORE_CONFIG (TABLE_NAME, DTIME_INSERTED, FLAG_ACTIVE)
                VALUES ('CRM_SCORE_B', NULL, NULL)
            SELECT * FROM DUAL;

            COMMIT;
    END;

    -- If NULL, initialize config with older date
    IF v_current_date_a IS NULL THEN
        v_current_date_a := v_latest_date - 1;

        UPDATE SDM.CRM_SCORE_CONFIG
        SET DTIME_INSERTED = v_current_date_a
        WHERE TABLE_NAME = 'CRM_SCORE_A';

        COMMIT;
    END IF;

    IF v_latest_date > v_current_date_a THEN
        -- 1. Copy CRM_SCORE_A to CRM_SCORE_B
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SDM.CRM_SCORE_B';
        INSERT INTO SDM.CRM_SCORE_B 
        (ID_CUID,
        CRM_SCORE_1,
        CRM_SCORE_2,
        CRM_SCORE_3,
        CRM_SCORE_4,
        CRM_SCORE_5,
        CRM_SCORE_6,
        CRM_SCORE_7,
        CRM_SCORE_8,
        CRM_SCORE_9,
        CRM_SCORE_10,
        INSUPD_DT_1,
        INSUPD_DT_2,
        INSUPD_DT_3,
        INSUPD_DT_4,
        INSUPD_DT_5,
        INSUPD_DT_6,
        INSUPD_DT_7,
        INSUPD_DT_8,
        INSUPD_DT_9,
        INSUPD_DT_10,
        DTIME_INSERTED, 
        DTIME_UPDATED)
        SELECT 
        ID_CUID,
        CRM_SCORE_1,
        CRM_SCORE_2,
        CRM_SCORE_3,
        CRM_SCORE_4,
        CRM_SCORE_5,
        CRM_SCORE_6,
        CRM_SCORE_7,
        CRM_SCORE_8,
        CRM_SCORE_9,
        CRM_SCORE_10,
        INSUPD_DT_1,
        INSUPD_DT_2,
        INSUPD_DT_3,
        INSUPD_DT_4,
        INSUPD_DT_5,
        INSUPD_DT_6,
        INSUPD_DT_7,
        INSUPD_DT_8,
        INSUPD_DT_9,
        INSUPD_DT_10,
        DTIME_INSERTED, 
        DTIME_UPDATED
        FROM SDM.CRM_SCORE_A;
        COMMIT;

        -- Switch view to CRM_SCORE_B
        EXECUTE IMMEDIATE 'CREATE OR REPLACE EDITIONABLE VIEW SDM.V_CRM_SCORE AS SELECT * FROM SDM.CRM_SCORE_B';

        -- Truncate CRM_SCORE_A
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SDM.CRM_SCORE_A';

        -- Insert new records from view
        INSERT INTO SDM.CRM_SCORE_A 
        (ID_CUID,
CRM_SCORE_1,
CRM_SCORE_2,
CRM_SCORE_3,
CRM_SCORE_4,
CRM_SCORE_5,
CRM_SCORE_6,
CRM_SCORE_7,
CRM_SCORE_8,
CRM_SCORE_9,
CRM_SCORE_10,
INSUPD_DT_1,
INSUPD_DT_2,
INSUPD_DT_3,
INSUPD_DT_4,
INSUPD_DT_5,
INSUPD_DT_6,
INSUPD_DT_7,
INSUPD_DT_8,
INSUPD_DT_9,
INSUPD_DT_10,
DTIME_INSERTED, 
DTIME_UPDATED)
        SELECT 
        ID_CUID,
CRM_SCORE_1,
CRM_SCORE_2,
CRM_SCORE_3,
CRM_SCORE_4,
CRM_SCORE_5,
CRM_SCORE_6,
CRM_SCORE_7,
CRM_SCORE_8,
CRM_SCORE_9,
CRM_SCORE_10,
INSUPD_DT_1,
INSUPD_DT_2,
INSUPD_DT_3,
INSUPD_DT_4,
INSUPD_DT_5,
INSUPD_DT_6,
INSUPD_DT_7,
INSUPD_DT_8,
INSUPD_DT_9,
INSUPD_DT_10,
DTIME_INSERTED, 
DTIME_UPDATED
FROM SDM_ETL.V_CRM_SCORE --SDM_ETL.V_CRM_SCORE  
        WHERE DTIME_INSERTED = v_latest_date;
        COMMIT;

        -- 4. Update CRM_SCORE_CONFIG with new date for correct table
        v_target_table := 'CRM_SCORE_A';

        MERGE INTO SDM.CRM_SCORE_CONFIG cfg
        USING (
            SELECT v_target_table AS table_name, v_latest_date AS DTIME_INSERTED FROM dual
        ) src
        ON (cfg.table_name = src.table_name)
        WHEN MATCHED THEN
            UPDATE SET cfg.DTIME_INSERTED = src.DTIME_INSERTED
        WHEN NOT MATCHED THEN
            INSERT (table_name, DTIME_INSERTED)
            VALUES (src.table_name, src.DTIME_INSERTED);
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('No new date found. No updates made.');
    END IF;

    -- Determine final target table for view switching
    SELECT MAX(DTIME_INSERTED) INTO v_latest_date_a FROM SDM.CRM_SCORE_A;
    SELECT MAX(DTIME_INSERTED) INTO v_latest_date_b FROM SDM.CRM_SCORE_B;

    IF v_latest_date_a IS NULL AND v_latest_date_b IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No data in CRM_SCORE_A or CRM_SCORE_B.');
        RETURN;
    ELSIF v_latest_date_a IS NULL OR (v_latest_date_b IS NOT NULL AND v_latest_date_b > v_latest_date_a) THEN
        v_target_table := 'CRM_SCORE_B';

        UPDATE SDM.CRM_SCORE_CONFIG
        SET FLAG_ACTIVE = 'Y', DTIME_INSERTED = v_latest_date_b
        WHERE TABLE_NAME = 'CRM_SCORE_B';

        UPDATE SDM.CRM_SCORE_CONFIG
        SET FLAG_ACTIVE = 'N'
        WHERE TABLE_NAME = 'CRM_SCORE_A';
    ELSE
        v_target_table := 'CRM_SCORE_A';

        UPDATE SDM.CRM_SCORE_CONFIG
        SET FLAG_ACTIVE = 'Y', DTIME_INSERTED = v_latest_date_a
        WHERE TABLE_NAME = 'CRM_SCORE_A';

        UPDATE SDM.CRM_SCORE_CONFIG
        SET FLAG_ACTIVE = 'N', DTIME_INSERTED = v_latest_date_b
        WHERE TABLE_NAME = 'CRM_SCORE_B';
    END IF;

    -- Update the view
    v_sql := 'CREATE OR REPLACE EDITIONABLE VIEW SDM.V_CRM_SCORE AS SELECT * FROM ' || v_target_table;
    EXECUTE IMMEDIATE v_sql;

    DBMS_OUTPUT.PUT_LINE('SDM.V_CRM_SCORE now points to: ' || v_target_table);

    -- Step 5: Mark job as run today
    UPDATE SDM.CRM_UPDATE_STATUS
    SET LAST_RUN_DATE = SYSDATE
    WHERE JOB_NAME = 'UPDATE_CRM_SCORE_DAILY';
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in UPDATE_CRM_SCORE: ' || SQLERRM);
        RAISE;
END;
/
