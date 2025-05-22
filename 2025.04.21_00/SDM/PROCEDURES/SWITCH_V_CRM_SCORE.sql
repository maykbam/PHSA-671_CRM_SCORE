--liquibase formatted.sql
--changeset michael.cawayan:SDM.SWITCH_V_CRM_SCORE contextFilter:PH endDelimiter:/ runOnChange:true

CREATE OR REPLACE PROCEDURE SDM.SWITCH_V_CRM_SCORE IS
    v_latest_date_a DATE;
    v_latest_date_b DATE;
    v_target_table  VARCHAR2(30);
    v_sql           CLOB;
BEGIN
    -- Get latest DTIME_INSERTED from CRM_SCORE_A
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

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in UPDATE_CRM_SCORE_TEST2: ' || SQLERRM);
        RAISE;
END;
/
  