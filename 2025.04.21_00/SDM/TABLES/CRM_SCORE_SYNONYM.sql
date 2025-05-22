--liquibase formatted.sql
--changeset michael.cawayan:SDM.CRM_SCORE_SYNONYM contextFilter:PH endDelimiter:/ runOnChange:true

CREATE SYNONYM SDM.CRM_SCORE FOR SDM.V_CRM_SCORE
/