
WITH TBL_DUP_2SOURCE_KDI55 AS 
(
    SELECT * FROM {{ref("KTLMDM_MDM_INDIVIDUAL_SUMMARY_DATA_BFR_MERGE")}}
), 

KDI2_KDI7_KDI55_DUP AS 
(
    SELECT 
    KDI55, 
    KDI2, 
    KDI7,
    COUNT(DISTINCT OPERATING_KDI1) CNT 
    FROM TBL_DUP_2SOURCE_KDI55 
    WHERE KDI55 IS NOT NULL AND KDI2 IS NOT NULL AND KDI7 IS NOT NULL 
    GROUP BY KDI55, KDI2, KDI7 HAVING COUNT(DISTINCT OPERATING_KDI1) > 1 
),

KDI22_01_DUP AS 
(
    SELECT KDI22_01,
    COUNT(DISTINCT OPERATING_KDI1) CNT 
    FROM TBL_DUP_2SOURCE_KDI55 WHERE KDI22_01 IS NOT NULL 
    GROUP BY KDI22_01 HAVING COUNT(DISTINCT OPERATING_KDI1) > 1 
),

KDI21_01_DUP AS 
(
    SELECT KDI21_01, 
    COUNT(DISTINCT OPERATING_KDI1) CNT 
    FROM TBL_DUP_2SOURCE_KDI55 WHERE KDI21_01 IS NOT NULL 
    GROUP BY KDI21_01 HAVING COUNT(DISTINCT OPERATING_KDI1) > 1 
),

KDI23_01_DUP AS 
(
    SELECT KDI23_01, 
    COUNT(DISTINCT OPERATING_KDI1) CNT 
    FROM TBL_DUP_2SOURCE_KDI55 WHERE KDI23_01 IS NOT NULL 
    GROUP BY KDI23_01 HAVING COUNT(DISTINCT OPERATING_KDI1) > 1 
),

KDI2_KDI7_KDI17_DUP AS 
(
    SELECT 
    KDI2,
    KDI7,
    KDI17,
    COUNT(DISTINCT OPERATING_KDI1) CNT 
    FROM TBL_DUP_2SOURCE_KDI55 
    WHERE KDI2 IS NOT NULL AND KDI7 IS NOT NULL AND KDI17 IS NOT NULL
    GROUP BY KDI2, KDI7, KDI17 HAVING COUNT(DISTINCT OPERATING_KDI1) > 1 
),

SUM_TBL AS (
    SELECT 
    KDI1,
    KDI2 || '|' || KDI55 MERGE_COLUMN,
    KDI55,
    KDI2,
    KDI7,
    KDI17,
    KDI22_01,
    KDI21_01,
    KDI23_01,
    OPERATING_KDI1,
    OPERATING_SYSTEM,
    'MS-KDI2-KDI7-KDI55' RULE_CODE
    FROM TBL_DUP_2SOURCE_KDI55 M WHERE EXISTS (SELECT 1 FROM KDI2_KDI7_KDI55_DUP M0 WHERE M.KDI2 = M0.KDI2 AND M.KDI55 = M0.KDI55 AND M.KDI7 = M0.KDI7 )
    
    UNION ALL 

    SELECT 
    KDI1,
    KDI22_01 MERGE_COLUMN,
    KDI55,
    KDI2,
    KDI7,
    KDI17,
    KDI22_01,
    KDI21_01,
    KDI23_01,
    OPERATING_KDI1,
    OPERATING_SYSTEM,
    'MS-KDI22_01' RULE_CODE
    FROM TBL_DUP_2SOURCE_KDI55 M WHERE EXISTS (SELECT 1 FROM KDI22_01_DUP M0 WHERE M.KDI22_01 = M0.KDI22_01 )
    
    UNION ALL

    SELECT 
    KDI1,
    KDI21_01 MERGE_COLUMN,
    KDI55,
    KDI2,
    KDI7,
    KDI17,
    KDI22_01,
    KDI21_01,
    KDI23_01,
    OPERATING_KDI1,
    OPERATING_SYSTEM,
    'MS-KDI21_01' RULE_CODE
    FROM TBL_DUP_2SOURCE_KDI55 M WHERE EXISTS (SELECT 1 FROM KDI21_01_DUP M0 WHERE M.KDI21_01 = M0.KDI21_01 )
    
    UNION ALL

    SELECT 
    KDI1,
    KDI23_01 MERGE_COLUMN,
    KDI55,
    KDI2,
    KDI7,
    KDI17,
    KDI22_01,
    KDI21_01,
    KDI23_01,
    OPERATING_KDI1,
    OPERATING_SYSTEM,
    'MS-KDI23_01' RULE_CODE
    FROM TBL_DUP_2SOURCE_KDI55 M WHERE EXISTS (SELECT 1 FROM KDI23_01_DUP M0 WHERE M.KDI23_01 = M0.KDI23_01 )

    UNION ALL

    SELECT 
    KDI1,
    KDI2 || '|' || KDI7 || '|' || KDI17 MERGE_COLUMN,
    KDI55,
    KDI2,
    KDI7,
    KDI17,
    KDI22_01,
    KDI21_01,
    KDI23_01,
    OPERATING_KDI1,
    OPERATING_SYSTEM,
    'MS-KDI2-KDI7-KDI17' RULE_CODE
    FROM TBL_DUP_2SOURCE_KDI55 M WHERE EXISTS (SELECT 1 FROM KDI2_KDI7_KDI17_DUP M0 WHERE M.KDI2 = M0.KDI2 AND M.KDI7 = M0.KDI7 AND M.KDI17 = M0.KDI17 )
)
SELECT KDI1, 
MERGE_COLUMN, 
KDI55,
KDI2,
KDI7,
KDI17,
KDI22_01,
KDI21_01,
KDI23_01,
OPERATING_KDI1, 
OPERATING_SYSTEM, 
RULE_CODE
FROM SUM_TBL