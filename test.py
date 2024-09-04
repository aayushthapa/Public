SELECT
    t1.PART_NUM,
    COUNT(DISTINCT t2.PART_NUM) AS count_same_able_code,
    COUNT(DISTINCT CASE WHEN t1.UNSPSC_CODE = t2.UNSPSC_CODE THEN t2.PART_NUM END) AS count_same_able_and_unspsc_code
FROM
    your_table AS t1
LEFT JOIN
    your_table AS t2
ON
    t1.ABLE_CODE = t2.ABLE_CODE
    AND t1.PART_NUM <> t2.PART_NUM
GROUP BY
    t1.PART_NUM;
