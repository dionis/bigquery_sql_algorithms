WITH Customer AS (
SELECT  *, 0 AS EXAMPLE, '0' AS EXAMPLE1 FROM `import_dev.Customer` LIMIT 5
),

Cashier AS (
SELECT * FROM `import_dev.cashier` LIMIT 5
),

Promotion AS (
SELECT *, 'Test' AS EXAMPLE, 'Test2' AS EXAMPLE1,  'Test3' AS EXAMPLE3, 'Test4' AS EXAMPLE4 FROM `import_dev.promotion` LIMIT 5
)

Select * FROM 
Customer 
 INNER JOIN Cashier ON Customer.EXAMPLE = 0