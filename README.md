# PythonSQL-store_data

query="""

SELECT 
    customer_id,
    SUM(CASE 
            WHEN transaction_type = 'PURCHASE' AND product_category = 'Grocery' AND currency='paise'  THEN value/100 
            WHEN transaction_type = 'PURCHASE' AND product_category = 'Grocery' AND currency!='paise'  THEN value
            ELSE 0
        END) AS Grocery_Purchases,
    SUM(CASE 
            WHEN transaction_type = 'PURCHASE' AND product_category = 'Electronics' currency='paise'  THEN value/100
            WHEN transaction_type = 'PURCHASE' AND product_category = 'Electronics' currency!='paise'  THEN value
            ELSE 0
        END) AS Electronics_Purchases,
    SUM(CASE 
            WHEN transaction_type = 'PURCHASE' AND product_category = 'Clothing' AND currency='paise'  THEN value/100
            WHEN transaction_type = 'RETURN' AND product_category = 'Clothing' AND currency!='paise'  THEN value
            ELSE 0
        END) AS Clothing_Purchases,
    SUM(CASE 
            WHEN transaction_type = 'RETURN' AND product_category = 'Grocery' AND currency='paise'  THEN value/100
            WHEN transaction_type = 'RETURN' AND product_category = 'Grocery' AND currency!='paise'  THEN value
            ELSE 0
        END) AS Grocery_Returns,
    SUM(CASE 
            WHEN transaction_type = 'RETURN' AND product_category = 'Electronics'AND currency='paise'  THEN value/100
            WHEN transaction_type = 'RETURN' AND product_category = 'Electronics'AND currency!='paise'  THEN value
            ELSE 0
        END) AS Electronics_Returns,
    SUM(CASE 
            WHEN transaction_type = 'RETURN' AND product_category = 'Clothing'AND currency='paise'  THEN value/100
            WHEN transaction_type = 'RETURN' AND product_category = 'Clothing'AND currency!='paise'  THEN value
            ELSE 0
        END) AS Clothing_Returns
FROM 
    sales_transactions
WHERE 
    transaction_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 
    customer_id;
"""
customer_data=pd.read_sql(query,con=conn1)
