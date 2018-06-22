/*Assignment 3 Part 3 a*/
SELECT STORE.STOREID, SOLDVIA.NOOFITEMS
FROM STORE, SALESTRANSACTION, SOLDVIA
WHERE STORE.STOREID = SALESTRANSACTION.STOREID
AND SALESTRANSACTION.TID = SOLDVIA.TID
ORDER BY SOLDVIA.NOOFITEMS ASC;

/*Assignment 3 Part 3 b*/
SELECT SALESTRANSACTION.TID, SALESTRANSACTION.TDATE, CUSTOMER.CUSTOMERNAME, 
CUSTOMER.CUSTOMERZIP, STORE.STOREID, STORE.STOREZIP
FROM SALESTRANSACTION, CUSTOMER, STORE
WHERE STORE.REGIONID = 'C'
AND SALESTRANSACTION.CUSTOMERID = CUSTOMER.CUSTOMERID
AND STORE.STOREID = SALESTRANSACTION.STOREID;

/*Assignment 3 Part 3 c*/
SELECT CUSTOMER.CUSTOMERNAME, PRODUCT.PRODUCTPRICE, PRODUCT.PRODUCTID, SOLDVIA.TID
FROM CUSTOMER, PRODUCT, SOLDVIA, SALESTRANSACTION
WHERE PRODUCT.PRODUCTPRICE >= 150
AND PRODUCT.PRODUCTID = SOLDVIA.PRODUCTID
AND SOLDVIA.TID = SALESTRANSACTION.TID
AND CUSTOMER.CUSTOMERID = SALESTRANSACTION.CUSTOMERID;

/*Assignment 3 Part 3 d*/
SELECT CATEGORY.CATEGORYNAME, SUM(SOLDVIA.NOOFITEMS * PRODUCT.PRODUCTPRICE) 
FROM SOLDVIA, PRODUCT, CATEGORY
WHERE SOLDVIA.PRODUCTID = PRODUCT.PRODUCTID
AND CATEGORY.CATEGORYID = PRODUCT.CATEGORYID
GROUP BY CATEGORY.CATEGORYNAME
ORDER BY SUM(SOLDVIA.NOOFITEMS * PRODUCT.PRODUCTPRICE) DESC;