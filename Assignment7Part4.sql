/* Assignment7Part4a */
SELECT ANAME
FROM ANIMAL
WHERE NOT EXISTS(
SELECT ZOOKEEPID FROM HANDLES
WHERE ANIMAL.AID = HANDLES.ANIMALID);

/* Assignment7Part4b */
CREATE TRIGGER reset BEFORE UPDATE OF TIMETOFEED ON ANIMAL
WHEN (TIMETOFEED < 0.25)
BEGIN
  :newTIMETOFEED := 0.25;
END;

/* Assignment7Part4c */
^(?!(000|666|9))\d{3}-(?!00)\d{2}-(?!0000)\d{4}$