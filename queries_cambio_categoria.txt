-- 0. raw
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "raw"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);

-- 1. called_no_answer
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "called_no_answer"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);

-- 2. interested
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "interested"
WHERE propertyCode IN (xxxxxx);

-- 3. call_back
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "call_back"
WHERE propertyCode IN (xxxx);

-- 4. not_interested
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "not_interested"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);

-- 5. valuation_scheduled
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "valuation_scheduled"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);

-- 6. valuation_done
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "valuation_done"
WHERE propertyCode IN (xxxxxx);

-- 7. closed_won
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "closed_won"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);

-- 8. closed_lost
UPDATE `peak-key-470610-t6.pisos.pisos_particular`
SET category = "closed_lost"
WHERE propertyCode IN (xxxxxxx,xxxxxxxx);
