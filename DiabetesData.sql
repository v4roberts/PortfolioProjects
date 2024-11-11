--Looking at Diabetes data to explore interesting correlations and determine groups that are most at risk for diabetes.

-- Average Biometrics of persons with and without Diabetes

select Outcome, avg(Age) as AvgAge
from DiabetesData
group by Outcome;

select Outcome, avg(BMI) as AvgBMI
from DiabetesData
group by Outcome;

select Outcome, avg(Glucose) as AvgGlucose
from DiabetesData
group by Outcome;

select Outcome, avg(pregnancies) as AvgPregnancies
from DiabetesData
group by Outcome;

-- Total Counts

SELECT COUNT(*) FROM DiabetesData;

SELECT Outcome, COUNT(*) AS count
FROM DiabetesData
GROUP BY Outcome;

SELECT Outcome, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DiabetesData) AS percentage
FROM DiabetesData
GROUP BY Outcome;

SELECT MAX(Insulin) AS MaxInsulin, MIN(Insulin) AS MinInsulin
FROM DiabetesData
WHERE Outcome = 1;

-- High-risk groups

SELECT *
FROM DiabetesData
WHERE Glucose > 140 AND BMI > 30 AND Outcome = 1;

SELECT Age, COUNT(*) AS count
FROM DiabetesData
WHERE DiabetesPedigreeFunction > 0.5 AND Outcome = 1
GROUP BY Age;

-- Pregnancy Related

SELECT *
FROM DiabetesData
WHERE Pregnancies > 5 AND Outcome = 1;

-- Diabetes Corollaries

SELECT COUNT(*) as HighBPandGlucose
FROM DiabetesData
WHERE Glucose > 140 AND BloodPressure > 80;

SELECT glucose_level, AVG(BMI) AS avg_bmi
FROM (
    SELECT CASE
             WHEN Glucose < 100 THEN 'Low'
             WHEN Glucose BETWEEN 100 AND 140 THEN 'Medium'
             ELSE 'High'
           END AS glucose_level,
           BMI
    FROM DiabetesData
) AS subquery
GROUP BY glucose_level;

-- Age-based Analysis

SELECT Outcome, MIN(Age) AS min_age, MAX(Age) AS max_age
FROM DiabetesData
GROUP BY Outcome;

SELECT TOP 5 *
FROM DiabetesData
WHERE Outcome = 1
ORDER BY Age DESC;

-- Combined columns looking for interesting outcomes

SELECT *
FROM DiabetesData
WHERE Insulin > 200 AND Glucose < 100 AND Outcome = 1;

SELECT SkinThickness, BMI, Outcome, COUNT(*) AS count
FROM DiabetesData
WHERE SkinThickness > 40 AND BMI > 35
GROUP BY SkinThickness, BMI, Outcome;
