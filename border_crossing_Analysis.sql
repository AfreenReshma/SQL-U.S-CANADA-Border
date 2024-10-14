CREATE TABLE BORDER_CROSSING (
    Port_Name VARCHAR(50),
    State VARCHAR(50),
    Port_Code INT,
    Border VARCHAR(50),
    date DATETIME,
    Measure VARCHAR(50),
    value INT
);

SET GLOBAL LOCAL_INFILE=ON;
LOAD DATA LOCAL INFILE "C:\Users\afree\OneDrive\Desktop\projects\Border_Crossing_Entry_Data.csv"
INTO TABLE border_crossing
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT 
    *
FROM
    border_crossing;

--  WEEK 1: DATA EXPLORATION AND BASIC QUERIES--

SELECT DISTINCT
    (portname), state
FROM
    border_crossing;

-- 2. COUNT THE TOTAL NUMBER OF UNIQUE BORDERS AND THE TOTAL NUMBER OF ENTRIES ASSOCIATED WITH EACH BORDER --
SELECT 
    border, COUNT(*) AS total_entries
FROM
    border_crossing
GROUP BY border;

SELECT 
    COUNT(DISTINCT border)
FROM
    border_crossing;

-- 3. RETREIVE THE TOTAL NUMBER OF ENTRIES(CROSSINGS) FOR EACH YEAR, SORTED FROM MOST RECENT TO OLDEST YEAR --
SELECT 
    YEAR(Date) AS year, SUM(value) AS total_value
FROM
    border_crossing
WHERE
    date IS NOT NULL
GROUP BY year;


-- 4.FIND ALL PORTS THAT HAVE RECORDED MORE THAN 5000 CROSSING FOR THE TRUCKS MEASURE TYPE --
SELECT 
    portname, measure, value
FROM
    border_crossing
WHERE
    measure = 'Trucks' AND value > 5000;


-- 5. IDENTIFY TOP 3 STATES WITH THE HIGHEST TOTAL NUMBER OF PEDESTRIAN CROSSINGS--
SELECT 
    portname, state, measure, value
FROM
    border_crossing
WHERE
    measure = 'pedestrians'
ORDER BY value DESC
LIMIT 3;

-- 6.FOR THE YEAR 2023, EXTRACT THE TOTAL NUMBER OF CROSSINGS PER MONTH, CATEGORISED BY MEASURE TYPE--
SELECT 
    state, measure, COUNT(*) As total_crossing
FROM
    border_crossing
GROUP BY state , measure
ORDER BY state , total_crossing DESC;

-- 7.FIND THE MEASURE TYPE MOST FREQUENTLY RECORDER IN EACH STATE --
SELECT 
    measure, state, COUNT(measure) AS measure_type
FROM
    border_crossing
GROUP BY state , measure;

-- 8.GENERATE A SUMMARY REPORT SHOWING THE TOTAL NUMBER OF CROSSINGS FOR EACH MEASURE TYPE, GROUPED BY BORDER--
SELECT 
    Measure, border, COUNT(*) AS total
FROM
    border_crossing
GROUP BY border , measure;


-- WEEK 2 INTERMEDIATE QUERIES WITH AGGREGATION --

-- 1.For Texas, calculate the average number of crossings per month for each measure type.--
SELECT 
    measure,
    YEAR(date) AS year,
    MONTH(date) AS month,
    AVG(value) AS avg_crossing
FROM
    border_crossing
GROUP BY measure , YEAR(date) , MONTH(date)
ORDER BY measure , year , month;

-- 2.Find the port on the U.S.-Canada border with the highest number of crossings. Include the measure type, total crossing--
SELECT 
    portname, measure, SUM(value) AS total_crossing
FROM
    border_crossing
WHERE
    border = 'US-Canada Border'
GROUP BY portname , measure
ORDER BY total_crossing DESC
LIMIT 1;

-- 3.Calculate the total number of crossings for the "Buses" measure type in each state, ordered by total crossing in desc --
SELECT 
    measure, state, COUNT(*) AS total_crossing
FROM
    border_crossing
WHERE
    measure = 'Buses'
GROUP BY state
ORDER BY total_crossing DESC;

-- 4.For the U.S.-Mexico border, calculate the average and total number of crossings for each port in the year 2022 --
SELECT 
    portname,
    AVG(value) AS avg_crossing,
    SUM(value) AS total_crossing
FROM
    border_crossing
WHERE
    border = 'US-Mexico Border'
        AND YEAR(date) = 2022
GROUP BY portname;

-- 5.list all the port that reported pedestrian as a crossing measure in 2023 and show there total number of pedestrian crosssing --
SELECT 
    portname, SUM(value) AS total
FROM
    border_crossing
WHERE
    measure = 'pedestrians'
        AND YEAR(date) = 2023
GROUP BY portname;

-- 6. extract the total number of crossing in each border of every year available in the data set --
SELECT 
    border, total, year
FROM
    (SELECT 
        border, SUM(value) AS total, YEAR(date) AS year
    FROM
        border_crossing
    GROUP BY border , year) AS yearly_crossing
ORDER BY border , year desc;

-- 7.Identify the month in  2023 with the highest number of truck crossing list the month and the total crossing --
SELECT 
    SUM(value) AS total_truck_crossing, MONTH(date) AS months
FROM
    border_crossing
WHERE
    measure = 'trucks' AND YEAR(date) = 2023
GROUP BY months
ORDER BY total_truck_crossing
LIMIT 1;

-- 8. list the top 5 ports and the highest crossing activity (all measure type in 2021) showing the measure type and total crossing for each port --
SELECT 
    measure, portname, COUNT(*) AS highest_crossing
FROM
    border_crossing
WHERE
    YEAR(date) = 2021
GROUP BY measure , portname
ORDER BY highest_crossing DESC;


-- ADVANCE QUERIES AND REASEARCH LIKE TASKS --

-- 1.CALCULATE THE TOTAL NUMBER OF CROSSING EACH YEAR FROM 2019 TO 2023 GROUPED BY BORDER AND MEASURE TYPE --
SELECT 
    border,
    measure,
    SUM(value) AS total_crossing,
    YEAR(date) AS year
FROM
    border_crossing
WHERE
    YEAR(date) BETWEEN 2019 AND 2023
GROUP BY border , measure , YEAR(date)
ORDER BY border , measure , YEAR(date);

-- 2.FOR TEXAS, FIND MOST FREQUENTLY RECORDED MEASURE TYPES FOR THE YEAR 2023.RANK THE MEASURE TYPES BY NUMBER NUMBER OF ENTRIES WITHOUT USING RANK --
select measure, count(*) as number_of_entries
from border_crossing
where state = 'Texas' and year(date) = 2023
group by measure
order by number_of_entries desc;

-- 3. COMPARE THE TOTAL NUMBER OF CONTAINER CROSSING OVER THE LAST 3 YEAR FOR EACH BORDER --
with containercrossing as(
SELECT 
    border, YEAR(date) AS year, SUM(value) AS total_container
FROM
    border_crossing
WHERE
    measure = 'containers'
        AND YEAR(date) BETWEEN YEAR(CURDATE()) - 3 AND YEAR(CURDATE()) - 1
GROUP BY border , YEAR(date)
),
rankedcrossing as (
select border, year, total_container, row_number() over( partition by border order by year desc) as row_num
from containercrossing
)
SELECT 
    border, year, total_container
FROM
    rankedcrossing
WHERE
    row_num <= 3
ORDER BY border , year DESC;

-- 4.IDENTIFY THE BUSIEST MONTH EACH YEAR (2019 TO 2023), IN TERMS OF PEDESTRIAN CROSSINGS, SHOW THE YEAR MONTH AND TOTAL PEDESTRIAN CROSIING --
with pedestriancrossing as (
SELECT 
    YEAR(date) AS year,
    MONTH(date) AS month,
    SUM(value) AS total_crossing
FROM
    border_crossing
WHERE
    measure = 'pedastrians' AND YEAR(date) BETWEEN 2019 AND 2023
GROUP BY YEAR(date) , MONTH(date)
),
rankedmonths as (
select year,month, total_crossing, row_number()over(partition by year order by total_crossing desc) as rn
from pedestriancrossing
)
SELECT 
    year, month, total_crossing
FROM
    rankedmonths
WHERE
    rn = 1
ORDER BY year;

-- 5.COMPARE THE TOTAL NUMBER OF TRUCK CROSSING IN 2021 AND 2022 AT TOP 5 BUSSIEST PORT FOR TRUCKS. DISPLAY BOTH YEAR TOTAL SIDE BY SIDE --
with truckcrossing as (
SELECT 
    portname, YEAR(date) AS year, SUM(value) AS totalcrossing
FROM
    border_crossing
WHERE
    measure = 'trucks'
        AND YEAR(date) IN (2021 , 2022)
GROUP BY portname , YEAR(date)
),
top5ports as (
SELECT 
    portname, SUM(totalcrossing) AS tc
FROM
    truckcrossing
GROUP BY portname
ORDER BY tc DESC
LIMIT 5
)
SELECT 
    t1.portname, 
    SUM(CASE WHEN t1.year = 2021 THEN t1.totalcrossing ELSE 0 END) AS total_2021,
    SUM(CASE WHEN t1.year = 2022 THEN t1.totalcrossing ELSE 0 END) AS total_2022
FROM 
    TruckCrossing t1
JOIN 
    Top5Ports t2
ON 
    t1.portname = t2.portname
GROUP BY 
    t1.portname
ORDER BY 
    total_2021 + total_2022 DESC;
    
-- 6.FIND THE PORT WITH THE LOWEST TOTAL CROSSINFG ON U.S AND CANADA BORDER FOR ANY MEASURE TYPE IN 2023 --
SELECT 
    portname, SUM(value) AS total_crossing
FROM
    border_crossing
WHERE
    border = 'US-Canada Border'
        AND YEAR(date) = 2023
GROUP BY portname
ORDER BY total_crossing ASC
LIMIT 1;

-- 7.LIST MONTHLY TOTAL NUMBER OF CROSSING FOR BUSSES ACROSS ALL THE STATES IN 2022, SORTED IN ASCENDING ORDER --
SELECT 
    state, MONTH(date), SUM(value) AS total_crossing
FROM
    border_crossing
WHERE
    measure = 'buses' AND YEAR(date) = 2022
GROUP BY state , MONTH(date)
ORDER BY total_crossing ASC;

-- 8.DISPLAY THE SUM AND AVERAGE NUMBER OF CROSSING FOR EACH STATE GROUP THE MEASURE TYPE AND YEAR, ONLY SHOW THE ENTRIES WHERE THE AVG CROSSING EXCEEDS 500 --
SELECT 
    state, 
    measure, 
    YEAR(date) AS year,
    SUM(value) AS total_crossings,
    AVG(value) AS average_crossings
FROM 
    border_crossing
GROUP BY 
    state, 
    measure, 
    YEAR(date)
HAVING 
    AVG(value) > 500
ORDER BY 
    state, 
    year, 
    measure;









