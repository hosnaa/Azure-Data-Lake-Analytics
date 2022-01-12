/**
Extract Trip Data
**/
@TripData =
    EXTRACT [Trip ID] string,
            [Duration] int,
            [Start Date] DateTime,
            [Start Terminal] int,
            [End Date] DateTime,
            [End Terminal] int,
            [Bike #] int,
            [Subscriber Type] string,
            [Zip Code] string
            
    FROM "/Input/201508_trip_data.csv"
    USING Extractors.Csv(skipFirstNRows : 1);

/**
Extract Station Data
**/
@StationData =
    EXTRACT [station_id] int,
            [name] string,
            [lat] decimal,
            [long] decimal,
            [dockcount] int,
            [landmark] string,
            [installation] DateTime
            
    FROM "/Input/201508_station_data.csv"
    USING Extractors.Csv(skipFirstNRows : 1);




//Question 3 : What are the top busiest terminals for bike pickup?
@Transform3 =
    SELECT *
    FROM @TripData
         FULL OUTER JOIN @StationData ON @TripData.[Start Terminal] == @StationData.[station_id];
 
@Transform4 =

    SELECT [Start Terminal],
           [name],
           COUNT([name]) AS NumberOfTrips
    FROM @Transform3 
    GROUP BY [Start Terminal], name;

                
OUTPUT @Transform4
TO "/Output/trial_Q3.csv"
ORDER BY NumberOfTrips DESC
USING Outputters.Csv(outputHeader : true);



// Question 4:  Which 5 terminal has the least drop-offs? 
@Transform5 =
    SELECT *
    FROM @TripData
         FULL OUTER JOIN @StationData ON @TripData.[End Terminal] == @StationData.[station_id];
 
@Transform6 =

    SELECT [End Terminal],
           [name],
           COUNT([name]) AS NumberOfDropOffs
    FROM @Transform5 
    GROUP BY [End Terminal], name;

                
OUTPUT @Transform6
TO "/Output/trial_Q4.csv"
ORDER BY NumberOfDropOffs ASC
FETCH 5 ROWS
USING Outputters.Csv(outputHeader : true);


