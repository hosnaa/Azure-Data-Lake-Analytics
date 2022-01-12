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


//Question 5: What is the monthly summary of bike rentals (format - month/year ex. 06/2020) 
@Transform7 =
    SELECT  [Start Date].Month + "/" + [Start Date].Year AS MontlySummary , SUM([Bike #]) AS NumberOfBikeRentals, COUNT([Trip ID]) AS NumberOfTrips
    FROM @TripData
    GROUP BY [Start Date].Year,[Start Date].Month;
                                               
OUTPUT @Transform7
TO "/Output/trial_Q5.csv"  
USING Outputters.Csv(outputHeader : true);