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




// Question 1: Top 20 zip codes where most bikes were rented from.
@TransformedData =
    SELECT [Zip Code], SUM([Bike #]) AS TotalRides
    FROM @TripData
    GROUP BY [Zip Code];
      
OUTPUT @TransformedData
TO "/Output/trial_Q1.csv"
ORDER BY TotalRides DESC 
FETCH 20 ROWS
USING Outputters.Csv(outputHeader : true);



//Question 2: Daily duration aggregate across the rental subscriber types
@Tranform2 =
    SELECT [Subscriber Type],[Start Date].ToString("dd/MM/yyyy") AS DateOnly,SUM([Duration]) AS TotalDuration
    FROM @TripData
    GROUP BY [Start Date].ToString("dd/MM/yyyy"),[Subscriber Type];
    
OUTPUT @Tranform2
TO "/Output/trial_Q2.csv"
USING Outputters.Csv(outputHeader : true);


