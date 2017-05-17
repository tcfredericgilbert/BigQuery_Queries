// Title: FXDate_FiscalDate_v1
// Description: FXDate_FiscalDate_v1

// ==== BigQuery ====

SELECT 
FXDate,
USD2CAD,
CAD2USD,
TransactionDate,
Transaction_Month,
FiscalYear,
FiscalPeriod,
StartDate,
EndDate,
NbDays,
NbWeeks
FROM (

SELECT 
A.FXDate AS FXDate,
A.USD2CAD AS USD2CAD,	
A.CAD2USD AS CAD2USD,	
B.TransactionDate AS TransactionDate,	
STRING(CONCAT(STRING(YEAR(B.TransactionDate)),'-',IF(MONTH(B.TransactionDate)<10,CONCAT('0',STRING(MONTH(B.TransactionDate))),STRING(MONTH(B.TransactionDate))),'-','01')) AS Transaction_Month,	
B.FiscalYear AS FiscalYear,	
B.FiscalPeriod AS FiscalPeriod,	
B.StartDate AS StartDate,	
B.EndDate AS EndDate,	
B.NbDays AS NbDays,	
B.NbWeeks AS NbWeeks,

FROM (
SELECT * FROM [extreme-pixel-830:ramp_reference.FXRates]
ORDER BY FXDate ASC ) AS A
JOIN ramp_reference.FiscalCalendar_20162017 AS B
ON A.FXDate = B.TransactionDate
ORDER BY FXDate ASC
)
ORDER BY FXDate DESC

//SAVES TO: ramp_reference.FXDate_FSDate

// ==== JavaScript UDF ====

// Example user-defined function, documentation: https://goo.gl/6KR8O0
// Sample SQL: SELECT outputA, outputB FROM (passthrough(SELECT "abc" AS inputA, "def" AS inputB))

/*
function passthroughExample(row, emit) {
  emit({outputA: row.inputA, outputB: row.inputB});
}

bigquery.defineFunction(
  'passthrough',                           // Name of the function exported to SQL
  ['inputA', 'inputB'],                    // Names of input columns
  [{'name': 'outputA', 'type': 'string'},  // Output schema
   {'name': 'outputB', 'type': 'string'}],
  passthroughExample                       // Reference to JavaScript UDF
);
*/