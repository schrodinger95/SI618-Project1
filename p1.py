import json
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Join two datasets
ShareRaceByCity = sqlContext.read.csv("ShareRaceByCity.csv", header=True)
IncomeByCity = sqlContext.read.csv("kaggle_income.csv", header=True)

ShareRaceByCity.registerTempTable("ShareRaceByCity")
IncomeByCity.registerTempTable("IncomeByCity")

RevisedShareRaceByCity = sqlContext.sql("SELECT *, LEFT(City, LENGTH(City) - LOCATE(' ', REVERSE(City))) as RevisedCity from ShareRaceByCity")
RevisedShareRaceByCity.registerTempTable("RevisedShareRaceByCity")

JoinByCity = sqlContext.sql("SELECT IncomeByCity.State_Name, IncomeByCity.City, IncomeByCity.ALand, IncomeByCity.AWater, IncomeByCity.Lat, IncomeByCity.Lon, IncomeByCity.Mean, IncomeByCity.Median, IncomeByCity.Stdev, IncomeByCity.sum_w, RevisedShareRaceByCity.share_white, RevisedShareRaceByCity.share_black, RevisedShareRaceByCity.share_native_american, RevisedShareRaceByCity.share_asian, RevisedShareRaceByCity.share_hispanic from IncomeByCity INNER JOIN RevisedShareRaceByCity ON (IncomeByCity.State_ab=RevisedShareRaceByCity.Geographic_area) AND (IncomeByCity.City=RevisedShareRaceByCity.RevisedCity)")
JoinByCity.registerTempTable("JoinByCity")

# Pre-task
HouseholdShareByRace = sqlContext.sql("SELECT share_white*sum_w as share_white_household, share_black*sum_w as share_black_household, share_native_american*sum_w as share_native_american_household, share_asian*sum_w as share_asian_household, share_hispanic*sum_w as share_hispanic_household from JoinByCity")
HouseholdShareByRace.registerTempTable("HouseholdShareByRace")

SumHouseholdShareByRace = sqlContext.sql("SELECT SUM(share_white_household) as white_household_sum, SUM(share_black_household) as black_household_sum, SUM(share_native_american_household) as native_american_household_sum, SUM(share_asian_household) as asian_household_sum, SUM(share_hispanic_household) as hispanic_household_sum from HouseholdShareByRace")
SumHouseholdShareByRace.registerTempTable("SumHouseholdShareByRace")

PercentHouseholdShareByRace = sqlContext.sql("SELECT white_household_sum/household_sum as white_household_percent, black_household_sum/household_sum as black_household_percent, native_american_household_sum/household_sum as native_american_household_percent, asian_household_sum/household_sum as asian_household_percent, hispanic_household_sum/household_sum as hispanic_household_percent from (SELECT *, white_household_sum+black_household_sum+native_american_household_sum+asian_household_sum+hispanic_household_sum as household_sum from SumHouseholdShareByRace)")
PercentHouseholdShareByRace.show()

# Task 1
AreaShareByRace = sqlContext.sql("SELECT share_white*ALand as share_white_land, share_white*AWater as share_white_water, share_black*ALand as share_black_land, share_black*AWater as share_black_water, share_native_american*ALand as share_native_american_land, share_native_american*AWater as share_native_american_water, share_asian*ALand as share_asian_land, share_asian*AWater as share_asian_water, share_hispanic*ALand as share_hispanic_land, share_hispanic*AWater as share_hispanic_water from JoinByCity")
AreaShareByRace.registerTempTable("AreaShareByRace")

SumAreaShareByRace = sqlContext.sql("SELECT SUM(share_white_land) as white_land_sum, SUM(share_white_water) as white_water_sum, SUM(share_black_land) as black_land_sum, SUM(share_black_water) as black_water_sum, SUM(share_native_american_land) as native_american_land_sum, SUM(share_native_american_water) as native_american_water_sum, SUM(share_asian_land) as asian_land_sum, SUM(share_asian_water) as asian_water_sum, SUM(share_hispanic_land) as hispanic_land_sum, SUM(share_hispanic_water) as hispanic_water_sum from AreaShareByRace")
SumAreaShareByRace.registerTempTable("SumAreaShareByRace")

PercentAreaShareByRace = sqlContext.sql("SELECT white_land_sum/land_sum as white_land_percent, white_water_sum/water_sum as white_water_percent, black_land_sum/land_sum as black_land_percent, black_water_sum/water_sum as black_water_percent, native_american_land_sum/land_sum as native_american_land_percent, native_american_water_sum/water_sum as native_american_water_percent, asian_land_sum/land_sum as asian_land_percent, asian_water_sum/water_sum as asian_water_percent, hispanic_land_sum/land_sum as hispanic_land_percent, hispanic_water_sum/water_sum as hispanic_water_percent from (SELECT *, white_land_sum+black_land_sum+native_american_land_sum+asian_land_sum+hispanic_land_sum as land_sum, white_water_sum+black_water_sum+native_american_water_sum+asian_water_sum+hispanic_water_sum as water_sum from SumAreaShareByRace)")
PercentAreaShareByRace.show()

# Task 2
SumLocShareByRace = sqlContext.sql("SELECT SUM(share_white*sum_w*Lat) as white_lat_sum, SUM(share_white*sum_w*Lon) as white_lon_sum, SUM(share_black*sum_w*Lat) as black_lat_sum, SUM(share_black*sum_w*Lon) as black_lon_sum, SUM(share_native_american*sum_w*Lat) as native_american_lat_sum, SUM(share_native_american*sum_w*Lon) as native_american_lon_sum, SUM(share_asian*sum_w*Lat) as asian_lat_sum, SUM(share_asian*sum_w*Lon) as asian_lon_sum, SUM(share_hispanic*sum_w*Lat) as hispanic_lat_sum, SUM(share_hispanic*sum_w*Lon) as hispanic_lon_sum from JoinByCity")
SumLocShareByRace.registerTempTable("SumLocShareByRace")

AvgLocShareByRace = sqlContext.sql("SELECT white_lat_sum/1.154102989920956E9 as white_avg_lat, white_lon_sum/1.154102989920956E9 as white_avg_lon, black_lat_sum/2.76861316445918E8 as black_avg_lat, black_lon_sum/2.76861316445918E8 as black_avg_lon, native_american_lat_sum/1.5634604132271528E7 as native_american_avg_lat, native_american_lon_sum/1.5634604132271528E7 as native_american_avg_lon, asian_lat_sum/6.846391499218632E7 as asian_avg_lat, asian_lon_sum/6.846391499218632E7 as asian_avg_lon, hispanic_lat_sum/2.8231032458925223E8 as hispanic_avg_lat, hispanic_lon_sum/2.8231032458925223E8 as hispanic_avg_lon from SumLocShareByRace")
AvgLocShareByRace.show()

