# Import the dependencies.

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import numpy as np

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta

from flask import Flask, jsonify

    
from collections import defaultdict  # to generate dictionary 'precipitation

################################################
# Database Setup
################################################
engine = create_engine("sqlite:///immigration_canada_pr.sqlite")


# reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)

# # reflect the tables
Base.classes.keys()

#to check
print(Base.classes.keys())

# Save references to each table
countries = Base.classes.countries
immigration = Base.classes.immigration
macrodata = Base.classes.macrodata

# Create our session (link) from Python to the DB
session = Session(engine)

#to check
print(session.query(countries).first().__dict__)
print(session.query(immigration).first().__dict__)
print(session.query(macrodata).first().__dict__)



#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

## welcome route presentation of the 6 routes available

@app.route("/")

def welcome():
    """List all available api routes."""
    
    return (
        f"This is your API to access to the analysis of 'immigration as Permanent resident to Canada motivations'  <br/>"
        f"  <br/>"
        f"Available Routes:<br/>"
        f"  <br/>"
        f"List of the countries available     : /api/v0.1/countries_list <br/>"
        f"immigation_flow_per_year_between 2015 and 2024       : /api/v0.1/immigation_flow_per_year_between/<year_start>/<year_end> <br/>"
        f"  <br/>"
        f"immigation_flow_per_year_between 2015 and 2024       : /api/v0.1/immigation_flow_per_year_between/<year_start>/<year_end> <br/>"


        )

# route countries : returns the list of available countries 

@app.route("/api/v0.1/countries_list")
def countries_list():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # query 'station' table
    results = session.query(immigration.country).distinct().order_by(immigration.country).all()
         
    # close the session               
    session.close()

    countries_list = []
    for country in results:
            # countries = {}
            # countries['Country_name'] = country[0]
            # countries_list.append(countries)
            countries_list.append(country[0])

   
    return jsonify(countries_list)


# # route immigration_yearly : returns the cumulated immigration flow per year - between to year (2015 and 2024)

@app.route("/api/v0.1/immigation_flow_per_year_between/<year_start>/<year_end>")
def immigation_flow_per_year(year_start, year_end):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # query measurment table for the last date in the DB

# set a condition to be sure that dates are consistant

    if year_end >= year_start :

        # grop by table measurment, and calculate TMIN, TAVG, TMAX) for each date after filter of dates              
        result = session.query(immigration.country, func.sum(immigration.immigration_flow)\
                        .filter(immigration.year >= year_start)\
                        .filter(immigration.year <= year_end))\
                        .group_by(immigration.country) \
                        .order_by(func.sum(immigration.immigration_flow).desc()) \
                        .all()

        # close session
        session.close()        
        
        #create a list to be jsonified, by loopong through the result above
 
        flow_by_country_list = []
        for country, sumflow in result:
            flow_dict = {}
            flow_dict['country'] = country
            flow_dict['flow'] = sumflow


            flow_by_country_list.append(flow_dict)

        return jsonify(flow_by_country_list)
    
    else : 
        # error message if dates are not consistant
        return 'NEIN!!! DAS IST NICH GUT !!! <br/> \
            <br/> \
            Year_end must be later than Year_start. <br/> \
            In other words, you must enter a "year_end" that is after the "year_start".' 



    

if __name__ == '__main__':
    app.run(debug=True)