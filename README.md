# OpenskyRecorder
Calls OpenSky REST API and stores response to SQLite databse

## Important before use
- This is a inofficial tool for interacting with the OpenSky REST API.
- For Terms and Conditions on how to use the provided data, visit https://opensky-network.org/ first!
- This script was built for my specific usecase. It's no built or tested with general use in mind. I just share this here, if anyone finds this useful, great!
- This script doesn't follow any standards or convetions. It's ugly, but it does the job. Feel free to make it beautiful :)

## Functionality
This is a simple Python script to calls the OpenSky REST API and store the recieved data in a SQLite Database.
- A new SQLite Database is initialized at the set path if none exists. If one is avaiable, the columnnames will be checked, if valid, the script begins.
- It has a simple queueing mechanic by generating timestamps for the last hour to call.
- It compares with your Database if a timestamp is already downloaded.
- You can set a timeinterval in seconds, if you don't want to call the API all the time.
- Since it uses the REST API, no additional libraries are needed.
- Simple Logging with logfile and print.

## How to use
1. Check if you're allowed to use the data for your usecase on https://opensky-network.org/ 
2. Read the API Documentation on https://opensky-network.org/apidoc/rest.html 
3. Create / Modify a JSON-file with your login-information for the Opensky-Network using the 'SAMPLE_.logindata.json'
  - Set your logininformation inside the JSON-File
  - Rename it to '.logindata.json'
4. (optional) Set a timeinterval for getting data. It is set as 'timeInterval' under '# General Params' inside 'OSDataRec.py'
5. Start 'OSDataRec.py' with python 3

### Licence
This Software is released AS IS under the MIT License
