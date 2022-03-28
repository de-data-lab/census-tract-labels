# Census Tract Generator for random address


## Installation
You can clone the repository via `git`
```
https://github.com/de-data-lab/census-tract-labels.git
```

To install all dependencies use ```requirements.txt``` to avoid any package conflicts
The following command will install the packages according to the configuration file
```
$ pip install -r requirements.txt
```

## Example Run

```
List_of_addresse = []  
#create a class image  
class_copy = censusTractLabel(List_of_addresse)  
#run the method  
output = class_copy.census_tract_addresses()  
#the process the output list as you wish  
```

## API
Run `apy.py` to start the API app.

```
python app.py
```

The app will be running at `localhost:8000`.
The API entrypoint is `localhost:8000/tracts`. The entry point can accept an argument `address` in a string format.

For example, You can query the census tract inforamtion about the adress, 411 Legislative Ave, Dover, DE, by:
```
localhost:8000/tracts?address=411 Legislative Ave, Dover, DE
```

The results will look like this:

```
input addresses	:	411 Legislative Ave, Dover, DE
matched addresses	:	411 LEGISLATIVE AVE, DOVER, DE, 19901
lat	:	39.157505
lon	:	-75.52055
state	:	DE
county	:	Kent County
census tract	:	041300
block group	:	2
block	:	2016
FIPS	:	100010413002016
GEOID	:	10001041300
```