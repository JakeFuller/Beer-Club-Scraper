#! python3
# beerscraper.py
# --push -> accepts list of links as argument, breaks up links, runs search term through untappd api, adds to mongodb
#TODO: Add proper comment documentation


import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from pprint import pprint
import requests, json, numpy as np

load_dotenv()
client = MongoClient(os.getenv('DATABASE_URL'))
db = client.beerClub

def read_links(linkArg):
    """
    Open a file in read mode...
    Add each line in file to array...
    Return an array.
    """
    linkList = linkArg.replace("[","").replace("]","").split(",")
    return linkList

def get_path(linkList):
    """
    Take an array...
    Split indices at the right-most '/'...
    Return an array.
    """
    pathNames = []
    for links in linkList:
        path = links.rsplit("/", 1)
        pathNames.append(path[1][:20])
    return(pathNames)

def get_brewery_domain(linkList):
    """
    Input array of urls...
    Split urls at "." (.com) and "/" (https://)...
    Add first 3 letters of website to array...
    Return array
    """
    breweryNames = []
    for links in linkList:
        urlSplit1 = links.rsplit(".", 2)
        urlSplit2 = urlSplit1[0].rsplit("/", 1)
        breweryNames.append(urlSplit2[1][:3])
    return breweryNames

def remove_character(stringArray, character):
    """
    Take an array...
    Replace any '-' with a space...
    Return an array.
    """
    cleanArray = []
    for string in stringArray:
        cleanText = string.replace(character, " ")
        cleanArray.append(cleanText)
    return(cleanArray)

def make_search_term(beerName, brewery):
    """
    Input 2 arrays...
    Combine 1D arrays into 2D array...
    Create new array combining terms from 2D array
    Return array of strings
    """
    searchTerms = np.vstack((beerName, brewery)).T
    searchStrings = []
    for i in range(0, len(searchTerms)):
        searchStrings.append(searchTerms[i][0] + " " + searchTerms[i][1])
    return(searchStrings)

def get_search(searchTerm):
    """
    Run search term through untappd api...
    Return response as json
    """
    response = requests.get(f"https://api.untappd.com/v4/search/beer?q={searchTerm}&sort=name&" + os.getenv('UNTAPPD_URL'))
    return(response.json())

def scrape_json(beerSearchData, beerLink):
    """
    Input dictionary (json) & beerLink(str)...
    Fetch beer name/style/abv/ibu/breweryname/breweryloc...
    Of top search result only...
    Return dictionary inclduding beerLink
    """
    beers = beerSearchData['response']['beers']['items'][0]
    beerDict = {
            "name" : beers['beer']['beer_name'],
            "style" : beers['beer']['beer_style'],
            "abv" : beers['beer']['beer_abv'],
            "ibu" : beers['beer']['beer_ibu'],
            "brewery_name" : beers['brewery']['brewery_name'],
            "brewery_city" : beers['brewery']['location']['brewery_city'],
            "brewery_country" : beers['brewery']['country_name'],
            "bid" : beers['beer']['bid'],
            "url" : beerLink
            }
    return beerDict

def db_insert_beer(dict):
    """
    Input dictionary of JSON...
    Push to database beers colection...
    Create new if non-existant based on bid...
    Update old if existant.
    """
    db.beers.replace_one({"bid" : dict["bid"]}, dict, upsert=True)

def db_id_by_url(url):
    """
    Input url as string...
    Find _id of beer with that url...
    Return found _id
    """
    beer = db.beers.find_one({"url" : url})
    return beer.get("_id")

def db_create_set(date):
    """
    Input date...
    Find the highest (newest) set by set_id...
    Create new set in sets collection with +1 set_id...
    """
    beerIds = []
    if db.sets.count() == 0:
        maxId = 0
    else:
        newestSet = db.sets.find().sort("set_id", -1).limit(1)
        for sets in newestSet: maxId = sets["set_id"]
    setData = {
        "set_id" : maxId+1,
        "date" : date,
        "current_set" : 0,
        "beer_ids" : beerIds
    }
    newSet = db.sets.insert_one(setData)
    print(f"Set created with _id: {newSet.inserted_id} and set_id: {maxId+1}. Scheduled for date: {date}")

def db_insert_set(setId, beerId):
    """
    Input set_id and beer _id...
    Push beer _id to beeer_ids in set document...
    """
    db.sets.update_one({"set_id" : setId}, {"$push": {"beer_ids": beerId}}, upsert = True)

def db_delete_beer(beerId):
    """
    Input bin...
    Delete document with bin from beers collection
    """
    db.beers.delete_one({"bin" : beerId})

def db_delete_set(setId):
    """
    Input set_id...
    Delete set with set_id from sets collection
    """
    db.sets.delete_one({"set_id" : setId})

def db_delete_set_beer(setId, beerId):
    """
    Input set_id and beer _id...
    Delete beer _id from beer_ids in set document
    """
    db.sets.update_one({"set_id" : setId}, {"$pull": {"beer_ids" : beerId}})

def db_find_current_set():
    """
    Print the set_id of set where current_set : 1
    """
    currentSet = db.sets.find_one({"current_set" : 1})
    print(currentSet["set_id"])

def db_set_current_set(setId):
    """
    Input set_it of set collection...
    Change current_set to 0 where current_set is 1...
    Change current_set to 1 in set with set_id...
    """
    db.sets.update_one({"current_set" : 1}, {"$set":{"current_set" : 0}})
    db.sets.update_one({"set_id" : setId}, {"$set": {"current_set" : 1}})

def main():
    if sys.argv[1] == "--push":
        #beerscraper.py [--push] [--beers] [links]
        if sys.argv[2] == "--beers":
            beerLinks = read_links(sys.argv[3])
            beerNames = get_path(beerLinks) # www.example.com/[beer] <--
            cleanBeer = remove_character(beerNames, "-") # paths have - instead of spaces
            breweryNames = get_brewery_domain(beerLinks) #www.[example].com/beer <--
            searchTerm = make_search_term(cleanBeer, breweryNames)
            for i in range(0, len(cleanBeer), 1):
                beerSearch = get_search(searchTerm[i])
                beerInfo = scrape_json(beerSearch, beerLinks[i]) #also adds the url to the json information
                pprint(beerInfo)
                db_insert_beer(beerInfo)
            print("Complete!")
            print(searchTerm)

        #beerscraper.py [--push] [--set] [set id] [links]
        elif sys.argv[2] == "--set":
            beerLinks = read_links(sys.argv[4])
            for links in beerLinks:
                beerId = db_id_by_url(links)
                db_insert_set(int(sys.argv[3]), beerId)

    #beerscraper.py [--new] [--set] [date (YYY-MM-DD)]
    elif sys.argv[1] == "--new":
        if sys.argv[2] == "--set":
            db_create_set(sys.argv[3])

    #beerscraper.py [--del] [--beer, --set, --set-beer] [url or set id] [url if set beer]
    elif sys.argv[1] == "--del":
        if sys.argv[2] == "--beer":
            db_delete_beer(sys.argv[3])
        elif sys.argv[2] == "--set":
            db_delete_set(int(sys.argv[3]))
        elif sys.argv[2] == "--set-beer":
            beerLinks = read_links(sys.argv[4])
            for links in beerLinks:
                beerId = db_id_by_url(links)
                db_delete_set_beer(int(sys.argv[3]), beerId)

    #beerscraper.py [--curent] [set id if changing current]
    elif sys.argv[1] == "--current":
        if len(sys.argv) < 3:
            db_find_current_set()
        else:
            db_set_current_set(int(sys.argv[2]))

    else:
        print("Wrong command")

if __name__ == '__main__':
    main()




