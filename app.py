from flask import Flask, request, jsonify, Response
import json
from neo4j import GraphDatabase


app = Flask(__name__)
app.secret_key = "sasank_databaseConnection123"
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Admin1234"

# connecting to neo4j database
try:
        driver = GraphDatabase.driver(uri=URI, auth=(USER, PASSWORD))
        session = driver.session()
except Exception as ex:
    print('Error in connection to database server', ex, '\n')

def getQuery(json):
        query = """
                WITH $json as data
                UNWIND data as record
                MERGE (m:Movie {title:record.title}) ON CREATE
                SET m.show_id = record.show_id, m.type = record.type, m.release_year = record.release_year, m.duration = record.duration, m.description = record.description, m.rating = record.rating                        
                """
        if "cast" in json:
                query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.title AS t
                                UNWIND SPLIT(record.cast,',') AS a
                                WITH *, trim(a) AS a2
                                MERGE (:Person{name:a2})
                                WITH * 
                                MATCH(m:Movie{title:t}),(person:Person{name:a2})
                                MERGE (person)-[:ACTED_IN]->(m)
                        """
        if "director" in json:
                query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.title AS t
                                UNWIND SPLIT(record.director,',') AS d
                                WITH *, trim(d) AS d2
                                MERGE (:Person{name:d2})
                                WITH * 
                                MATCH(m:Movie{title:t}),(person:Person{name:d2})
                                MERGE (person)-[:DIRECTED]->(m)
                        """
        if "country" in json:
                query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.title AS t
                                UNWIND SPLIT(record.country,',') AS c
                                WITH *, trim(c) AS c2
                                MERGE (:Country{name:c2})
                                WITH * 
                                MATCH(m:Movie{title:t}),(country:Country{name:c2})
                                MERGE (country)<-[:DISTRIBUTED_IN]-(m)
                        """
        query = query + """
                RETURN collect(distinct m.title) AS Title"""
        return query



@app.route('/', methods=['GET', 'POST'])
def index():
        if request.method == 'GET':
                data = {
                        "available_dataoperation_info": {
                                "Create": {
                                               "action": "http://127.0.0.1:5000/title",
                                               "method": "POST"
                                        },
                                "Retrieve All": {
                                               "action": "http://127.0.0.1:5000/title",
                                               "method": "GET"
                                        },
                                "Update": {
                                                "action": "http://127.0.0.1:5000/title/<title>",
                                                "method": "PATCH"
                                },
                                "Delete": {
                                                "action": "http://127.0.0.1:5000/title/<title>",
                                                "method": "DELETE"
                                },
                                "Retrieve  By Title": {
                                                "action": "http://127.0.0.1:5000/title/<title>",
                                                "method": "GET"
                                }
                        }
                }
                return jsonify(data)
        else:
                return ({'Error': '401 Bad Request'})


@app.route('/title', methods=['POST'])
def create_netflix_document():
        try:
                request_data = request.get_json()
                title_list = []
                if request_data != {} and request_data != []:
                        if type(request_data) == list:
                                for each in request_data:
                                        query = getQuery(each)
                                        session = driver.session()
                                        nodes = session.run(query,json=each )
                                        nodes_data = list(nodes.data())
                                        title_list.append(nodes_data)
                        elif type(request_data) == dict:
                                query = getQuery(request_data)
                                session = driver.session()
                                nodes = session.run(query,json=request_data )
                                nodes_data = list(nodes.data())
                                title_list.append(nodes_data)

                        if title_list != []:
                                response = Response(f"Movies are Succesfully Created",status=200,mimetype='application/json')
                                return response 
                        else:  
                                response = Response("Error in Creation",status=200,mimetype='application/json')
                                return response        
                else:
                        response = Response("Error: Please Check JSON Data",status=500, mimetype='application/json')
                        return response

        except Exception as ex:
                response = Response(f"Error in Create Operation: {ex}", status=500, mimetype="application/json")
                return response 

@app.route('/title', methods=['GET'])
def retrieve_netflix_document():
        try:     
                query = """  
                        MATCH (m:Movie)
                        OPTIONAL MATCH (m:Movie)<-[:ACTED_IN]-(actor)
                        OPTIONAL MATCH (m:Movie)<-[:DIRECTED]-(director) 
                        OPTIONAL MATCH (country:Country)<-[:DISTRIBUTED_IN]-(m:Movie)
                        RETURN m as ShowInfo, collect(distinct actor.name) as Cast, collect(distinct director.name) as Director , collect(distinct country.name) as Country"""
                session = driver.session()
                nodes = session.run(query)
                nodes_data = list(nodes.data()) 
                print(nodes_data)
                result =list()
                for each in nodes_data:
                        movie_json = each['ShowInfo']
                        movie_json['cast'] = list(each['Cast'])
                        movie_json['director'] = list(each['Director'])
                        movie_json['country'] = list(each['Country'])
                        result.append(movie_json)
                if(len(movie_json) > 0):
                        return jsonify(result)
        except Exception as ex:
                response = Response(f"Error in Retrieve Operation: {ex}", status=500, mimetype="application/json")
                return response


@app.route('/title/<string:movie_title>', methods=['GET'])
def get_netflix_document_by_title(movie_title):
        try:     
                query = """  
                        MATCH (m:Movie)
                        WHERE toLower(m.title) CONTAINS toLower('{}')
                        OPTIONAL MATCH (m:Movie)<-[:ACTED_IN]-(actor:Person)
                        OPTIONAL MATCH (m:Movie)<-[:DIRECTED]-(director:Person) 
                        OPTIONAL MATCH (country:Country)<-[:DISTRIBUTED_IN]-(m:Movie)
                        RETURN m as ShowInfo, collect(distinct actor.name) as Cast, collect(distinct director.name) as Director , collect(distinct country.name) as Country"""
                session = driver.session()
                nodes = session.run(query.format(movie_title))
                nodes_data = list(nodes.data()) 
                print(nodes_data)
                result =list();
                for each in nodes_data:
                        movie_json = each['ShowInfo']
                        movie_json['cast'] = list(each['Cast'])
                        movie_json['director'] = list(each['Director'])
                        movie_json['country'] = list(each['Country'])
                        result.append(movie_json)
                if(len(result) > 0):
                        return jsonify(result)
                else:
                        response = Response(f"No Nodes are found with Movie title: {movie_title}", status=400)
                        return response
        except Exception as ex:
                response = Response(f"Error in Retrieve Operation: {ex}", status=500, mimetype="application/json")
                return response




@app.route('/title/<string:fname>', methods=['PATCH'])
def update_netflix_document(fname):
        try:   
                request_data = request.get_json()
                if request_data != {}:
                        query = """
                                MATCH (m:Movie {title: $fname})
                                Return m as ShowInfo
                                """
                        session = driver.session()
                        movie_node = session.run(query, fname = fname)
                        movie_data = list(movie_node.data())
                        if(movie_data != {} and movie_data !='' and movie_data != []):
                                for each in movie_data:
                                        movie_title = request_data['title'] if 'title' in request_data  else  each['ShowInfo']['title']
                                        movie_description = request_data['description'] if 'description' in request_data else each['ShowInfo']['description']
                                        movie_rating = request_data['rating'] if 'rating' in request_data else each['ShowInfo']['rating']
                                        updateQuery =   """
                                                        MATCH (m:Movie {title: $fname})
                                                        SET m.title = $movie_title, m.description = $movie_description, m.rating = $movie_rating
                                                        RETURN m as ShowInfo
                                                        """
                                        updated_node = session.run(updateQuery, fname=fname, movie_title = movie_title, movie_description = movie_description, movie_rating = movie_rating)
                                        updated_data = list(updated_node.data())
                                        return jsonify(updated_data)
                        
                        else:
                                response = Response(f"No Movie Found with title: {fname}", status=400, mimetype="application/json")
                                return response 
                else:
                        response = Response("No Raw JSON Found in Body to Update, please provide only title, description, rating to update", status=400, mimetype="application/json")
                        return response
        except Exception as ex:
                response = Response(f"Error in Update Operation: {ex}", status=500, mimetype="application/json")
                return response  
        


@app.route('/title/<string:movie_title>', methods=['DELETE'])
def delete_netflix_document(movie_title):
        try:
                query = """
                        MATCH (m:Movie {title: $filmName})
                        Return m as ShowInfo
                        """
                session = driver.session()
                movie_node = session.run(query, filmName = movie_title)
                movie_data = list(movie_node.data())
                if len(movie_data) > 0:
                        query = """
                                MATCH (m:Movie {title: $filmName})
                                DETACH DELETE m
                                """
                        session = driver.session()
                        session.run(query, filmName = movie_title)
                        query = """
                                MATCH (n)
                                WHERE size((n)--())=0
                                DELETE (n)
                        """
                        session.run(query)
                        response = Response(f"Movie {movie_title} is Deleted Successfully.", status=200, mimetype="application/json")
                        return response 
                else:  
                        response = Response(f"No Movie is found with title: {movie_title}.", status=500, mimetype="application/json")
                        return response
        except Exception as ex:
                response = Response(f"Error in Delete Operation: {ex}", status=500, mimetype="application/json")
                return response    

if __name__== '__main__':
        app.run(debug=True)
