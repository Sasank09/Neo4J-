> These are the basic APIs to perform at most functions on web applications. Configuring the backend web framework using Python Flask connect to Neo4J to perform crud operations on graphdatabase. To test the backend web framework, you must use API platforms such as Postman API Platform(https://www.postman.com).
> The CSV file (Netflix.csv-Movie and Show on Netflix) is provided in this assignment with the column names including: 

---
```
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Admin1234"
# connecting to neo4j database
try:
        driver = GraphDatabase.driver(uri=URI, auth=(USER, PASSWORD))
        session = driver.session()
except Exception as ex:
    print('Error in connection to database server', ex, '\n')


In app.py file, performed the following main functions: 
1.	Insert the new movie and show. 
@app.route('/title', methods=['POST'])

2.	Update the movie and show information using title. (By update only title, description, and rating)
@app.route('/title/<string:fname>', methods=['PATCH'])

3.	Delete the movie and show information using title.
@app.route('/title/<string:fname>', methods=['DELETE'])

4.	Retrieve all the movies and shows in database.
@app.route('/title', methods=['GET'])

5.	Display the movie and show’s detail includes actors, directors and distributed country using title.
@app.route('/title/<string:fname>', methods=['GET'])

```

Please Install the following before executing
- [Flask](https://flask.palletsprojects.com/en/2.2.x/installation/)
- [neo4j](https://pypi.org/project/neo4j/)
