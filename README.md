# CSE511-S2024
Data Processing at Scale (CSE 511) class's master repository for Summer 2024.

<img src="Image/1.png" height=300px />

## 1. Create_Movie_Recommendation_Database
* users: userid (int, primary key), name (text)
* movies: movieid (integer, primary key), title (text)
* taginfo: tagid (int, primary key), content (text)
* genres: genreid (integer, primary key), name (text)
* ratings: userid (int, foreign key), movieid (int, foreign key), rating (numeric), timestamp (bigint, seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970)
* tags: userid (int, foreign key), movieid (int, foreign key), tagid (int, foreign key), timestamp (bigint, seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970).
* hasagenre: movieid (int, foreign key), genreid (int, foreign key)

## 2. SQL_Query_for_Movie_Recommendation
* Write an SQL query to return the total number of movies for each genre.
* Write an SQL query to return the average rating per genre.
* Write an SQL query to return the movies that have at least 10 ratings.
* Write a SQL query to return all “Comedy” movies, including movieid and title.
* Write an SQL query to return the average rating per movie.
* Write a SQL query to return the average rating for all “Comedy” movies.
* Write a SQL query to return the average rating for all movies and each of these movies is both
“Comedy” and “Romance”.
* Write a SQL query to return the average rating for all movies and each of these movies is
“Romance” but not “Comedy”.
* Find all movies that are rated by a user such that the userId is equal to v1. The v1 will be an
integer parameter passed to the SQL query.

## 3. Data_Fragmentation
1. Implement a Python function loadRatings() that takes a file system path that contains the
ratings.dat file as input. loadRatings() then load the ratings.dat content into a table (saved in
PostgreSQL) named ‘ratings’ that has the following schema:
userid(int) – movieid(int) – rating(float
2. Implement a Python function rangePartition() that takes as input: (1) the ‘ratings’ table stored
in PostgreSQL and (2) an integer value N; that represents the number of partitions.
rangePartition() then generates N horizontal fragments of the ‘ratings’ table and stores them
in PostgreSQL. The algorithm should partition the ‘ratings’ table based on N uniform ranges of
the rating attribute.
3. Implement a Python function roundRobinPartition() that takes as input: (1) the ‘ratings’ table
stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. The
function then generates N horizontal fragments of the ‘ratings’ table and stores them in
PostgreSQL. The algorithm should partition the ‘ratings’ table using the round-robin partitioning
approach (explained in class).
4. Implement a Python function roundrobininsert() that takes as input: (1) ‘ratings’ table stored
in PostgreSQL, (2) userid, (3) movieid, (4) rating. roundrobininsert() then inserts a new tuple
to the ‘ratings’ table and the right fragment based on the round-robin approach.
5. Implement a Python function rangeinsert() that takes as input: (1) ‘ratings’ table stored in
Postgresql (2) userid, (3) movieid, (4) rating. rangeinsert() then inserts a new tuple to the
‘ratings’ table and the correct fragment (of the partitioned ratings table) based upon the Rating
value.
## 4. Query_Processing
1. Implement a Python function RangeQuery that takes as input: (1) ratings table stored in
PostgreSQL, (2) RatingMinValue, (3) RatingMaxValue, and (4) openconnection.
RangeQuery() then returns all tuples for which the rating value is larger than or equal to
RatingMinValue and less than or equal to RatingMaxValue.
2. Implement a Python function PointQuery that takes as input: (1) ratings table stored in
PostgreSQL, (2) RatingValue, and (3) openconnection.
PointQuery() then returns all tuples for which the rating value is equal to RatingValue.
## 5. NoSQL
To implement the functions provided in the Jupyter Notebook to perform the operations as listed below.
* FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection) – This function searches the ‘collection’ given to find all the business present in the city provided in ‘cityToSearch’ and save it to ‘saveLocation1’. For each business you find, you should store the name, full address, city, and state of the business in the following format. Each line of the saved file will contain: Name$FullAddress$City$State.

* FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection) – This function searches the ‘collection’ given to find the name of all the businesses present in the ‘maxDistance’ from the given ‘myLocation’ and save them to ‘saveLocation2’. Each line of the output file will contain the name of the business only.

## 6. Hot_Spot_Analysis
To do spatial hot spot analysis. In particular, to complete two different hot spot analysis tasks.
1. Hot Zone Analysis: This task will need to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it includes more points. So this task is to calculate the hotness of all the rectangles.

2. Hot Cell Analysis: This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.
* [Problem Definition page](https://sigspatial2016.sigspatial.org/giscup2016/problem)
* [Submit Format page](https://sigspatial2016.sigspatial.org/giscup2016/submit)
