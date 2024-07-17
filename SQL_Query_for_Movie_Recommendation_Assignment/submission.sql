
-- 1. Write an SQL query to return the total number of movies for each genre.
CREATE TABLE query1 AS
SELECT genres.name, count(*) AS moviecount
FROM genres
    JOIN hasagenre USING(genreid)
GROUP BY genres.name;

-- 2. Write an SQL query to return the average rating per genre.
CREATE TABLE query2 AS
SELECT genres.name, avg(rating) AS rating
FROM genres
    JOIN hasagenre USING (genreid)
    JOIN movies USING (movieid)
    JOIN ratings USING (movieid)
GROUP BY genres.name;

-- 3. Write an SQL query to return the movies that have at least 10 ratings.
CREATE TABLE query3 AS
SELECT movies.title, count(*) AS countofratings
FROM movies
    JOIN ratings USING (movieid)
GROUP BY movies.title
HAVING count(*) >= 10;

-- 4. Write a SQL query to return all “Comedy” movies, including movieid and title.
CREATE TABLE query4 AS
SELECT movies.movieid, movies.title
FROM movies
    JOIN hasagenre USING (movieid)
    JOIN genres USING (genreid)
WHERE genres.name = 'Comedy';

-- 5. Write an SQL query to return the average rating per movie.
CREATE TABLE query5 AS
SELECT movies.title, avg(ratings.rating) AS average
FROM movies
    JOIN ratings USING (movieid)
GROUP BY movies.title;

-- 6. Write a SQL query to return the average rating for all “Comedy” movies.
CREATE TABLE query6 AS
SELECT avg(ratings.rating) AS average
FROM ratings
    JOIN movies USING (movieid)
    JOIN hasagenre USING (movieid)
    JOIN genres USING (genreid)
WHERE genres.name = 'Comedy';

-- 7. Write a SQL query to return the average rating for all movies and each of these movies is both “Comedy” and “Romance”.
CREATE TABLE query7 AS
SELECT avg(ratings.rating) AS average
FROM ratings
    JOIN 
        (SELECT movies.movieid 
        FROM  movies JOIN hasagenre USING (movieid) JOIN genres USING (genreid)
        WHERE genres.name = 'Comedy' OR genres.name = 'Romance'
        GROUP BY movies.movieid
        HAVING count(DISTINCT genres.name) = 2)
    USING (movieid);

-- 8. Write a SQL query to return the average rating for all movies and each of these movies is “Romance” but not “Comedy”.
CREATE TABLE query8 AS
SELECT avg(ratings.rating) AS average
FROM ratings
    JOIN 
        (SELECT movies.movieid 
        FROM movies JOIN hasagenre USING (movieid) JOIN genres USING (genreid)
        WHERE genres.name = 'Comedy' OR genres.name = 'Romance'
        GROUP BY movies.movieid
        HAVING count(DISTINCT genres.name) = 1 AND max(genres.name) = 'Romance')
    USING (movieid);

-- 9. Find all movies that are rated by a user such that the userId is equal to v1. The v1 will be an integer parameter passed to the SQL query.
CREATE TABLE query9 AS
SELECT movies.movieid, ratings.rating
FROM movies
    JOIN ratings USING(movieid)
WHERE ratings.userid = :v1;