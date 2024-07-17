
CREATE TABLE users (
    userid INT,
    name TEXT,
    PRIMARY KEY (userid)
);

CREATE TABLE movies (
    movieid INT,
    title TEXT,
    PRIMARY KEY (movieid)
);

CREATE TABLE taginfo (
    tagid INT,
    content TEXT,
    PRIMARY KEY (tagid)
);

CREATE TABLE genres (
    genreid INT,
    name TEXT,
    PRIMARY KEY (genreid)
);

CREATE TABLE ratings (
    userid INT,
    movieid INT,
    rating NUMERIC CHECK (rating >= 0 AND rating <= 5),
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users(userid),
    FOREIGN KEY (movieid) REFERENCES movies(movieid)
);

CREATE TABLE tags (
    userid INT,
    movieid INT,
    tagid INT,
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid, tagid),
    FOREIGN KEY (userid) REFERENCES users(userid),
    FOREIGN KEY (movieid) REFERENCES movies(movieid),
    FOREIGN KEY (tagid) REFERENCES taginfo(tagid)
);

CREATE TABLE hasagenre (
    movieid INT,
    genreid INT,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY (movieid) REFERENCES movies(movieid),
    FOREIGN KEY (genreid) REFERENCES genres(genreid)
);