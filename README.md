# Big Data Analytics and Management
Implement several map reduce design patterns to derive some statistics from IMDB movie data using Hadoop framework. Coursework CS 6301

## Assignments
### Dataset
- Licence and Readme included inside the folder
- ratings.dat UserID::MovieID::Rating::Timestamp
- users.dat UserID::Gender::Age::Occupation::Zip-code
- movies.dat MovieID::Title::Genres

### Map Reduce Design
1. Given a input zipcode, find all the user-ids that belongs to that zipcode. You must take the input zipcode in command line.
2. Find top 10 average rated movies with descending order of rating.
3. Find all the user ids who has rated at least n movies.
4. Given some movie titles in csv format - find all the genres of the movies.
5. Find the top 10 zipcodes based on the average age of users belong to that zipcode, in the ascending order of the average age.

### Map Reduce Joins
1. Given a movieID as input, Find the number of male users who has rated that movie using map side join.
2. Find top 10 average rated movie names with descending order of rating using reduce side join.
3. Find the top 10 users (userID, age, gender) who has rated most number of movies in descending order of the counts.

### Pig (Assume dataset available on hdfs)
1. List the unique userid of female users whose age between 20-35 and who has rated the highest rated Action AND War movies. (You should consider all movies that has Action AND War both in its genre list) Print only users whose zip starts with 1.
2. Implement cogroup command on UserID for the datasets ratings_new and users_new. Print first 11 rows.
3. Repeat above (implement join) with cogroup commands. Print first 11 rows.
4. Using Pig Latin script, use the FORMAT_GENRE function on movies_new dataset and print the movie name with its genre(s). Write a UDF (User Define Function) FORMAT_GENRE in Pig which basically formats the genre in movies_new in the following

		Before formatting:  Children's
		After formatting:  Children's <ID>
		Before formatting:  Animation|Children's
		After formatting:  Children's & Animation <ID>
		Before formatting:  Children's|Adventure|Animation
		After formatting:  Children's, Adventure & Animation <ID>

### Hive
1. Using Hive script, find  top 11 average rated "Action" movies with descending order of rating. (Show the create table command, load from local, and the Hive query).
2. Using Hive script,  List all the movies with its genre where the movie genre is Action or Drama and the average movie rating is in between 4.4 - 4.9 and only the male users rate the movie. (Show the create table command, load from local, and the Hive query).
3. Dataset three files (2009, 2010, 2011). Using Hive script, create one table partitioned by year. (Show the create table one command, load from local three commands, and one Hive query that selects all columns from the table for the virtual column year of 2009).
4. Create three tables that have three columns each (MovieID, MovieName, Genre). Each table will represent a year. The three years are 2009, 2010 and 2011.
Using Hive multi-table insert, insert values from the table you created in Q4 to these three tables (each table should have names of movies e.g. movies_2009 etc. for the specified year).
5. Using Hive script, use the FORMAT_GENRE function on movies_new dataset and print the movie name with its genre(s). Write a UDF(User Define Function) FORMAT_GENRE in Hive which basically formats the genre in movies_new in the following

		Before formatting:  Children's
		After formatting:  Children's - <ID>
		Before formatting:  Animation|Children's
		After formatting:  Children's, & Animation - <ID>
		Before formatting:  Children's|Adventure|Animation
		After formatting:  Children's, Adventure, & Animation - <ID>

### Cassandra

Part I

- Using Cassandra CLI, write commands to do the following.
- Create a COLUMN FAMILY for this dataset.
- Insert the following to the column family created in step 1. Use MovieID as the key.
	- "70#From Dusk Till Dawn (1996)#Action|Comedy|Crime|Horror|Thriller"
	- "83#Once Upon a Time When We Were Colored (1995)#Drama"
	- "112#Escape from New York (1981)#Action|Adventure|Sci-Fi|Thriller" with time to live (ttl) clause after 300 seconds
- Show the following:
	- Get the movie name and genre for the movie id 70 ?
	- Retrieve all rows and columns.
	- Delete column Genres for the movie id 83.
	- Drop the column family.
- Use describe keyspace command with your netid and show content.

Part II

- Using Cassandra CQL3, write commands to do the following.
- Create a table for this dataset. Use (MovieID) as the Primary Key.
- Load all records in the dataset to this table.
- Insert record “1162#New Comedy Movie#Comedy" to the table.
- Select the tuple which has movie id 1150
- Delete all rows in the table.
- Drop the table.

Part III

- Run nodetool command and determine how much unbalanced the cluster is.

### Spark

Part I Scala

1. Given a input zipcode, find all the user-ids that belongs to that zipcode. You must take the input zipcode in command line.
2. Find top 10 average rated movies with descending order of rating.

Part II Mahout and Recommendation

- Apply item-based collaborative filtering using mahout’s spark-itemsimilarity. spark-itemsimilarity can be used to create "other people also liked these things" type recommendations.
- Construct the item-similarity matrix of each movie having rating 4 (use ratings.dat)
- Using Apache spark interactive shell. Take the user id as input. Now finds all the movies that he rates as 4.
- Find the movies that match with the user’s rated movies with the key of the item-similarity file.
- Reference https://mahout.apache.org/users/recommender/intro-cooccurrence-spark.html 

## License

MIT: http://vineetdhanawat.mit-license.org/