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

### Pig (Assume dataset on hdfs)
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

## License

MIT: http://vineetdhanawat.mit-license.org/