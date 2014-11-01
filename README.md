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

## License

MIT: http://vineetdhanawat.mit-license.org/