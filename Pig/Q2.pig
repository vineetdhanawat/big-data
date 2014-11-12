Users = load '/Fall2014/users_new.dat' using PigStorage('#') as (UserID:chararray, Gender:chararray, Age:int, Occupation:chararray, Zipcode:int);
Ratings = load '/Fall2014/ratings_new.dat' using PigStorage('#') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);

UsersRatings = cogroup Users by (UserID), Ratings by (UserID);
final = limit UsersRatings 11;
dump final;