Movies = load '/Fall2014/movies_new.dat' using PigStorage('#') as (MovieID:chararray, Title:chararray, Genres:chararray);
Ratings = load '/Fall2014/ratings_new.dat' using PigStorage('#') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);
Users = load '/Fall2014/users_new.dat' using PigStorage('#') as (UserID:chararray, Gender:chararray, Age:int, Occupation:chararray, Zipcode:chararray);

MovieRatings = join Movies by(MovieID), Ratings by(MovieID);
WarAction = filter MovieRatings by (Genres matches '.*War.*' and Genres matches '.*Action.*');

groupA = group WarAction by $0;
groupB = foreach groupA generate flatten(group), AVG(WarAction.$5);
groupdesc = order groupB by $1 desc;

Limitdesc = limit groupdesc 1;
A= foreach Limitdesc generate $1;

jmovie= join groupB by ($1), A by ($0);
jratings = join Ratings by(MovieID), jmovie by($0);
juserratings = join jratings by($0), Users by($0);

Age = filter juserratings by (Gender matches '.*F.*' and (Age > 20 AND Age < 35) and Zipcode matches '1.*');
final_userID = foreach Age generate $0;
final = distinct final_userID;
dump final;