REGISTER /home/FORMAT_GENRE_PIG.jar;
Movies = load '/Fall2014/movies_new.dat' using PigStorage('#') as (MovieID:chararray, Title:chararray, Genres:chararray);
final = FOREACH Movies GENERATE Title, FORMAT_GENRE_PIG(Genres);
DUMP final;