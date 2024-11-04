-- The outputs folder is refreshed with new outputs at the start of this script.

fs -rm -f -r -R CSC1109-Spotify-cleaning-and-analysis/outputs;



/*

Cleaning Spotify dataset

1. Load dataset.csv.
2. Remove the index column that is included in the dataset.
3. Filter out the header from the csv using a filter on each field.
4. Filter out rows containing nulls for the column 'artists'.
5. Remove duplicate rows in the dataset.

This cleaned tsv file will be saved in the outputs directory for use later on.

*/



-- 1. Load the Spotify dataset and check the structure.
tracks = LOAD 'CSC1109-Spotify-cleaning-and-analysis/data/dataset.tsv' USING PigStorage('\t') AS (index:int, track_id:chararray, artists:chararray, album_name:chararray, track_name:chararray, popularity:int, duration_ms:int, explicit:chararray, danceability:float, energy:float, key:int, loudness:float, mode:int, speechiness:float, acousticness:float, instrumentalness:float, liveness:float, valence:float, tempo:float, time_signature:int, track_genre:chararray);
first_5_tracks = LIMIT tracks 5;
-- DUMP first_5_tracks;
-- Checking the first 5 rows of the dataset. 




-- 2. Remove the index column, because it will not be useful for analysis.
tracks = foreach tracks generate track_id, artists, album_name, track_name, popularity, duration_ms, explicit, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature, track_genre;
track_columns = LIMIT tracks 1;
-- DUMP track_columns;
-- The output shows that the index column has been removed.





-- 3. Filter out the header from the dataset using a FILTER on the first column.
filter_out_header = FILTER tracks BY (track_id != 'track_id');
first_rows = LIMIT filter_out_header 1;
-- DUMP first_rows;
-- Can see the first row of the dataset without the header.



-- 4. Filter out rows containing nulls for the column 'artists', the columns 'artists', 'album_name' and 'track_name' are important for data analysis, so these columns should not have nulls
clean_tracks = FILTER filter_out_header BY (artists IS NOT NULL) OR (album_name IS NOT NULL) OR (track_name IS NOT NULL);

grouped_tracks = GROUP filter_out_header ALL;
grouped_clean_tracks = GROUP clean_tracks ALL;

count_tracks = foreach grouped_tracks generate COUNT(filter_out_header.track_id);
-- DUMP count_tracks;
-- The initial dataset has 114000 rows.



count_clean_tracks = foreach grouped_clean_tracks generate COUNT(clean_tracks.track_id);
-- DUMP count_clean_tracks;
-- The dataset has 113999 rows remaining, which means that there was only one tuple containing nulls for these columns.



-- 5. Remove duplicates, if any, from the data.

unique_rows = DISTINCT clean_tracks; 
group_unique_rows = GROUP unique_rows ALL;

count_unique_rows = foreach group_unique_rows generate COUNT(unique_rows.track_id);
-- DUMP count_unique_rows; 
-- After removing duplicates, there dataset has 113549 rows remaining.


-- STORE unique_rows INTO 'CSC1109-Spotify-cleaning-and-analysis/outputs/clean_tracks' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
-- Store a csv file for later use. 

