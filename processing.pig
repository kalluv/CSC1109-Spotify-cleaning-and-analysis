


-- Load cleaned Spotify dataset

tracks = LOAD 'CSC1109-Spotify-cleaning-and-analysis/outputs/clean_tracks/part-r-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (track_id:chararray, artists:chararray, album_name:chararray, track_name:chararray, popularity:int, duration_ms:int, explicit:chararray, danceability:float, energy:float, key:int, loudness:float, mode:int, speechiness:float, acousticness:float, instrumentalness:float, liveness:float, valence:float, tempo:float, time_signature:int, track_genre:chararray);




-- 1. Top 10 Most Popular Genres Based on Average Popularity of Tracks


-- Group By Genre
group_by_genre = GROUP tracks BY track_genre;

-- Get Average Popularity and Number of Tracks for each Genre
avg_popularity_by_genre = foreach group_by_genre generate group as genre, AVG(tracks.popularity) as avg_popularity, COUNT(tracks.track_id) as count_tracks;

-- Filter Genres by Number of Tracks greater than 10

avg_popularity_by_genre = FILTER avg_popularity_by_genre BY count_tracks >= 10;

-- Order By Average Popularity DESC
avg_popularity_by_genre = ORDER avg_popularity_by_genre BY avg_popularity DESC;

-- Get Top 10
top_genre = LIMIT avg_popularity_by_genre 10;

top_genre = foreach top_genre generate genre, avg_popularity;


-- Dump the results
DUMP top_genre;





-- 2.  Top 10 Solo Artists with the Most Tracks
-- Filter solo artists (no ";" in the artist field)
solo_artists = FILTER tracks BY NOT (artists MATCHES '.*;.*');

-- Group by artist
grouped_by_artists = GROUP solo_artists BY artists;

-- Count tracks per artist
artist_track_count = FOREACH grouped_by_artists GENERATE group AS artist, COUNT(solo_artists) AS track_count;

-- Filter artists with more than 5 tracks
filtered_artists = FILTER artist_track_count BY track_count > 5;

-- Order by track count
ordered_artists = ORDER filtered_artists BY track_count DESC;

-- Take the top 10
top_10_artists = LIMIT ordered_artists 10;

-- Dump the results
DUMP top_10_artists;
