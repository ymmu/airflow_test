use {{ params.db }};

SET @time_ = '{{ params.start_t }}'; # '2017-09-01 15:00:00'

INSERT INTO artist_track_rank_hourly

# 1. Lag 함수로 diff 작업 준비 (next_t)  -----------
WITH cte AS(
SELECT
	user_name,
	track_name,
	recording_msid,
	artist_name,
	artist_msid,
	listened_at,
	LEAD(listened_at) over(PARTITION BY user_name ORDER BY listened_at) AS next_t
	#DATE(listened_at) AS date_,
	#EXTRACT(HOUR FROM listened_at) AS hour_
FROM sample.listenbrainz
WHERE listened_at > DATE_SUB(@time_, INTERVAL 1 HOUR) AND listened_at < @time_ #DATE_ADD(@time_, INTERVAL 1 HOUR)
#LIMIT 0,40000
),

# 2. 가장 많이 플레이된 아티스트 랭크  -----------
artist_ranks
AS(
SELECT
	artist_msid,
	artist_name, # 아티스트 이름
DENSE_RANK() over (
ORDER BY num_artist DESC) AS artist_rank, # 플레이 순 artist 랭크
	num_artist # 플레이 된 횟수
FROM(
	SELECT
		artist_msid,
		artist_name, COUNT(artist_msid) num_artist # 이 아티스트 곡이 플레이 된 횟수
	FROM(
		SELECT
			artist_msid,
			artist_name, TIMESTAMPDIFF(SECOND, listened_at, next_t) AS diff_t #곡이 플레이 된 시간
		FROM cte
	) a
	WHERE a.diff_t < 3600 AND a.diff_t > 30 # 30초 이상 6분 이하 플레이 된
	GROUP BY artist_msid
	ORDER BY num_artist DESC
) b
),


# 테스트
# SELECT * FROM artist_ranks


# 3. 아티스트 당 가장 많이 플레이 된 곡  -----------
num_track AS (
SELECT
	artist_msid,
	#artist_name,
	recording_msid,
	track_name,
COUNT(track_name) num_track_played
	#COUNT(*) over (PARTITION BY artist_msid, track_name) num_track
FROM cte
GROUP BY artist_msid, track_name
)

# 테스트
# SELECT * FROM num_track


# 4. 위의 artist rank와 track 플레이 정보를 가져와 artist 플레이 랭크, artist당 가장 플레이 많이 된 곡 가져오기
SELECT *
FROM(
	SELECT
		DATE(@time_) AS date_,
		HOUR(@time_) AS hour_,
		ar.artist_msid,
		#ar.artist_name,
		ar.artist_rank,
		nt.recording_msid,
		#nt.track_name,
		nt.num_track_played,
		DENSE_RANK() over (ORDER BY nt.num_track_played DESC) AS total_track_rank,
		DENSE_RANK() over (PARTITION BY artist_msid ORDER BY nt.num_track_played DESC) AS artist_track_rank
	FROM artist_ranks ar
	LEFT JOIN num_track nt ON ar.artist_msid = nt.artist_msid
	ORDER BY ar.artist_rank, nt.num_track_played DESC
	#GROUP BY ar.artist_msid, nt.track_name
) temp
WHERE temp.artist_track_rank = 1 #artist 별 가장 플레이가 많이 된 곡만 선별 (rank1 위)

