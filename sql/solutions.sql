/* 
For the ingested commits, determine the top 5 committers ranked by count of commits and their number of commits.
*/
select tbl.* from (
	SELECT
		distinct
		author_login,
		(select distinct t.name from branch_table t where t.branch_id = t1.branch_id) as branch_name,
        /* Use row_number() just to ensure it will only display 5 records for each branch */
		row_number() over (partition by branch_id order by branch_id asc, count(1) desc) as ranking, 
		count(1) as total_count
	FROM
		commit_table t1
	group by
		branch_id, author_login
	order by
		ranking asc
) tbl
where
    tbl.ranking <= 5
order by
    tbl.branch_name, tbl.ranking asc


/* 
For the ingested commits, determine the committer with the longest commit streak.
*/
select
	author_login,
	branch_id,
	(select distinct t.name from branch_table t where t.branch_id = tbl.branch_id) as branch_name,
	# group_set,
	count(1) as total_streak
from (
	SELECT 
		author_login,
		branch_id,
		rank() over (partition by branch_id order by branch_id, commit_datetime asc) -
		rank() over (partition by branch_id, author_login order by branch_id, commit_datetime asc) as group_set,
		commit_datetime
	FROM
		commit_table
	order by
		branch_id, commit_datetime asc
) tbl
group by
	author_login, branch_id, group_set  
ORDER BY total_streak DESC;



/* 
For the ingested commits, generate a heatmap of number of commits count by all users by day of the week and by 3 hour blocks.
*/
select 
	day_of_week,
	SUM(if(hour_section=0, 1, 0)) as "00-03",
	SUM(if(hour_section=1, 1, 0)) as "03-06",
	SUM(if(hour_section=2, 1, 0)) as "06-09",
	SUM(if(hour_section=3, 1, 0)) as "09-12",
	SUM(if(hour_section=4, 1, 0)) as "12-15",
	SUM(if(hour_section=5, 1, 0)) as "15-18",
	SUM(if(hour_section=6, 1, 0)) as "18-21",
	SUM(if(hour_section=7, 1, 0)) as "21-00"
	, count(1) as total_commit
from (
	select
		distinct /* Use distinct as same commit can be in different branch */
		commit_sha,
        /* Ensure the heatmap starts from monday */
		WEEKDAY(commit_datetime) as week_day,
		SUBSTR(DAYNAME(commit_datetime), 1, 3) as day_of_week,
		HOUR(commit_datetime) as commit_hour,
		FLOOR(HOUR(commit_datetime)/3) as hour_section
	from commit_table
) tbl
group by day_of_week
order by week_day asc
