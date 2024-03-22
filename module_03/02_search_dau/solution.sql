insert into
    `//home/student/assignments/search_dau/output`
with truncate
select
    `date`,
    count(*) as users
from
    (
    select distinct
        `userid`,
        DateTime::MakeDate(DateTime::Split(`timestamp`)) as `date`,
        `action`
    from
        `//home/student/logs/user_activity_log`
    where
        `action` == "search"
    )
group by
    `date`
;