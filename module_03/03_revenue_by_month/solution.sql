$format = DateTime::Format("%Y-%m");

insert into
    `//home/student/assignments/revenue_by_month/output`
with truncate
select
    `month`,
    sum(`value`) as `value`
from
    (
    select
        `action`,
        `value`,
        $format(DateTime::Split(`timestamp`)) as `month`
    from
        `//home/student/logs/user_activity_log`
    where
        `action` == "confirmation"
    )
group by
    `month`
;