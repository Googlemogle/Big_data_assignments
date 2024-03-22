insert into
    `//home/student/assignments/checkout_freq/output`
with truncate
select
    `userid`,
    count(*) as frequency
from
    `//home/student/logs/user_activity_log`
where
    `action` == "checkout"
group by
    `userid`
;