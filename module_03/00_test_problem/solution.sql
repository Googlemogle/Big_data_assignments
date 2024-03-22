-- Напишите SQL-запрос, находящий всех уникальных пользователей, которые перестали использовать сайт в мае
-- (совершивших последнее действие с 1 по 31 мая 2022 г. включительно).

-- Результат запишите в таблицу `//home/student/assignments/churned_in_may/output` в следующем формате:
--   * `userid: str` — идентификатор пользователя;
--   * `date: date`  — дата последнего посещения сайта.
insert into
    `//home/student/assignments/churned_in_may/output`
with truncate
select
    `userid`,
    `date`
from
    (
    select
        `userid`,
        max(DateTime::MakeDate(DateTime::Split(`timestamp`))) as `date`
    from
        `//home/student/logs/user_activity_log`
    group by
        `userid`
    )
where
    `date` >= DateTime::MakeDate(DateTime("2022-05-01T00:00:00Z")) and
    `date` < DateTime::MakeDate(DateTime("2022-06-01T00:00:00Z"))
;

