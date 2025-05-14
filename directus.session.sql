select 
    service_name,
    count(*) as total
from 
    public.cicd_application_service
group by 
    service_name
order by
    total desc

SELECT
    service_type,
    count(*) as total
FROM
    public.cicd_application_servicetype
GROUP BY
    service_type
ORDER BY
    total DESC;

SELECT
    application,
    count(*) as total
FROM
    public.cicd_application
GROUP BY
    application
ORDER BY
    total DESC;


SELECT
    s.service_name,
    a.application,
    st.service_type
from 
    public.cicd_application_service s
full JOIN
    public.cicd_application a
on
    s.application = a.id
full JOIN
    public.cicd_application_servicetype st
on
    s.service_type = st.id
where
    s.service_name is not null
    and a.application is not null
    and st.service_type is not null
ORDER BY
    2,3;


create or replace table dev.cicd_app_service_languages_wh (
    id serial primary key,
    application varchar(255),
    service_name varchar(255),
    service_type varchar(255),
    script_languages json,
    unique (service_type, service_name, application)
);
alter table dev.cicd_app_service_languages_wh
    alter column application type varchar(255) using application::varchar(255),
    alter column service_name type varchar(255) using service_name::varchar(255),
    alter column service_type type varchar(255) using service_type::varchar(255),
    alter column script_languages type json using script_languages::json,
    alter column application drop not null,
    alter column service_name drop not null,
    alter column service_type drop not null,
    alter column script_languages drop not null;
