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