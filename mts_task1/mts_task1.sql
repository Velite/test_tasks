select id, max(value) as mv, max(date) as md 
from task1
group by id
order by id