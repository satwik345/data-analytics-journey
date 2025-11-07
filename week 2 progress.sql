'''
week 2:
    🔹 Subqueries
    🔹 Window Functions
    🔹 Common Table Expressions (CTEs)
    🔹 Triggers & Events
    🔹 Temporary Tables
    🔹 Joins & Set Operations

'''



--Find employees whose salary is above the company average.
select * from employee_records 
where salary > (select avg(salary) from employee_records)
order by salary ;

--Show departments with fewer employees than the average department size.
select department, count(*) as total_employees from employee_records
group by department
having total_employees < (select AVG(department_wise_employees) from (select count(*) as department_wise_employees from employee_records group by department) as department) 
order by total_employees;

--List employees who joined after the most recent hire in the 'HR' department.
select * from employee_records
where joining_date > (select joining_date from employee_records where department = 'HR' order by joining_date desc limit 1)
order by joining_date ;

--Rank employees by salary within each department
select *, rank() over(partition by department order by salary desc) as salary_rank_by_department from employee_records;

--Calculate running total of salaries by department 
select *, sum(salary) over(partition by department order by salary desc) as running_total from employee_records ;

--Find the first employee who joined in each department
with first_employees_by_department as
(
select *, dense_rank() over(partition by department order by joining_date) as first_employee from employee_records
) 
select * from first_employees_by_department
where first_employee = 1;

--Flag top 3 earners in each department.
with salary_ranking as
(
select *, rank() over(partition by department order by salary desc ) as salary_rank from employee_records
)
select * from salary_ranking where salary_rank < 4;


--Show salary difference between each employee and department average ().
with avg_salary_by_department as
(
select *,avg(salary) over(partition by department ) as avg_salary_department_wise from employee_records
)
select *,(salary - avg_salary_department_wise) as salary_difference  from avg_salary_by_department ;

--Create a stored procedure to fetch employee details by department name.
delimiter $$
create procedure department_wise_employees (department_name varchar(50))
begin
	select* from employee_records
    where department = department_name; 
end $$
delimiter ;

-- Procedure to Insert New Employee Records with Validation
delimiter $$
create procedure inserting_employees(in en varchar(50),in age int,in country varchar(50),in deptn varchar(50),in position varchar(50),in salary int,in joiningdate date)
begin
     if salary < 0 or  age < 0 then SIGNAL SQLSTATE '45000' set MESSAGE_TEXT = 'cannot be negative' ;
	 else INSERT INTO employee_records (employee_name, age,country,department,position, salary, joining_date)
        VALUES (en,age,country, deptn,position, salary, joiningdate);
     end if;
end $$
delimiter ;

-- Procedure to Generate Monthly Headcount Report
delimiter $$
create procedure Monthly_Headcount_Report ( report_year int, report_month int)
begin
     select department, count(*) as headcount from employee_records 
     where year(joining_date) = report_year and month(joining_date) = report_month
     group by department
     order by headcount;
end $$
delimiter ;

-- Schedule a monthly cleanup of inactive employee records.
delimiter $$
create event delete_retirees
on schedule every 1 month
do 
begin
     delete from employee_records 
     where age >= 60;
end $$
delimiter ;    
