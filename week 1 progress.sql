''' 
    week 1:
    🔹 Filtering data with WHERE, AND, OR, NOT, IN
    🔹 Grouping and aggregating using GROUP BY, HAVING, COUNT, AVG, SUM
    🔹 Pattern matching with LIKE
    🔹 Using CASE, DISTINCT, ALIAS for cleaner logic
    🔹 Playing with string and date functions
    🔹 Sorting and slicing with ORDER BY, LIMIT
'''

use employees;
select * from employee_records;

--List all employees with their names, departments,annual salary (monthly_salary * 12) using an alias.
select employee_name,department,ceil(salary*12) as annual_salary from employee_records order by annual_salary;

--Find employees who joined after January 1, 2022.
select * from employee_records where joining_date > 2021 order by joining_date;

--Show employees whose salary is between 50,000 and 80,000.
select * from employee_records where salary between 50000 and 80000 order by salary;

--Retrieve employees who are not in the 'HR' or 'Finance' departments.
select * from employee_records where department not in ('HR','Finance');

--Find employees who are in either 'Sales' or 'Marketing'.
select * from employee_records where department in ('Sales','Marketing');

--List employees who are not in the 'IT' department and earn more than 60,000.
select * from employee_records where not department = 'IT' and salary > 60000 ;

--Show employees whose designation is not in ('Manager', 'Director').
select * from employee_records where department not in ('Manager', 'Director');

--Find employees whose names start with 'A'.
select * from employee_records  where employee_name like 'A%'; 

--Show employees whose job title contains 'Engineer'.
select * from employee_records where department like '%Engineer%';

--Get a list of distinct job titles.
select distinct position from employee_records;

--Count the number of employees in each department.
select department,count(*) as number_of_employees from employee_records group by department;

--Show the maximum,minimum and average salary in each job role.
select position, ceil(max(salary)) as max_salary,ceil(min(salary)) as min_salary, ceil(avg(salary)) as avg_salary from employee_records group by position order by avg_salary;

--List departments having more than 5000 employees.
select department, count(*) as no_of_employees from employee_records group by department having no_of_employees >5000;

--Categorize employees as 'High', 'Medium', or 'Low' earners based on salary.
select employee_name,salary,
case 
    when salary > 100000 then 'High'
    when salary < 40000  then 'Low'
    else 'Medium' 
end as salary_type
from employee_records;

--Display employee names in UPPERCASE.
select upper(employee_name)  from employee_records;

==Calculate the number of years each employee has worked.
select employee_name,(year(now()) - year(joining_date)) as no_of_years_worked from employee_records ;
#select datediff(now(),joining_date) from  employee_records ;

--List employees who have birthdays this month.
select employee_name,joining_date from employee_records where month(now()) = month(joining_date);

--Show employees who joined in the 2025.
select employee_name,joining_date from employee_records where month(now()) = month(joining_date); 

--Show the top 5 highest-paid employees.
select * from employee_records order by salary desc limit 5 ;
