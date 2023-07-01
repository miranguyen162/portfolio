``` Data tables:```
-- Indicator definition: country_series_definitions
-- Main data: indicators_data
-- Topic and related indicators; series_summary
-- Year ranges of year representative: series_time
-- country_summary + footnotes: unknown


```1. Explore Data```
-- Ques: How many topics used for measuring WDI?
select distinct(main_topic) from
(select 
    split(topic, ": ")[safe_offset(0)] as main_topic,
    split(topic, ": ")[safe_offset(1)] as sub_topic
from bigquery-public-data.world_bank_wdi.series_summary)
-- Eval: There are 11 main fields. Picking a field to dig down: Financial Sector.

-- Ques: How many subtopics of Financial Sector?
select distinct(sub_topic) from
(select 
    split(topic, ": ")[safe_offset(0)] as main_topic,
    split(topic, ": ")[safe_offset(1)] as sub_topic
from bigquery-public-data.world_bank_wdi.series_summary)
where main_topic like "%Financial Sector%"
-- Eval: There are: Access, Assets, Interest rates, Capital markets, Exchange rates & prices, Monetary holdings (liabilities)

-- Walk through all indicators of Financial Sector
select distinct main_topic, sub_topic, indicator_name from
(select 
    split(topic, ": ")[safe_offset(0)] as main_topic,
    split(topic, ": ")[safe_offset(1)] as sub_topic,
    indicator_name
from bigquery-public-data.world_bank_wdi.series_summary)
where main_topic like "%Financial Sector%"
order by sub_topic, indicator_name

-- Create topic column into main data for further analysis
create view wdi.data_with_topic as(
select * from bigquery-public-data.world_bank_wdi.indicators_data as a
left join 
(select distinct series_code, 
    split(topic, ": ")[safe_offset(0)] as main_topic,
    split(topic, ": ")[safe_offset(1)] as sub_topic
from bigquery-public-data.world_bank_wdi.series_summary) as b
on a.indicator_code = b.series_code)

-- Ques: What are data of indicators belonging Financial Sector that collected in Vietnam. The lasted updated year of those?
select a.main_topic, a.sub_topic, a.indicator_name, a.latest_year, b.value from
  (
    select main_topic, sub_topic, indicator_name, max(year) as latest_year 
    from wdi.data_with_topic
  where country_name = 'Vietnam' and main_topic = 'Financial Sector' 
  group by main_topic, sub_topic, indicator_name
  ) as a
left join 
  (select * from wdi.data_with_topic where country_name = 'Vietnam') as b
  on a.indicator_name = b.indicator_name and a.latest_year = b.year
order by main_topic, sub_topic, indicator_name
-- Eval:
-- Mostly data updated from 2019.
-- Indicators about Account Ownership from Global Findex Report is from 2017.
-- Risk premium on lending from 2015. Wholesale price index from 1974.


```
2. Deeper Analysis on sub topic: Financial Access.
  Indicators: Account Ownership, Number of ATM & Commercial Bank Branches.
  Targeted countries: 
  - Main: Vietnam.
  - Compare to some ASEAN countries: Singapore, Thailand, Philippines, Malaysia, Indonesia.
```
-- (1) Account Ownership
-- Categorize sub-indicators of Account Ownership
create table
wdi.account_owner_asean_years as (
select case 
    when class in ('female', 'male') then "Gender"
    when class in ('young_adult', 'older_adult') then "Age"
    when class in ('poorest_40', 'richest_60') then "Income"
    when class in ('primary_education', 'secondary_education') then "Education"
    else "all"
    end as category,
    * from 
(select country_name,
    case 
    when indicator_name like "%female%" then 'female'
    when indicator_name like "% male%" then "male"
    when indicator_name like "%young adult%" then "young_adult"
    when indicator_name like "%older adult%" then "older_adult"
    when indicator_name like "%poorest%" then "poorest_40"
    when indicator_name like "%richest%" then "richest_60"
    when indicator_name like "%primary education%" then "primary_education"
    when indicator_name like "%secondary education%" then "secondary_education" 
    else "all"
    end as class,
    year, round(value,1) as value from bigquery-public-data.world_bank_wdi.indicators_data
where country_name in ("Vietnam", "Singapore", "Thailand", "Philippines", "Malaysia", "Indonesia")
  and lower(indicator_name) like "%account ownership%")
order by country_name, category, class, year 
)

select * from `propane-shell-390604.wdi.account_owner_asean_years`
order by country_name, category, class, year

-- How are gaps between class in a category?
-- Set up number label for classes
create view wdi.class_num_in_cate as (
select distinct category, class, 
    NTILE(2) over
    (partition by country_name, category, year order by country_name, year, category, class) as class_label
from `propane-shell-390604.wdi.account_owner_asean_years`
where category <> 'all'
order by category, class
)

-- Calculate gap values
select country_name, 
 case when category = "Age" then "Young more than Older adults"
      when category = "Education" then "Secondary education upward more than Primary education downward"
      when category = "Gender" then "Male more than Female"
      when category = "Income" then "60% Richest more than 40% Poorest"
      end as indicator_name,
  year, round(`2` - `1`,1) as gap from (
select * from (
select a.country_name, a.category, a.year, a.value, cast(b.class_label as string) class_label
from `propane-shell-390604.wdi.account_owner_asean_years` a
left join wdi.class_num_in_cate b
on a.category = b.category and a.class = b.class
)
pivot (
  sum(value) for class_label in ("1", "2")
)
where category <> 'all'
order by category, year, country_name
)

-- (2) Number of ATM & Commercial Bank Branches
-- series code: FB.ATM.TOTL.P5, FB.CBK.BRCH.P5
select country_name, 
  case  
  when indicator_code like "%ATM%" then "ATM"
  when indicator_code like "%BRCH%" then "Commercial Bank Branches"
  end as indicator, indicator_name, year, round(value) as value from
(select country_name, indicator_code, indicator_name, year, value  
from bigquery-public-data.world_bank_wdi.indicators_data
where
  indicator_code in ("FB.ATM.TOTL.P5", "FB.CBK.BRCH.P5") and
  country_name in ("Vietnam", "Singapore", "Thailand", "Philippines", "Malaysia", "Indonesia")
)
order by country_name, indicator, year
