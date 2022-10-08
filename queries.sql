-- BigQuery select of the game of thrones data

-- Total count for each entity

select count(1) from `learn-bigquery-364004`.game_of_thrones.books;
select count(1) from `learn-bigquery-364004`.game_of_thrones.characters;
select count(1) from `learn-bigquery-364004`.game_of_thrones.houses;

-- select book names, authors, release dates, character names, genders and titles
select * from `learn-bigquery-364004`.game_of_thrones.books_details;

create or replace view `learn-bigquery-364004`.game_of_thrones.books_details as 
with q_books_flatten as (
select 
  name, 
  authors, 
  released, 
  character 
from `learn-bigquery-364004`.game_of_thrones.books bks, 
     unnest(characters) as character
)
select 
  bf.name as book_name, 
  bf.authors, 
  bf.released, 
  ch.name as character_name, 
  ch.gender, 
  ch.titles
from q_books_flatten bf
join `learn-bigquery-364004`.game_of_thrones.characters ch
  on bf.character = ch.url;

-- list of character names and played by actor names
select 
  url, 
  name, 
  playedBy 
from `learn-bigquery-364004`.game_of_thrones.characters
where playedBy[OFFSET(0)] <> '';

-- list of house names, regions, overlord names, sworn member names
select * from `learn-bigquery-364004`.game_of_thrones.houses_details;

create or replace view `learn-bigquery-364004`.game_of_thrones.houses_details as 
with q_overl as (
  select url, name 
  from `learn-bigquery-364004`.game_of_thrones.houses
),
q_chars as (
  select url, name
  from `learn-bigquery-364004`.game_of_thrones.characters
)
select 
  hs.url, 
  hs.name, 
  hs.region, 
  ch_curr.name as currentLordName,
  overld.name as overlord, 
  array_agg(ch.name) as swornMemberName
from `learn-bigquery-364004`.game_of_thrones.houses hs
cross join unnest(swornMembers) as swornMember
left join q_overl overld
  on overld.url = hs.overlord
left join q_chars ch
  on ch.url = swornMember
left join q_chars ch_curr
  on ch_curr.url = currentLord  
group by
 hs.url, 
  hs.name, 
  hs.region, 
  ch_curr.name,
  overld.name;
