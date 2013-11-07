We are working on the 10% dataset.

This is the query plan generated:

                            π(d.fname,d.lname),card:323818
                            |
                            ⨝(a.id=c.pid),card:323818
  __________________________|___________________________
  |                                                    |
  σ(a.lname=Spicer),card:1                             ⨝(m.mid=c.mid),card:323818
  |                                    ________________|_________________
  σ(a.fname=John),card:1               |                                |
  |                                    ⨝(d.id=m.did),card:29762         |
  |                           _________|_________                       |
  |                           |                 |                     scan(Casts c)
scan(Actor a)               scan(Director d)  scan(Movie_Director m)

The reason this plan is generated Casts has the most number of roles, so it will be joined with the join of Director and Movie.
As for Actor, we are doing filter first so it only has one row left, thus, it will be joined as the outer most loop.


Our own query is:

select a.lname, a.fname 
from Movie m, Actor a, Casts c 
where m.id = c.mid and c.pid = a.id 
and m.year < 1900
and a.gender = 'F'
and c.role = 'Waiter';

The query plan generated is:
                                  π(a.lname,a.fname),card:7
                                  |
                                  ⨝(c.mid=m.id),card:7
               ___________________|___________________
               |                                     |
               ⨝(c.pid=a.id),card:1                  σ(m.year<1900),card:7
  _____________|______________                       |
  |                          |                       |
  σ(c.role=Waiter),card:1    σ(a.gender=F),card:1    |
  |                          |                     scan(Movie m)
scan(Casts c)              scan(Actor a)

The reason this plan is generated is that after filter, Casts and Actor only has one row, so they will be joined first and their result would be the outer loop. Then, their result would be joined with Movie table, which is filtered and only has 7 rows.
