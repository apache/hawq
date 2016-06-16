SET TIME ZONE UTC;
INSERT INTO tmp 
  (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, v, w, x, y, z)
   VALUES (4, 
           'name', 
           'text', 
           4.1, 
           4.1, 
           2, 
           '(4.1,4.1,3.1,3.1)',
           'Mon May  1 00:30:30 1995', 
           'c', 
           '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}',
           314159, 
           '(1,1)', 
           '512',
           '1 2 3 4 5 6 7 8', 
           'magnetic disk', 
           '(1.1,1.1)', 
           '(4.1,4.1,3.1,3.1)',
           '(0,2,4.1,4.1,3.1,3.1)', 
           '(4.1,4.1,3.1,3.1)', 
           '["epoch" "infinity"]',
           'epoch', 
           '01:00:10', 
           '{1.0,2.0,3.0,4.0}', 
           '{1.0,2.0,3.0,4.0}', 
           '{1,2,3,4}');

SELECT * FROM tmp;
