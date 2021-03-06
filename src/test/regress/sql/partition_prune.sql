--
-- Test partitioning planner code
--
create table lp (a char) partition by list (a);
create table lp_default partition of lp default;
create table lp_ef partition of lp for values in ('e', 'f');
create table lp_ad partition of lp for values in ('a', 'd');
create table lp_bc partition of lp for values in ('b', 'c');
create table lp_g partition of lp for values in ('g');
create table lp_null partition of lp for values in (null);
explain (costs off) select * from lp;
explain (costs off) select * from lp where a > 'a' and a < 'd';
explain (costs off) select * from lp where a > 'a' and a <= 'd';
explain (costs off) select * from lp where a = 'a';
explain (costs off) select * from lp where 'a' = a;	/* commuted */
explain (costs off) select * from lp where a is not null;
explain (costs off) select * from lp where a is null;
explain (costs off) select * from lp where a = 'a' or a = 'c';
explain (costs off) select * from lp where a is not null and (a = 'a' or a = 'c');
explain (costs off) select * from lp where a <> 'g';
explain (costs off) select * from lp where a <> 'a' and a <> 'd';
explain (costs off) select * from lp where a not in ('a', 'd');

-- collation matches the partitioning collation, pruning works
create table coll_pruning (a text collate "C") partition by list (a);
create table coll_pruning_a partition of coll_pruning for values in ('a');
create table coll_pruning_b partition of coll_pruning for values in ('b');
create table coll_pruning_def partition of coll_pruning default;
explain (costs off) select * from coll_pruning where a collate "C" = 'a' collate "C";
-- collation doesn't match the partitioning collation, no pruning occurs
explain (costs off) select * from coll_pruning where a collate "POSIX" = 'a' collate "POSIX";

create table rlp (a int, b varchar) partition by range (a);
create table rlp_default partition of rlp default partition by list (a);
create table rlp_default_default partition of rlp_default default;
create table rlp_default_10 partition of rlp_default for values in (10);
create table rlp_default_30 partition of rlp_default for values in (30);
create table rlp_default_null partition of rlp_default for values in (null);
create table rlp1 partition of rlp for values from (minvalue) to (1);
create table rlp2 partition of rlp for values from (1) to (10);

create table rlp3 (b varchar, a int) partition by list (b varchar_ops);
create table rlp3_default partition of rlp3 default;
create table rlp3abcd partition of rlp3 for values in ('ab', 'cd');
create table rlp3efgh partition of rlp3 for values in ('ef', 'gh');
create table rlp3nullxy partition of rlp3 for values in (null, 'xy');
alter table rlp attach partition rlp3 for values from (15) to (20);

create table rlp4 partition of rlp for values from (20) to (30) partition by range (a);
create table rlp4_default partition of rlp4 default;
create table rlp4_1 partition of rlp4 for values from (20) to (25);
create table rlp4_2 partition of rlp4 for values from (25) to (29);

create table rlp5 partition of rlp for values from (31) to (maxvalue) partition by range (a);
create table rlp5_default partition of rlp5 default;
create table rlp5_1 partition of rlp5 for values from (31) to (40);

explain (costs off) select * from rlp where a < 1;
explain (costs off) select * from rlp where 1 > a;	/* commuted */
explain (costs off) select * from rlp where a <= 1;
explain (costs off) select * from rlp where a = 1;
explain (costs off) select * from rlp where a = 1::bigint;		/* same as above */
explain (costs off) select * from rlp where a = 1::numeric;		/* no pruning */
explain (costs off) select * from rlp where a <= 10;
explain (costs off) select * from rlp where a > 10;
explain (costs off) select * from rlp where a < 15;
explain (costs off) select * from rlp where a <= 15;
explain (costs off) select * from rlp where a > 15 and b = 'ab';
explain (costs off) select * from rlp where a = 16;
explain (costs off) select * from rlp where a = 16 and b in ('not', 'in', 'here');
explain (costs off) select * from rlp where a = 16 and b < 'ab';
explain (costs off) select * from rlp where a = 16 and b <= 'ab';
explain (costs off) select * from rlp where a = 16 and b is null;
explain (costs off) select * from rlp where a = 16 and b is not null;
explain (costs off) select * from rlp where a is null;
explain (costs off) select * from rlp where a is not null;
explain (costs off) select * from rlp where a > 30;
explain (costs off) select * from rlp where a = 30;	/* only default is scanned */
explain (costs off) select * from rlp where a <= 31;
explain (costs off) select * from rlp where a = 1 or a = 7;
explain (costs off) select * from rlp where a = 1 or b = 'ab';

explain (costs off) select * from rlp where a > 20 and a < 27;
explain (costs off) select * from rlp where a = 29;
explain (costs off) select * from rlp where a >= 29;

-- redundant clauses are eliminated
explain (costs off) select * from rlp where a > 1 and a = 10;	/* only default */
explain (costs off) select * from rlp where a > 1 and a >=15;	/* rlp3 onwards, including default */
explain (costs off) select * from rlp where a = 1 and a = 3;	/* empty */
explain (costs off) select * from rlp where (a = 1 and a = 3) or (a > 1 and a = 15);

-- multi-column keys
create table mc3p (a int, b int, c int) partition by range (a, abs(b), c);
create table mc3p_default partition of mc3p default;
create table mc3p0 partition of mc3p for values from (minvalue, minvalue, minvalue) to (1, 1, 1);
create table mc3p1 partition of mc3p for values from (1, 1, 1) to (10, 5, 10);
create table mc3p2 partition of mc3p for values from (10, 5, 10) to (10, 10, 10);
create table mc3p3 partition of mc3p for values from (10, 10, 10) to (10, 10, 20);
create table mc3p4 partition of mc3p for values from (10, 10, 20) to (10, maxvalue, maxvalue);
create table mc3p5 partition of mc3p for values from (11, 1, 1) to (20, 10, 10);
create table mc3p6 partition of mc3p for values from (20, 10, 10) to (20, 20, 20);
create table mc3p7 partition of mc3p for values from (20, 20, 20) to (maxvalue, maxvalue, maxvalue);

explain (costs off) select * from mc3p where a = 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) < 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) = 1;
explain (costs off) select * from mc3p where a = 1 and abs(b) = 1 and c < 8;
explain (costs off) select * from mc3p where a = 10 and abs(b) between 5 and 35;
explain (costs off) select * from mc3p where a > 10;
explain (costs off) select * from mc3p where a >= 10;
explain (costs off) select * from mc3p where a < 10;
explain (costs off) select * from mc3p where a <= 10 and abs(b) < 10;
explain (costs off) select * from mc3p where a = 11 and abs(b) = 0;
explain (costs off) select * from mc3p where a = 20 and abs(b) = 10 and c = 100;
explain (costs off) select * from mc3p where a > 20;
explain (costs off) select * from mc3p where a >= 20;
explain (costs off) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20);
explain (costs off) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20) or a < 1;
explain (costs off) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20) or a < 1 or a = 1;
explain (costs off) select * from mc3p where a = 1 or abs(b) = 1 or c = 1;
explain (costs off) select * from mc3p where (a = 1 and abs(b) = 1) or (a = 10 and abs(b) = 10);
explain (costs off) select * from mc3p where (a = 1 and abs(b) = 1) or (a = 10 and abs(b) = 9);

-- a simpler multi-column keys case
create table mc2p (a int, b int) partition by range (a, b);
create table mc2p_default partition of mc2p default;
create table mc2p0 partition of mc2p for values from (minvalue, minvalue) to (1, minvalue);
create table mc2p1 partition of mc2p for values from (1, minvalue) to (1, 1);
create table mc2p2 partition of mc2p for values from (1, 1) to (2, minvalue);
create table mc2p3 partition of mc2p for values from (2, minvalue) to (2, 1);
create table mc2p4 partition of mc2p for values from (2, 1) to (2, maxvalue);
create table mc2p5 partition of mc2p for values from (2, maxvalue) to (maxvalue, maxvalue);

explain (costs off) select * from mc2p where a < 2;
explain (costs off) select * from mc2p where a = 2 and b < 1;
explain (costs off) select * from mc2p where a > 1;
explain (costs off) select * from mc2p where a = 1 and b > 1;

-- boolean partitioning
create table boolpart (a bool) partition by list (a);
create table boolpart_default partition of boolpart default;
create table boolpart_t partition of boolpart for values in ('true');
create table boolpart_f partition of boolpart for values in ('false');

explain (costs off) select * from boolpart where a in (true, false);
explain (costs off) select * from boolpart where a = false;
explain (costs off) select * from boolpart where not a = false;
explain (costs off) select * from boolpart where a is true or a is not true;
explain (costs off) select * from boolpart where a is not true;
explain (costs off) select * from boolpart where a is not true and a is not false;
explain (costs off) select * from boolpart where a is unknown;
explain (costs off) select * from boolpart where a is not unknown;

--
-- some more cases
--

--
-- pruning for partitioned table appearing inside a sub-query
--
-- pruning won't work for mc3p, because some keys are Params
explain (costs off) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.a = t1.b and abs(t2.b) = 1 and t2.c = 1) s where t1.a = 1;

-- pruning should work fine, because values for a prefix of keys (a, b) are
-- available
explain (costs off) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.c = t1.b and abs(t2.b) = 1 and t2.a = 1) s where t1.a = 1;

-- also here, because values for all keys are provided
explain (costs off) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.a = 1 and abs(t2.b) = 1 and t2.c = 1) s where t1.a = 1;

--
-- pruning with clauses containing <> operator
--

-- doesn't prune range partitions
create table rp (a int) partition by range (a);
create table rp0 partition of rp for values from (minvalue) to (1);
create table rp1 partition of rp for values from (1) to (2);
create table rp2 partition of rp for values from (2) to (maxvalue);

explain (costs off) select * from rp where a <> 1;
explain (costs off) select * from rp where a <> 1 and a <> 2;

-- null partition should be eliminated due to strict <> clause.
explain (costs off) select * from lp where a <> 'a';

-- ensure we detect contradictions in clauses; a can't be NULL and NOT NULL.
explain (costs off) select * from lp where a <> 'a' and a is null;
explain (costs off) select * from lp where (a <> 'a' and a <> 'd') or a is null;

-- check that it also works for a partitioned table that's not root,
-- which in this case are partitions of rlp that are themselves
-- list-partitioned on b
explain (costs off) select * from rlp where a = 15 and b <> 'ab' and b <> 'cd' and b <> 'xy' and b is not null;

--
-- different collations for different keys with same expression
--
create table coll_pruning_multi (a text) partition by range (substr(a, 1) collate "POSIX", substr(a, 1) collate "C");
create table coll_pruning_multi1 partition of coll_pruning_multi for values from ('a', 'a') to ('a', 'e');
create table coll_pruning_multi2 partition of coll_pruning_multi for values from ('a', 'e') to ('a', 'z');
create table coll_pruning_multi3 partition of coll_pruning_multi for values from ('b', 'a') to ('b', 'e');

-- no pruning, because no value for the leading key
explain (costs off) select * from coll_pruning_multi where substr(a, 1) = 'e' collate "C";

-- pruning, with a value provided for the leading key
explain (costs off) select * from coll_pruning_multi where substr(a, 1) = 'a' collate "POSIX";

-- pruning, with values provided for both keys
explain (costs off) select * from coll_pruning_multi where substr(a, 1) = 'e' collate "C" and substr(a, 1) = 'a' collate "POSIX";

--
-- LIKE operators don't prune
--
create table like_op_noprune (a text) partition by list (a);
create table like_op_noprune1 partition of like_op_noprune for values in ('ABC');
create table like_op_noprune2 partition of like_op_noprune for values in ('BCD');
explain (costs off) select * from like_op_noprune where a like '%BC';

--
-- tests wherein clause value requires a cross-type comparison function
--
create table lparted_by_int2 (a smallint) partition by list (a);
create table lparted_by_int2_1 partition of lparted_by_int2 for values in (1);
create table lparted_by_int2_16384 partition of lparted_by_int2 for values in (16384);
explain (costs off) select * from lparted_by_int2 where a = 100000000000000;

create table rparted_by_int2 (a smallint) partition by range (a);
create table rparted_by_int2_1 partition of rparted_by_int2 for values from (1) to (10);
create table rparted_by_int2_16384 partition of rparted_by_int2 for values from (10) to (16384);
-- all partitions pruned
explain (costs off) select * from rparted_by_int2 where a > 100000000000000;
create table rparted_by_int2_maxvalue partition of rparted_by_int2 for values from (16384) to (maxvalue);
-- all partitions but rparted_by_int2_maxvalue pruned
explain (costs off) select * from rparted_by_int2 where a > 100000000000000;

drop table lp, coll_pruning, rlp, mc3p, mc2p, boolpart, rp, coll_pruning_multi, like_op_noprune, lparted_by_int2, rparted_by_int2;

--
-- Test Partition pruning for HASH partitioning
--
-- Use hand-rolled hash functions and operator classes to get predictable
-- result on different matchines.  See the definitions of
-- part_part_test_int4_ops and part_test_text_ops in insert.sql.
--

create table hp (a int, b text) partition by hash (a part_test_int4_ops, b part_test_text_ops);
create table hp0 partition of hp for values with (modulus 4, remainder 0);
create table hp3 partition of hp for values with (modulus 4, remainder 3);
create table hp1 partition of hp for values with (modulus 4, remainder 1);
create table hp2 partition of hp for values with (modulus 4, remainder 2);

insert into hp values (null, null);
insert into hp values (1, null);
insert into hp values (1, 'xxx');
insert into hp values (null, 'xxx');
insert into hp values (2, 'xxx');
insert into hp values (1, 'abcde');
select tableoid::regclass, * from hp order by 1;

-- partial keys won't prune, nor would non-equality conditions
explain (costs off) select * from hp where a = 1;
explain (costs off) select * from hp where b = 'xxx';
explain (costs off) select * from hp where a is null;
explain (costs off) select * from hp where b is null;
explain (costs off) select * from hp where a < 1 and b = 'xxx';
explain (costs off) select * from hp where a <> 1 and b = 'yyy';
explain (costs off) select * from hp where a <> 1 and b <> 'xxx';

-- pruning should work if either a value or a IS NULL clause is provided for
-- each of the keys
explain (costs off) select * from hp where a is null and b is null;
explain (costs off) select * from hp where a = 1 and b is null;
explain (costs off) select * from hp where a = 1 and b = 'xxx';
explain (costs off) select * from hp where a is null and b = 'xxx';
explain (costs off) select * from hp where a = 2 and b = 'xxx';
explain (costs off) select * from hp where a = 1 and b = 'abcde';
explain (costs off) select * from hp where (a = 1 and b = 'abcde') or (a = 2 and b = 'xxx') or (a is null and b is null);

drop table hp;

--
-- Test runtime partition pruning
--
create table ab (a int not null, b int not null) partition by list (a);
create table ab_a2 partition of ab for values in(2) partition by list (b);
create table ab_a2_b1 partition of ab_a2 for values in (1);
create table ab_a2_b2 partition of ab_a2 for values in (2);
create table ab_a2_b3 partition of ab_a2 for values in (3);
create table ab_a1 partition of ab for values in(1) partition by list (b);
create table ab_a1_b1 partition of ab_a1 for values in (1);
create table ab_a1_b2 partition of ab_a1 for values in (2);
create table ab_a1_b3 partition of ab_a1 for values in (3);
create table ab_a3 partition of ab for values in(3) partition by list (b);
create table ab_a3_b1 partition of ab_a3 for values in (1);
create table ab_a3_b2 partition of ab_a3 for values in (2);
create table ab_a3_b3 partition of ab_a3 for values in (3);

-- Disallow index only scans as concurrent transactions may stop visibility
-- bits being set causing "Heap Fetches" to be unstable in the EXPLAIN ANALYZE
-- output.
set enable_indexonlyscan = off;

prepare ab_q1 (int, int, int) as
select * from ab where a between $1 and $2 and b <= $3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);

explain (analyze, costs off, summary off, timing off) execute ab_q1 (2, 2, 3);
explain (analyze, costs off, summary off, timing off) execute ab_q1 (1, 2, 3);

deallocate ab_q1;

-- Runtime pruning after optimizer pruning
prepare ab_q1 (int, int) as
select a from ab where a between $1 and $2 and b < 3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);

explain (analyze, costs off, summary off, timing off) execute ab_q1 (2, 2);
explain (analyze, costs off, summary off, timing off) execute ab_q1 (2, 4);

-- Ensure a mix of external and exec params work together at different
-- levels of partitioning.
prepare ab_q2 (int, int) as
select a from ab where a between $1 and $2 and b < (select 3);

execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);

explain (analyze, costs off, summary off, timing off) execute ab_q2 (2, 2);

-- As above, but with swap the exec param to the first partition level
prepare ab_q3 (int, int) as
select a from ab where b between $1 and $2 and a < (select 3);

execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);

explain (analyze, costs off, summary off, timing off) execute ab_q3 (2, 2);

-- Parallel append
prepare ab_q4 (int, int) as
select avg(a) from ab where a between $1 and $2 and b < 4;

-- Encourage use of parallel plans
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set min_parallel_table_scan_size = 0;

-- set this so we get a parallel plan
set max_parallel_workers_per_gather = 2;

-- and zero this so that workers don't destabilize the explain output
set max_parallel_workers = 0;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);

explain (analyze, costs off, summary off, timing off) execute ab_q4 (2, 2);

-- Test run-time pruning with IN lists.
prepare ab_q5 (int, int, int) as
select avg(a) from ab where a in($1,$2,$3) and b < 4;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);

explain (analyze, costs off, summary off, timing off) execute ab_q5 (1, 1, 1);
explain (analyze, costs off, summary off, timing off) execute ab_q5 (2, 3, 3);

-- Try some params whose values do not belong to any partition.
-- We'll still get a single subplan in this case, but it should not be scanned.
explain (analyze, costs off, summary off, timing off) execute ab_q5 (33, 44, 55);

-- Test parallel Append with IN list and parameterized nested loops
create table lprt_a (a int not null);
-- Insert some values we won't find in ab
insert into lprt_a select 0 from generate_series(1,100);

-- and insert some values that we should find.
insert into lprt_a values(1),(1);

analyze lprt_a;

create index ab_a2_b1_a_idx on ab_a2_b1 (a);
create index ab_a2_b2_a_idx on ab_a2_b2 (a);
create index ab_a2_b3_a_idx on ab_a2_b3 (a);
create index ab_a1_b1_a_idx on ab_a1_b1 (a);
create index ab_a1_b2_a_idx on ab_a1_b2 (a);
create index ab_a1_b3_a_idx on ab_a1_b3 (a);
create index ab_a3_b1_a_idx on ab_a3_b1 (a);
create index ab_a3_b2_a_idx on ab_a3_b2 (a);
create index ab_a3_b3_a_idx on ab_a3_b3 (a);

set enable_hashjoin = 0;
set enable_mergejoin = 0;

prepare ab_q6 (int, int, int) as
select avg(ab.a) from ab inner join lprt_a a on ab.a = a.a where a.a in($1,$2,$3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);

explain (analyze, costs off, summary off, timing off) execute ab_q6 (0, 0, 1);

insert into lprt_a values(3),(3);

explain (analyze, costs off, summary off, timing off) execute ab_q6 (1, 0, 3);
explain (analyze, costs off, summary off, timing off) execute ab_q6 (1, 0, 0);

delete from lprt_a where a = 1;

explain (analyze, costs off, summary off, timing off) execute ab_q6 (1, 0, 0);

reset enable_hashjoin;
reset enable_mergejoin;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset min_parallel_table_scan_size;
reset max_parallel_workers_per_gather;

-- Test run-time partition pruning with an initplan
explain (analyze, costs off, summary off, timing off)
select * from ab where a = (select max(a) from lprt_a) and b = (select max(a)-1 from lprt_a);

deallocate ab_q1;
deallocate ab_q2;
deallocate ab_q3;
deallocate ab_q4;
deallocate ab_q5;
deallocate ab_q6;

drop table ab, lprt_a;

-- Join
create table tbl1(col1 int);
insert into tbl1 values (501), (505);

-- Basic table
create table tprt (col1 int) partition by range (col1);
create table tprt_1 partition of tprt for values from (1) to (501);
create table tprt_2 partition of tprt for values from (501) to (1001);
create table tprt_3 partition of tprt for values from (1001) to (2001);
create table tprt_4 partition of tprt for values from (2001) to (3001);
create table tprt_5 partition of tprt for values from (3001) to (4001);
create table tprt_6 partition of tprt for values from (4001) to (5001);

create index tprt1_idx on tprt_1 (col1);
create index tprt2_idx on tprt_2 (col1);
create index tprt3_idx on tprt_3 (col1);
create index tprt4_idx on tprt_4 (col1);
create index tprt5_idx on tprt_5 (col1);
create index tprt6_idx on tprt_6 (col1);

insert into tprt values (10), (20), (501), (502), (505), (1001), (4500);

set enable_hashjoin = off;
set enable_mergejoin = off;

explain (analyze, costs off, summary off, timing off)
select * from tbl1 join tprt on tbl1.col1 > tprt.col1;

explain (analyze, costs off, summary off, timing off)
select * from tbl1 join tprt on tbl1.col1 = tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 > tprt.col1
order by tbl1.col1, tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

-- Multiple partitions
insert into tbl1 values (1001), (1010), (1011);
explain (analyze, costs off, summary off, timing off)
select * from tbl1 inner join tprt on tbl1.col1 > tprt.col1;

explain (analyze, costs off, summary off, timing off)
select * from tbl1 inner join tprt on tbl1.col1 = tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 > tprt.col1
order by tbl1.col1, tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

-- Last partition
delete from tbl1;
insert into tbl1 values (4400);
explain (analyze, costs off, summary off, timing off)
select * from tbl1 join tprt on tbl1.col1 < tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 < tprt.col1
order by tbl1.col1, tprt.col1;

-- No matching partition
delete from tbl1;
insert into tbl1 values (10000);
explain (analyze, costs off, summary off, timing off)
select * from tbl1 join tprt on tbl1.col1 = tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

drop table tbl1, tprt;

-- Test with columns defined in varying orders between each level
create table part_abc (a int not null, b int not null, c int not null) partition by list (a);
create table part_bac (b int not null, a int not null, c int not null) partition by list (b);
create table part_cab (c int not null, a int not null, b int not null) partition by list (c);
create table part_abc_p1 (a int not null, b int not null, c int not null);

alter table part_abc attach partition part_bac for values in(1);
alter table part_bac attach partition part_cab for values in(2);
alter table part_cab attach partition part_abc_p1 for values in(3);

prepare part_abc_q1 (int, int, int) as
select * from part_abc where a = $1 and b = $2 and c = $3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);

-- Single partition should be scanned.
explain (analyze, costs off, summary off, timing off) execute part_abc_q1 (1, 2, 3);

deallocate part_abc_q1;

drop table part_abc;

-- Ensure that an Append node properly handles a sub-partitioned table
-- matching without any of its leaf partitions matching the clause.
create table listp (a int, b int) partition by list (a);
create table listp_1 partition of listp for values in(1) partition by list (b);
create table listp_1_1 partition of listp_1 for values in(1);
create table listp_2 partition of listp for values in(2) partition by list (b);
create table listp_2_1 partition of listp_2 for values in(2);
select * from listp where b = 1;

-- Ensure that an Append node properly can handle selection of all first level
-- partitions before finally detecting the correct set of 2nd level partitions
-- which match the given parameter.
prepare q1 (int,int) as select * from listp where b in ($1,$2);

execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);

explain (analyze, costs off, summary off, timing off)  execute q1 (1,1);

explain (analyze, costs off, summary off, timing off)  execute q1 (2,2);

-- Try with no matching partitions. One subplan should remain in this case,
-- but it shouldn't be executed.
explain (analyze, costs off, summary off, timing off)  execute q1 (0,0);

deallocate q1;

-- Test more complex cases where a not-equal condition further eliminates partitions.
prepare q1 (int,int,int,int) as select * from listp where b in($1,$2) and $3 <> b and $4 <> b;

execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);

-- Both partitions allowed by IN clause, but one disallowed by <> clause
explain (analyze, costs off, summary off, timing off)  execute q1 (1,2,2,0);

-- Both partitions allowed by IN clause, then both excluded again by <> clauses.
-- One subplan will remain in this case, but it should not be executed.
explain (analyze, costs off, summary off, timing off)  execute q1 (1,2,2,1);

drop table listp;

-- Ensure runtime pruning works with initplans params with boolean types
create table boolvalues (value bool not null);
insert into boolvalues values('t'),('f');

create table boolp (a bool) partition by list (a);
create table boolp_t partition of boolp for values in('t');
create table boolp_f partition of boolp for values in('f');

explain (analyze, costs off, summary off, timing off)
select * from boolp where a = (select value from boolvalues where value);

explain (analyze, costs off, summary off, timing off)
select * from boolp where a = (select value from boolvalues where not value);

drop table boolp;

reset enable_indexonlyscan;
